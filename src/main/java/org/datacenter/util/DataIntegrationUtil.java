package org.datacenter.util;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.datacenter.config.HumanMachineConfig;
import org.datacenter.config.keys.HumanMachineSysConfigKey;
import org.datacenter.exception.ZorathosException;
import org.datacenter.model.IntegrationType;
import org.datacenter.model.base.TiDBDatabase;
import org.datacenter.model.base.TiDBTable;
import org.datacenter.model.column.BaseColumn;
import org.datacenter.model.column.ColumnFactory;
import org.datacenter.model.cte.CteCacheFactory;
import org.jetbrains.annotations.NotNull;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.datacenter.config.keys.HumanMachineSysConfigKey.FLINK_CHECKPOINT_INTERVAL;
import static org.datacenter.config.keys.HumanMachineSysConfigKey.FLINK_CHECKPOINT_TIMEOUT;
import static org.datacenter.config.keys.HumanMachineSysConfigKey.TIDB_MYSQL_DRIVER_NAME;

/**
 * @author : [wangminan]
 * @description : 数据接收器工具类
 */
@Slf4j
@SuppressWarnings("deprecation")
public class DataIntegrationUtil {

    public static ObjectMapper mapper = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .configure(JsonParser.Feature.ALLOW_UNQUOTED_CONTROL_CHARS, true)
            .configure(JsonParser.Feature.INCLUDE_SOURCE_IN_LOCATION, true)
            .configure(JsonParser.Feature.ALLOW_NON_NUMERIC_NUMBERS, true);

    public static StreamExecutionEnvironment prepareStreamEnv() {
        log.info("Preparing flink execution environment.");
        // 创建配置
        Configuration configuration = new Configuration();
        configuration.set(CheckpointingOptions.CHECKPOINTING_INTERVAL,
                Duration.ofSeconds(Long.parseLong(HumanMachineConfig.getProperty(FLINK_CHECKPOINT_INTERVAL))));
        configuration.set(CheckpointingOptions.CHECKPOINTING_CONSISTENCY_MODE,
                CheckpointingMode.EXACTLY_ONCE);
        configuration.set(CheckpointingOptions.CHECKPOINTING_TIMEOUT,
                Duration.ofSeconds(Long.parseLong(HumanMachineConfig.getProperty(FLINK_CHECKPOINT_TIMEOUT))));
        configuration.set(CheckpointingOptions.EXTERNALIZED_CHECKPOINT_RETENTION,
                ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);
        configuration.set(TaskManagerOptions.TASK_HEAP_MEMORY, MemorySize.parse("4gb"));
        configuration.set(TaskManagerOptions.TASK_OFF_HEAP_MEMORY, MemorySize.parse("1gb"));
        configuration.set(TaskManagerOptions.NETWORK_MEMORY_MIN, MemorySize.parse("256mb"));
        // 开启非对齐检查点
        configuration.set(CheckpointingOptions.ENABLE_UNALIGNED, true);
        configuration.set(CheckpointingOptions.ALIGNED_CHECKPOINT_TIMEOUT,
                Duration.ofSeconds(120));

        // 根据配置创建环境
        return StreamExecutionEnvironment.getExecutionEnvironment(configuration);
    }

    public static StreamTableEnvironment prepareBatchTableEnv() {
        log.info("Preparing batch execution environment.");
        StreamExecutionEnvironment env = DataIntegrationUtil.prepareStreamEnv();
        env.setParallelism(1);
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        return StreamTableEnvironment.create(env);
    }

    public static void createTiDBCatalog(StreamTableEnvironment tEnv) {
        tEnv.executeSql(String.format(
                """
                        CREATE CATALOG tidb_catalog WITH (
                          'type' = 'jdbc',
                          'default-database' = '%s',
                          'username' = '%s',
                          'password' = '%s',
                          'base-url' = '%s'
                        )
                        """,
                TiDBDatabase.SIMULATION.getName(),
                HumanMachineConfig.getProperty(HumanMachineSysConfigKey.TIDB_USERNAME),
                HumanMachineConfig.getProperty(HumanMachineSysConfigKey.TIDB_PASSWORD),
                HumanMachineConfig.getProperty(HumanMachineSysConfigKey.TIDB_URL_PREFIX)
        ));
        tEnv.executeSql("USE CATALOG tidb_catalog");
    }
}
