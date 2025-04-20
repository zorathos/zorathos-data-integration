package org.datacenter.util;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.datacenter.config.HumanMachineConfig;

import java.time.Duration;

import static org.datacenter.config.keys.HumanMachineSysConfigKey.FLINK_CHECKPOINT_INTERVAL;
import static org.datacenter.config.keys.HumanMachineSysConfigKey.FLINK_CHECKPOINT_TIMEOUT;

/**
 * @author : [wangminan]
 * @description : 数据接收器工具类
 */
@Slf4j
public class DataIntegrationUtil {

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

        // 开启非对齐检查点
        configuration.set(CheckpointingOptions.ENABLE_UNALIGNED, true);
        configuration.set(CheckpointingOptions.ALIGNED_CHECKPOINT_TIMEOUT,
                Duration.ofSeconds(120));

        // 根据配置创建环境

        return StreamExecutionEnvironment.getExecutionEnvironment(configuration);
    }
}
