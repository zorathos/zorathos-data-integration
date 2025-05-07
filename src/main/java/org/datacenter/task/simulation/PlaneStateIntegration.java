package org.datacenter.task.simulation;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.datacenter.config.integration.simulation.SimulationIntegrationConfig;
import org.datacenter.config.keys.HumanMachineIntegrationConfigKey;
import org.datacenter.exception.ZorathosException;
import org.datacenter.model.IntegrationType;
import org.datacenter.model.base.TiDBTable;
import org.datacenter.task.BaseIntegration;
import org.datacenter.util.DataIntegrationUtil;

import java.util.List;
import java.util.Map;

/**
 * @author : [wangminan]
 * @description : 飞机状态数据整合
 */
@EqualsAndHashCode(callSuper = true)
@Data
@Slf4j
public class PlaneStateIntegration extends BaseIntegration {
    private SimulationIntegrationConfig config;

    @Override
    public void run() {
        log.info("Start plane state integration for sortie {}", config.getSortieNumber());
        // 2. 初始化 Flink 执行环境（批模式）
        StreamTableEnvironment tEnv = DataIntegrationUtil.prepareBatchTableEnv();
        // 3. 注册并使用 TiDB JDBC Catalog
        DataIntegrationUtil.createTiDBCatalog(tEnv);

        List<TiDBTable> allPlaneStateTables = List.of(
                TiDBTable.CD_DRONE_PLANE_STATE,
                TiDBTable.CD_DRONE_TSPI,
                TiDBTable.COMMAND,
                TiDBTable.PLANE_STATE,
                TiDBTable.TSPI
        );

        // 5. 统计每张表在本次 sortie 的数据量，仅保留 count > 0
        Map<TiDBTable, Long> tableCounts = DataIntegrationUtil.getTableCounts(allPlaneStateTables, config.getSortieNumber());

        if (tableCounts.isEmpty()) {
            log.warn("No data found for sortie {}", config.getSortieNumber());
            return;
        }

        // 6. 根据最小行数选择主表
        TiDBTable mainTable = tableCounts.entrySet().stream()
                .min(Map.Entry.comparingByValue())
                .orElseThrow(() -> new ZorathosException("Unable to confirm main table for plane state integration, all tables are empty."))
                .getKey();
        log.info("Chosen main table: {}", mainTable.getName());

        StringBuilder withClause = DataIntegrationUtil.generateWithClause(IntegrationType.SIMULATION_PLANE_STATE, tableCounts, mainTable, config.getSortieNumber());
        // 9. 最终 INSERT
        String finalSql =
                "INSERT INTO `tidb_catalog`.`simulation_integration`.`plane_state_integration`\n"
                        + withClause
                        + """
                        SELECT sortie_number, event_ts, aircraft_id, aircraft_type, message_time, satellite_guidance_time,
                            local_time, message_sequence_number,
                            longitude, latitude, pressure_altitude, satellite_altitude, roll, pitch, heading,
                            north_velocity, vertical_velocity, east_velocity, indicated_airspeed,
                            true_angle_of_attack, mach_number, normal_load_factor, field_elevation, radio_altitude, remaining_fuel,
                            training_status, scenario, manual_respawn, parameter_setting_status, encryption_status,
                            chaff, afterburner,
                            command_type, command_id, command_content, response_sequence_number,
                            delay_status, '%s' AS source_table
                        FROM joined_data
                        ORDER BY event_ts
                        """.formatted(mainTable.getName());
        log.info("Final sql: {}", finalSql);
        tEnv.executeSql(finalSql);

        log.info("Plane state data integration completed");
    }

    public static void main(String[] args) {
        ParameterTool param = ParameterTool.fromArgs(args);
        String sortieNumber = param.getRequired(
                HumanMachineIntegrationConfigKey.SORTIE_NUMBER.getKeyForParamsMap()
        );
        SimulationIntegrationConfig config = SimulationIntegrationConfig.builder()
                .sortieNumber(sortieNumber)
                .build();
        PlaneStateIntegration integration = new PlaneStateIntegration();
        integration.setConfig(config);
        integration.start();
    }
}
