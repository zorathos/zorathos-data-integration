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
import org.datacenter.util.SimulationIntegrationUtil;

import java.util.List;
import java.util.Map;

/**
 * @author wangminan
 * @description 仿真导弹数据整合（使用 TiDBTable 枚举管理源表）
 */
@EqualsAndHashCode(callSuper = true)
@Slf4j
@Data
public class MissileIntegration extends BaseIntegration {

    private SimulationIntegrationConfig config;

    public static void main(String[] args) {
        ParameterTool param = ParameterTool.fromArgs(args);
        String sortieNumber = param.getRequired(
                HumanMachineIntegrationConfigKey.SORTIE_NUMBER.getKeyForParamsMap()
        );
        MissileIntegration missileIntegration = new MissileIntegration();
        missileIntegration.setConfig(new SimulationIntegrationConfig(sortieNumber));
        missileIntegration.start();
    }

    @Override
    public void run() {
        log.info("Start missile integration for sortie {}", config.getSortieNumber());

        // 2. 初始化 Flink 执行环境（批模式）
        StreamTableEnvironment tEnv = DataIntegrationUtil.prepareBatchTableEnv();

        // 3. 注册并使用 TiDB JDBC Catalog
        DataIntegrationUtil.createTiDBCatalog(tEnv);

        // 4. 枚举所有待整合的 TiDBTable 源表
        List<TiDBTable> allMissileTables = List.of(
                TiDBTable.AA_TRAJ,
                TiDBTable.AG_RTSN,
                TiDBTable.AG_TRAJ,
                TiDBTable.IR_MSL,
                TiDBTable.PL17_RTKN,
                TiDBTable.PL17_RTSN,
                TiDBTable.PL17_TRAJ,
                TiDBTable.RTSN,
                TiDBTable.RTKN,
                TiDBTable.SA_TRAJ
        );

        // 5. 统计每张表在本次 sortie 的数据量，仅保留 count > 0
        Map<TiDBTable, Long> tableCounts = SimulationIntegrationUtil.getTableCounts(allMissileTables, config.getSortieNumber());

        if (tableCounts.isEmpty()) {
            log.warn("No data found for sortie {}", config.getSortieNumber());
            return;
        }

        // 6. 根据最小行数选择主表
        TiDBTable mainTable = tableCounts.entrySet().stream()
                .min(Map.Entry.comparingByValue())
                .orElseThrow(() -> new ZorathosException("Unable to confirm main table for missile integration, all tables are empty."))
                .getKey();
        log.info("Chosen main table: {}", mainTable.getName());

        StringBuilder withClause = SimulationIntegrationUtil.generateWithClause(IntegrationType.SIMULATION_MISSILE, tableCounts, mainTable, config.getSortieNumber());

        // 9. 最终 INSERT
        String finalSql =
                "INSERT INTO `tidb_catalog`.`simulation_integration`.`missile_integration`\n"
                        + withClause
                        + """
                        SELECT sortie_number, event_ts, aircraft_id, message_time, satellite_guidance_time,
                               local_time, message_sequence_number, weapon_id, weapon_type, target_id,
                               pylon_id, longitude, latitude, altitude, heading, pitch, roll,
                               missile_speed, aircraft_ground_speed, north_speed, sky_speed, east_speed,
                               north_wind_speed, vertical_wind_speed, east_wind_speed,
                               aircraft_north_speed, aircraft_vertical_speed, aircraft_east_speed,
                               missile_target_distance, target_distance, target_longitude, target_latitude, target_altitude,
                               target_elevation_angle, target_azimuth_angle, target_tspi_status,
                               target_real_or_virtual, target_coordinate_validity, intercepted_weapon_id,
                               seeker_azimuth, seeker_elevation, seeker_id, seeker_head_number,
                               seeker_azimuth_center, seeker_pitch_center, interception_status, interception_flag,
                               non_interception_reason, hit_result, miss_reason, miss_distance,
                               matching_failure_reason, termination_flag, distance_interception_flag,
                               speed_interception_flag, angle_interception_flag, command_machine_status,
                               ground_angle_satisfaction_flag, zero_crossing_flag, aircraft_angle_of_attack,
                               impact_angle_validity, entry_angle, impact_angle, direction_validity,
                               missile_attack_mode, trajectory_type, jamming_effective, jamming,
                               afterburner, head_on, intercepting_member_id, intercepting_equipment_id,
                               intercepting_equipment_type, launcher_id, ground_defense_equipment_type,
                               ground_defense_equipment_id, '%s' AS source_table
                        FROM joined_data
                        ORDER BY event_ts
                        """.formatted(mainTable.getName());
        log.info("Final sql: {}", finalSql);
        tEnv.executeSql(finalSql);

        log.info("Missile data integration completed");
    }
}
