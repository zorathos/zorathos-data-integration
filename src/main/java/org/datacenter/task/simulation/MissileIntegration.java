package org.datacenter.task.simulation;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.datacenter.config.HumanMachineConfig;
import org.datacenter.config.keys.HumanMachineIntegrationConfigKey;
import org.datacenter.config.keys.HumanMachineSysConfigKey;
import org.datacenter.exception.ZorathosException;
import org.datacenter.model.base.TiDBDatabase;
import org.datacenter.model.base.TiDBTable;
import org.datacenter.model.column.BaseColumn;
import org.datacenter.model.column.missile.AaTrajColumn;
import org.datacenter.model.column.missile.AgRtsnColumn;
import org.datacenter.model.column.missile.AgTrajColumn;
import org.datacenter.model.column.missile.IrMslColumn;
import org.datacenter.model.column.missile.Pl17RtknColumn;
import org.datacenter.model.column.missile.Pl17RtsnColumn;
import org.datacenter.model.column.missile.Pl17TrajColumn;
import org.datacenter.model.column.missile.RtknColumn;
import org.datacenter.model.column.missile.RtsnColumn;
import org.datacenter.model.column.missile.SaTrajColumn;
import org.datacenter.util.DataIntegrationUtil;
import org.datacenter.util.MySQLDriverConnectionPool;
import org.jetbrains.annotations.NotNull;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.datacenter.config.keys.HumanMachineSysConfigKey.TIDB_MYSQL_DRIVER_NAME;

/**
 * @author wangminan
 * @description 仿真导弹数据整合（使用 TiDBTable 枚举管理源表）
 */
@Slf4j
public class MissileIntegration {

    private static final Set<String> usedColumns = new HashSet<>();

    public static void main(String[] args) throws Exception {
        // 1. 准备阶段
        // 1.1 加载全局配置 & 参数
        HumanMachineConfig config = new HumanMachineConfig();
        config.loadConfig();
        ParameterTool param = ParameterTool.fromArgs(args);
        String sortieNumber = param.getRequired(
                HumanMachineIntegrationConfigKey.SORTIE_NUMBER.getKeyForParamsMap()
        );

        log.info("Start missile integration for sortie {}", sortieNumber);

        // 2. 初始化 Flink 执行环境（批模式）
        StreamExecutionEnvironment env = DataIntegrationUtil.prepareStreamEnv();
        env.setParallelism(1);
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 3. 注册并使用 TiDB JDBC Catalog
        tEnv.executeSql(String.format(
                "CREATE CATALOG tidb_catalog WITH (" +
                        "  'type' = 'jdbc'," +
                        "  'default-database' = '%s'," +
                        "  'username' = '%s'," +
                        "  'password' = '%s'," +
                        "  'base-url' = '%s'" +
                        ")",
                TiDBDatabase.SIMULATION.getName(),
                HumanMachineConfig.getProperty(HumanMachineSysConfigKey.TIDB_USERNAME),
                HumanMachineConfig.getProperty(HumanMachineSysConfigKey.TIDB_PASSWORD),
                HumanMachineConfig.getProperty(HumanMachineSysConfigKey.TIDB_URL_PREFIX)
        ));
        tEnv.executeSql("USE CATALOG tidb_catalog");

        // 4. 枚举所有待整合的 TiDBTable 源表
        List<TiDBTable> allTables = Arrays.asList(
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
        Map<TiDBTable, Long> tableCounts = getMissileTableCounts(allTables, sortieNumber);

        if (tableCounts.isEmpty()) {
            log.warn("No data found for sortie {}", sortieNumber);
            return;
        }

        // 6. 根据最小行数选择主表
        TiDBTable mainTable = tableCounts.entrySet().stream()
                .min(Map.Entry.comparingByValue())
                .get()
                .getKey();
        log.info("Chosen main table: {}", mainTable.getName());

        // 7. 动态生成 WITH 子句
        StringBuilder withClause = new StringBuilder("WITH\n");
        List<TiDBTable> tablesWithData = new ArrayList<>(tableCounts.keySet());
        for (TiDBTable tbl : tablesWithData) {
            String name = tbl.getName();
            withClause
                    .append(generateLagCte(name, sortieNumber))
                    .append(",\n")
                    .append(generateProcessedCte(name))
                    .append(",\n");
        }

        // 8. 构造 joined_data CTE：以主表为基准按 event_ts ±1s 左关联其它表
        BaseColumn mainTableColumn = getBaseColumnForTable(mainTable);
        withClause.append("joined_data AS (\n")
                .append("  SELECT ")
                .append(mainTableColumn.getColumnsSql(usedColumns));

        usedColumns.addAll(mainTableColumn.getColumns());

        // 添加所有非主表的列
        for (TiDBTable otherTable : tablesWithData) {
            if (!otherTable.equals(mainTable)) {
                BaseColumn baseColumnForTable = getBaseColumnForTable(otherTable);
                if (!baseColumnForTable.getColumnsSql(usedColumns).isEmpty()) {
                    withClause.append(",\n        ")
                            .append(baseColumnForTable.getColumnsSql(usedColumns));
                    usedColumns.addAll(baseColumnForTable.getColumns());
                }
            }
        }

        withClause.append("\n  FROM ")
                .append(mainTable.getName()).append("_processed\n");

        // 对每个非主表进行LEFT JOIN
        for (TiDBTable tbl : tablesWithData) {
            if (!tbl.equals(mainTable)) {
                String tableName = tbl.getName() + "_processed";
                withClause.append("  LEFT JOIN ")
                        .append(tableName).append("\n")
                        .append("    ON ").append(mainTable.getName()).append("_processed.sortie_number = ")
                        .append(tableName).append(".sortie_number\n")
                        .append("   AND ").append(tableName)
                        .append(".event_ts BETWEEN ").append(mainTable.getName())
                        .append("_processed.event_ts - INTERVAL '1' SECOND AND ")
                        .append(mainTable.getName()).append("_processed.event_ts + INTERVAL '1' SECOND\n");
            }
        }
        withClause.append(")\n");

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

    private static BaseColumn getBaseColumnForTable(TiDBTable table) {
        return switch (table) {
            case AA_TRAJ -> new AaTrajColumn();
            case AG_RTSN -> new AgRtsnColumn();
            case AG_TRAJ -> new AgTrajColumn();
            case IR_MSL -> new IrMslColumn();
            case PL17_RTKN -> new Pl17RtknColumn();
            case PL17_RTSN -> new Pl17RtsnColumn();
            case PL17_TRAJ -> new Pl17TrajColumn();
            case RTKN -> new RtknColumn();
            case RTSN -> new RtsnColumn();
            case SA_TRAJ -> new SaTrajColumn();
            default -> throw new ZorathosException("Unsupported table for missile: " + table);
        };
    }

    /**
     * 统计每张表在本次 sortie 的数据量，仅保留 count > 0 之所以走JDBC是因为Flink执行完count时候就结束minicluster了
     *
     * @param allTables    missile条件下的所有表
     * @param sortieNumber 架次
     * @return 表名和行数的映射
     * @throws ClassNotFoundException JDBC驱动类未找到异常
     */
    @NotNull
    private static Map<TiDBTable, Long> getMissileTableCounts(List<TiDBTable> allTables, String sortieNumber) throws ClassNotFoundException {
        MySQLDriverConnectionPool pool = new MySQLDriverConnectionPool(TiDBDatabase.SIMULATION);
        Class.forName(HumanMachineConfig.getProperty(TIDB_MYSQL_DRIVER_NAME));
        Connection conn = pool.getConnection();
        Map<TiDBTable, Long> tableCounts = new HashMap<>();
        try {
            String countSql = "SELECT COUNT(*) AS cnt FROM `%s` WHERE sortie_number = ?";
            for (TiDBTable tbl : allTables) {
                String tblName = tbl.getName();
                try (PreparedStatement ps = conn.prepareStatement(
                        String.format(countSql, tblName)
                )) {
                    ps.setString(1, sortieNumber);
                    try (ResultSet rs = ps.executeQuery()) {
                        if (rs.next()) {
                            long cnt = rs.getLong("cnt");
                            if (cnt > 0) {
                                tableCounts.put(tbl, cnt);
                                log.info("Table {} has {} rows for sortie {}", tblName, cnt, sortieNumber);
                            }
                        }
                    }
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to count rows via JDBC", e);
        } finally {
            // 4.2 关闭或归还连接
            pool.returnConnection(conn);
            pool.closePool();
        }
        return tableCounts;
    }


    /**
     * 生成 <table>_lag CTE
     */
    private static String generateLagCte(String table, String sortie) {
        return switch (table) {
            case "aa_traj" -> String.format("""
                    aa_traj_lag AS (
                      SELECT sortie_number, aircraft_id, message_time, satellite_guidance_time, local_time,
                             message_sequence_number, weapon_id, pylon_id, weapon_type, target_id,
                             longitude, latitude, altitude, missile_target_distance, missile_speed,
                             interception_status, non_interception_reason, seeker_azimuth, seeker_elevation,
                             target_tspi_status, command_machine_status, ground_angle_satisfaction_flag, zero_crossing_flag,
                             LAG(satellite_guidance_time) OVER (PARTITION BY sortie_number ORDER BY message_sequence_number) AS prev_sgt
                      FROM `tidb_catalog`.`simulation`.`aa_traj`
                      WHERE sortie_number = '%s'
                    )""", sortie);
            case "ag_rtsn" -> String.format("""
                    ag_rtsn_lag AS (
                      SELECT sortie_number, aircraft_id, message_time, satellite_guidance_time, local_time,
                             message_sequence_number, weapon_id, weapon_pylon_id AS pylon_id, weapon_type,
                             number_of_missiles_released, aircraft_ground_speed, aircraft_longitude AS longitude,
                             aircraft_latitude AS latitude, aircraft_altitude AS altitude, aircraft_heading AS heading,
                             aircraft_pitch AS pitch, aircraft_roll AS roll, aircraft_angle_of_attack,
                             aircraft_north_speed, aircraft_vertical_speed, aircraft_east_speed,
                             north_wind_speed, vertical_wind_speed, east_wind_speed,
                             target_longitude, target_latitude, target_altitude, target_distance,
                             seeker_head_number, target_coordinate_validity, target_azimuth_elevation_validity,
                             target_elevation_angle, target_azimuth_angle, impact_angle_validity, entry_angle,
                             impact_angle, direction_validity,
                             LAG(satellite_guidance_time) OVER (PARTITION BY sortie_number ORDER BY message_sequence_number) AS prev_sgt
                      FROM `tidb_catalog`.`simulation`.`ag_rtsn`
                      WHERE sortie_number = '%s'
                    )""", sortie);
            case "ag_traj" -> String.format("""
                    ag_traj_lag AS (
                      SELECT sortie_number, aircraft_id, message_time, satellite_guidance_time, local_time,
                             message_sequence_number, weapon_id, weapon_type, longitude, latitude, altitude,
                             heading, pitch, north_speed, sky_speed, east_speed, seeker_id, interception_flag,
                             termination_flag, intercepting_member_id, intercepting_equipment_id,
                             intercepting_equipment_type, launcher_id, seeker_azimuth_center, seeker_pitch_center,
                             target_id, missile_target_distance,
                             LAG(satellite_guidance_time) OVER (PARTITION BY sortie_number ORDER BY message_sequence_number) AS prev_sgt
                      FROM `tidb_catalog`.`simulation`.`ag_traj`
                      WHERE sortie_number = '%s'
                    )""", sortie);
            case "ir_msl" -> String.format("""
                    ir_msl_lag AS (
                      SELECT sortie_number, aircraft_id, message_time, satellite_guidance_time, local_time,
                             message_sequence_number, seeker_azimuth, seeker_elevation, weapon_type, interception_flag,
                             LAG(satellite_guidance_time) OVER (PARTITION BY sortie_number ORDER BY message_sequence_number) AS prev_sgt
                      FROM `tidb_catalog`.`simulation`.`ir_msl`
                      WHERE sortie_number = '%s'
                    )""", sortie);
            case "pl17_rtkn" -> String.format("""
                    pl17_rtkn_lag AS (
                      SELECT sortie_number, aircraft_id, message_time, satellite_guidance_time, local_time,
                             message_sequence_number, weapon_id, weapon_type, target_id, target_real_or_virtual,
                             hit_result, miss_reason, miss_distance, matching_failure_reason, jamming_effective,
                             jamming, afterburner, head_on, heading, pitch,
                             LAG(satellite_guidance_time) OVER (PARTITION BY sortie_number ORDER BY message_sequence_number) AS prev_sgt
                      FROM `tidb_catalog`.`simulation`.`pl17_rtkn`
                      WHERE sortie_number = '%s'
                    )""", sortie);
            case "pl17_rtsn" -> String.format("""
                    pl17_rtsn_lag AS (
                      SELECT sortie_number, aircraft_id, message_time, satellite_guidance_time, local_time,
                             message_sequence_number, target_id, intercepted_weapon_id, target_real_or_virtual,
                             weapon_id, pylon_id, weapon_type, trajectory_type, missile_attack_mode,
                             LAG(satellite_guidance_time) OVER (PARTITION BY sortie_number ORDER BY message_sequence_number) AS prev_sgt
                      FROM `tidb_catalog`.`simulation`.`pl17_rtsn`
                      WHERE sortie_number = '%s'
                    )""", sortie);
            case "pl17_traj" -> String.format("""
                    pl17_traj_lag AS (
                      SELECT sortie_number, aircraft_id, message_time, satellite_guidance_time, local_time,
                             message_sequence_number, weapon_id, pylon_id, weapon_type, target_id,
                             longitude, latitude, altitude, missile_target_distance, missile_speed,
                             interception_status, non_interception_reason, seeker_azimuth, seeker_elevation,
                             target_tspi_status, command_machine_status, ground_angle_satisfaction_flag,
                             zero_crossing_flag, distance_interception_flag, speed_interception_flag, angle_interception_flag,
                             LAG(satellite_guidance_time) OVER (PARTITION BY sortie_number ORDER BY message_sequence_number) AS prev_sgt
                      FROM `tidb_catalog`.`simulation`.`pl17_traj`
                      WHERE sortie_number = '%s'
                    )""", sortie);
            case "rtsn" -> String.format("""
                    rtsn_lag AS (
                      SELECT sortie_number, aircraft_id, message_time, satellite_guidance_time, local_time,
                             message_sequence_number, target_id, intercepted_weapon_id, target_real_or_virtual,
                             weapon_id, pylon_id, weapon_type, trajectory_type, missile_attack_mode,
                             LAG(satellite_guidance_time) OVER (PARTITION BY sortie_number ORDER BY message_sequence_number) AS prev_sgt
                      FROM `tidb_catalog`.`simulation`.`rtsn`
                      WHERE sortie_number = '%s'
                    )""", sortie);
            case "rtkn" -> String.format("""
                    rtkn_lag AS (
                      SELECT sortie_number, aircraft_id, message_time, satellite_guidance_time, local_time,
                             message_sequence_number, weapon_id, weapon_type, target_id, intercepted_weapon_id,
                             hit_result, miss_reason, miss_distance, matching_failure_reason,
                             ground_defense_equipment_type, ground_defense_equipment_id,
                             ground_defense_equipment_type1, ground_defense_equipment_id1,
                             ground_defense_equipment_type2, ground_defense_equipment_id2,
                             ground_defense_equipment_type3, ground_defense_equipment_id3,
                             jamming_effective, jamming, afterburner, head_on, heading, pitch,
                             LAG(satellite_guidance_time) OVER (PARTITION BY sortie_number ORDER BY message_sequence_number) AS prev_sgt
                      FROM `tidb_catalog`.`simulation`.`rtkn`
                      WHERE sortie_number = '%s'
                    )""", sortie);
            case "sa_traj" -> String.format("""
                    sa_traj_lag AS (
                      SELECT sortie_number, aircraft_id, message_time, satellite_guidance_time, local_time,
                             message_sequence_number, weapon_id, pylon_id, weapon_type, target_id,
                             intercepted_weapon_id, longitude, latitude, altitude, missile_target_distance,
                             missile_speed, interception_status, non_interception_reason, seeker_azimuth,
                             seeker_elevation, target_tspi_status, command_machine_status,
                             LAG(satellite_guidance_time) OVER (PARTITION BY sortie_number ORDER BY message_sequence_number) AS prev_sgt
                      FROM `tidb_catalog`.`simulation`.`sa_traj`
                      WHERE sortie_number = '%s'
                    )""", sortie);
            default -> throw new IllegalArgumentException("Unknown table: " + table);
        };
    }

    /**
     * 生成 <table>_processed CTE
     */
    private static String generateProcessedCte(String table) {
        return switch (table) {
            case "aa_traj" -> """
                    aa_traj_processed AS (
                      SELECT sortie_number, aircraft_id, message_time, satellite_guidance_time, local_time,
                             message_sequence_number, weapon_id, pylon_id, weapon_type, target_id,
                             longitude, latitude, altitude, missile_target_distance, missile_speed,
                             interception_status, non_interception_reason, seeker_azimuth, seeker_elevation,
                             target_tspi_status, command_machine_status, ground_angle_satisfaction_flag, zero_crossing_flag,
                             CAST(satellite_guidance_time AS TIMESTAMP(3)) +
                             INTERVAL '1' DAY * SUM(CASE WHEN satellite_guidance_time < prev_sgt THEN 1 ELSE 0 END)
                               OVER (PARTITION BY sortie_number ORDER BY message_sequence_number ROWS UNBOUNDED PRECEDING)
                             AS event_ts,
                             'aa_traj' AS source_table
                      FROM aa_traj_lag
                    )""";
            case "ag_rtsn" -> """
                    ag_rtsn_processed AS (
                      SELECT sortie_number, aircraft_id, message_time, satellite_guidance_time, local_time,
                             message_sequence_number, weapon_id, pylon_id, weapon_type,
                             number_of_missiles_released, aircraft_ground_speed, longitude,
                             latitude, altitude, heading, pitch, roll, aircraft_angle_of_attack,
                             aircraft_north_speed, aircraft_vertical_speed, aircraft_east_speed,
                             north_wind_speed, vertical_wind_speed, east_wind_speed,
                             target_longitude, target_latitude, target_altitude, target_distance,
                             seeker_head_number, target_coordinate_validity, target_azimuth_elevation_validity,
                             target_elevation_angle, target_azimuth_angle, impact_angle_validity, entry_angle,
                             impact_angle, direction_validity,
                             CAST(satellite_guidance_time AS TIMESTAMP(3)) +
                             INTERVAL '1' DAY * SUM(CASE WHEN satellite_guidance_time < prev_sgt THEN 1 ELSE 0 END)
                               OVER (PARTITION BY sortie_number ORDER BY message_sequence_number ROWS UNBOUNDED PRECEDING)
                             AS event_ts,
                             'ag_rtsn' AS source_table
                      FROM ag_rtsn_lag
                    )""";
            case "ag_traj" -> """
                    ag_traj_processed AS (
                      SELECT sortie_number, aircraft_id, message_time, satellite_guidance_time, local_time,
                             message_sequence_number, weapon_id, weapon_type, longitude, latitude, altitude,
                             heading, pitch, north_speed, sky_speed, east_speed, seeker_id, interception_flag,
                             termination_flag, intercepting_member_id, intercepting_equipment_id,
                             intercepting_equipment_type, launcher_id, seeker_azimuth_center, seeker_pitch_center,
                             target_id, missile_target_distance,
                             CAST(satellite_guidance_time AS TIMESTAMP(3)) +
                             INTERVAL '1' DAY * SUM(CASE WHEN satellite_guidance_time < prev_sgt THEN 1 ELSE 0 END)
                               OVER (PARTITION BY sortie_number ORDER BY message_sequence_number ROWS UNBOUNDED PRECEDING)
                             AS event_ts,
                             'ag_traj' AS source_table
                      FROM ag_traj_lag
                    )""";
            case "ir_msl" -> """
                    ir_msl_processed AS (
                      SELECT sortie_number, aircraft_id, message_time, satellite_guidance_time, local_time,
                             message_sequence_number, seeker_azimuth, seeker_elevation, weapon_type, interception_flag,
                             CAST(satellite_guidance_time AS TIMESTAMP(3)) +
                             INTERVAL '1' DAY * SUM(CASE WHEN satellite_guidance_time < prev_sgt THEN 1 ELSE 0 END)
                               OVER (PARTITION BY sortie_number ORDER BY message_sequence_number ROWS UNBOUNDED PRECEDING)
                             AS event_ts,
                             'ir_msl' AS source_table
                      FROM ir_msl_lag
                    )""";
            case "pl17_rtkn" -> """
                    pl17_rtkn_processed AS (
                      SELECT sortie_number, aircraft_id, message_time, satellite_guidance_time, local_time,
                             message_sequence_number, weapon_id, weapon_type, target_id, target_real_or_virtual,
                             hit_result, miss_reason, miss_distance, matching_failure_reason, jamming_effective,
                             jamming, afterburner, head_on, heading, pitch,
                             CAST(satellite_guidance_time AS TIMESTAMP(3)) +
                             INTERVAL '1' DAY * SUM(CASE WHEN satellite_guidance_time < prev_sgt THEN 1 ELSE 0 END)
                               OVER (PARTITION BY sortie_number ORDER BY message_sequence_number ROWS UNBOUNDED PRECEDING)
                             AS event_ts,
                             'pl17_rtkn' AS source_table
                      FROM pl17_rtkn_lag
                    )""";
            case "pl17_rtsn" -> """
                    pl17_rtsn_processed AS (
                      SELECT sortie_number, aircraft_id, message_time, satellite_guidance_time, local_time,
                             message_sequence_number, target_id, intercepted_weapon_id, target_real_or_virtual,
                             weapon_id, pylon_id, weapon_type, trajectory_type, missile_attack_mode,
                             CAST(satellite_guidance_time AS TIMESTAMP(3)) +
                             INTERVAL '1' DAY * SUM(CASE WHEN satellite_guidance_time < prev_sgt THEN 1 ELSE 0 END)
                               OVER (PARTITION BY sortie_number ORDER BY message_sequence_number ROWS UNBOUNDED PRECEDING)
                             AS event_ts,
                             'pl17_rtsn' AS source_table
                      FROM pl17_rtsn_lag
                    )""";
            case "pl17_traj" -> """
                    pl17_traj_processed AS (
                      SELECT sortie_number, aircraft_id, message_time, satellite_guidance_time, local_time,
                             message_sequence_number, weapon_id, pylon_id, weapon_type, target_id,
                             longitude, latitude, altitude, missile_target_distance, missile_speed,
                             interception_status, non_interception_reason, seeker_azimuth, seeker_elevation,
                             target_tspi_status, command_machine_status, ground_angle_satisfaction_flag,
                             zero_crossing_flag, distance_interception_flag, speed_interception_flag, angle_interception_flag,
                             CAST(satellite_guidance_time AS TIMESTAMP(3)) +
                             INTERVAL '1' DAY * SUM(CASE WHEN satellite_guidance_time < prev_sgt THEN 1 ELSE 0 END)
                               OVER (PARTITION BY sortie_number ORDER BY message_sequence_number ROWS UNBOUNDED PRECEDING)
                             AS event_ts,
                             'pl17_traj' AS source_table
                      FROM pl17_traj_lag
                    )""";
            case "rtsn" -> """
                    rtsn_processed AS (
                      SELECT sortie_number, aircraft_id, message_time, satellite_guidance_time, local_time,
                             message_sequence_number, target_id, intercepted_weapon_id, target_real_or_virtual,
                             weapon_id, pylon_id, weapon_type, trajectory_type, missile_attack_mode,
                             CAST(satellite_guidance_time AS TIMESTAMP(3)) +
                             INTERVAL '1' DAY * SUM(CASE WHEN satellite_guidance_time < prev_sgt THEN 1 ELSE 0 END)
                               OVER (PARTITION BY sortie_number ORDER BY message_sequence_number ROWS UNBOUNDED PRECEDING)
                             AS event_ts,
                             'rtsn' AS source_table
                      FROM rtsn_lag
                    )""";
            case "rtkn" -> """
                    rtkn_processed AS (
                      SELECT sortie_number, aircraft_id, message_time, satellite_guidance_time, local_time,
                             message_sequence_number, weapon_id, weapon_type, target_id, intercepted_weapon_id,
                             hit_result, miss_reason, miss_distance, matching_failure_reason,
                             ground_defense_equipment_type, ground_defense_equipment_id,
                             ground_defense_equipment_type1, ground_defense_equipment_id1,
                             ground_defense_equipment_type2, ground_defense_equipment_id2,
                             ground_defense_equipment_type3, ground_defense_equipment_id3,
                             jamming_effective, jamming, afterburner, head_on, heading, pitch,
                             CAST(satellite_guidance_time AS TIMESTAMP(3)) +
                             INTERVAL '1' DAY * SUM(CASE WHEN satellite_guidance_time < prev_sgt THEN 1 ELSE 0 END)
                               OVER (PARTITION BY sortie_number ORDER BY message_sequence_number ROWS UNBOUNDED PRECEDING)
                             AS event_ts,
                             'rtkn' AS source_table
                      FROM rtkn_lag
                    )""";
            case "sa_traj" -> """
                    sa_traj_processed AS (
                      SELECT sortie_number, aircraft_id, message_time, satellite_guidance_time, local_time,
                             message_sequence_number, weapon_id, pylon_id, weapon_type, target_id,
                             intercepted_weapon_id, longitude, latitude, altitude, missile_target_distance,
                             missile_speed, interception_status, non_interception_reason, seeker_azimuth,
                             seeker_elevation, target_tspi_status, command_machine_status,
                             CAST(satellite_guidance_time AS TIMESTAMP(3)) +
                             INTERVAL '1' DAY * SUM(CASE WHEN satellite_guidance_time < prev_sgt THEN 1 ELSE 0 END)
                               OVER (PARTITION BY sortie_number ORDER BY message_sequence_number ROWS UNBOUNDED PRECEDING)
                             AS event_ts,
                             'sa_traj' AS source_table
                      FROM sa_traj_lag
                    )""";
            default -> throw new IllegalArgumentException("Unknown table: " + table);
        };
    }
}
