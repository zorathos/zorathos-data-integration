package org.datacenter.task.simulation;

import lombok.Data;
import lombok.EqualsAndHashCode;
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
import org.datacenter.model.column.ColumnFactory;
import org.datacenter.model.cte.missile.MissileTableCteCache;
import org.datacenter.task.BaseIntegration;
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
import java.util.List;
import java.util.Map;

import static org.datacenter.config.keys.HumanMachineSysConfigKey.TIDB_MYSQL_DRIVER_NAME;

/**
 * @author wangminan
 * @description 仿真导弹数据整合（使用 TiDBTable 枚举管理源表）
 */
@EqualsAndHashCode(callSuper = true)
@Slf4j
@Data
public class MissileIntegration extends BaseIntegration {

    private String sortieNumber;

    public static void main(String[] args) {
        ParameterTool param = ParameterTool.fromArgs(args);
        String sortieNumber = param.getRequired(
                HumanMachineIntegrationConfigKey.SORTIE_NUMBER.getKeyForParamsMap()
        );
        MissileIntegration missileIntegration = new MissileIntegration();
        missileIntegration.setSortieNumber(sortieNumber);
        missileIntegration.start();
    }

    /**
     * 统计每张表在本次 sortie 的数据量，仅保留 count > 0 之所以走JDBC是因为Flink执行完count时候就结束minicluster了
     *
     * @param allTables missile条件下的所有表
     * @return 表名和行数的映射
     */
    @NotNull
    private Map<TiDBTable, Long> getMissileTableCounts(List<TiDBTable> allTables) {
        MySQLDriverConnectionPool pool = new MySQLDriverConnectionPool(TiDBDatabase.SIMULATION);
        try {
            Class.forName(HumanMachineConfig.getProperty(TIDB_MYSQL_DRIVER_NAME));
        } catch (ClassNotFoundException e) {
            throw new ZorathosException("Can't find MySQL driver");
        }
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
    private String generateLagCte(TiDBTable table, String sortie) {
        return MissileTableCteCache.CTE_CACHE.get(table).getLagCteTemplate().formatted(sortie);
    }

    /**
     * 生成 <table>_processed CTE
     */
    private String generateProcessedCte(TiDBTable table) {
        return MissileTableCteCache.CTE_CACHE.get(table).getProcessedCteTemplate();
    }

    @Override
    public void run() {
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
        Map<TiDBTable, Long> tableCounts = getMissileTableCounts(allTables);

        if (tableCounts.isEmpty()) {
            log.warn("No data found for sortie {}", sortieNumber);
            return;
        }

        // 6. 根据最小行数选择主表
        TiDBTable mainTable = tableCounts.entrySet().stream()
                .min(Map.Entry.comparingByValue())
                .orElseThrow(() -> new ZorathosException("Unable to confirm main table, all tables are empty."))
                .getKey();
        log.info("Chosen main table: {}", mainTable.getName());

        // 7. 动态生成 WITH 子句
        StringBuilder withClause = new StringBuilder("WITH\n");
        List<TiDBTable> tablesWithData = new ArrayList<>(tableCounts.keySet());
        for (TiDBTable tiDBTable : tablesWithData) {
            withClause
                    .append(generateLagCte(tiDBTable, sortieNumber))
                    .append(generateProcessedCte(tiDBTable));
        }

        // 8. 构造 joined_data CTE：以主表为基准按 event_ts ±1s 左关联其它表
        // 8.1 构建select语句
        Map<String, List<String>> columnTableMap = new HashMap<>();
        BaseColumn mainTableColumn = ColumnFactory.getMissileColumn(mainTable);
        mainTableColumn.getColumns().forEach(column -> {
            List<String> tableList = new ArrayList<>();
            tableList.add(mainTable.getName() + "_processed");
            columnTableMap.put(column, tableList);
        });

        // 添加所有非主表的列
        for (TiDBTable otherTable : tablesWithData) {
            if (!otherTable.equals(mainTable)) {
                BaseColumn baseColumnForTable = ColumnFactory.getMissileColumn(otherTable);
                for (String column : baseColumnForTable.getColumns()) {
                    if (columnTableMap.containsKey(column)) {
                        // 已经有同名的列了
                        columnTableMap.get(column).add(otherTable.getName() + "_processed");
                    } else {
                        // 没有同名的列
                        List<String> tableList = new ArrayList<>();
                        tableList.add(otherTable.getName() + "_processed");
                        columnTableMap.put(column, tableList);
                    }
                }
            }
        }

        // 基于columnTableMap统一构造select
        withClause.append("joined_data AS (\n")
                .append("  SELECT ");

        columnTableMap.forEach((column, tableList) -> {
            if (tableList.size() == 1) {
                withClause.append(tableList.getFirst()).append(".").append(column).append(", ");
            } else {
                withClause.append("COALESCE(");
                for (int i = 0; i < tableList.size(); i++) {
                    String tableName = tableList.get(i);
                    if (i == tableList.size() - 1) {
                        withClause.append(tableName).append(".").append(column);
                    } else {
                        withClause.append(tableName).append(".").append(column).append(", ");
                    }
                }
                withClause.append(") AS ").append(column).append(", ");
            }
        });

        // 去掉最后一个逗号
        if (!withClause.isEmpty()) {
            withClause.delete(withClause.length() - 2, withClause.length());
        }

        withClause.append("\n  FROM ")
                .append(mainTable.getName()).append("_processed\n");

        // 8.2 对每个非主表进行LEFT JOIN
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
}
