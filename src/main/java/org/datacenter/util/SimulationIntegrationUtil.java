package org.datacenter.util;

import lombok.extern.slf4j.Slf4j;
import org.datacenter.config.HumanMachineConfig;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.datacenter.config.keys.HumanMachineSysConfigKey.TIDB_MYSQL_DRIVER_NAME;

/**
 * @author : [wangminan]
 * @description : 仿真数据整合工具
 */
@Slf4j
public class SimulationIntegrationUtil {
    /**
     * 统计每张表在本次 sortie 的数据量，仅保留 count > 0 之所以走JDBC是因为Flink执行完count时候就结束minicluster了
     *
     * @param allTables    missile条件下的所有表
     * @param sortieNumber 架次号
     * @return 表名和行数的映射
     */
    @NotNull
    public static Map<TiDBTable, Long> getTableCounts(List<TiDBTable> allTables, String sortieNumber) {
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
    private static String generateLagCte(IntegrationType integrationType, TiDBTable table, String sortieNumber) {
        return CteCacheFactory.getCteCache(integrationType)
                .get(table)
                .getLagCteTemplate()
                .formatted(sortieNumber);
    }

    /**
     * 生成 <table>_processed CTE
     */
    private static String generateProcessedCte(IntegrationType integrationType, TiDBTable table) {
        return CteCacheFactory.getCteCache(integrationType)
                .get(table)
                .getProcessedCteTemplate();
    }

    @NotNull
    public static StringBuilder generateWithClause(IntegrationType integrationType, Map<TiDBTable, Long> tableCounts, TiDBTable mainTable, String sortieNumber) {
        // 7. 动态生成 WITH 子句
        StringBuilder withClause = new StringBuilder("WITH\n");
        List<TiDBTable> tablesWithData = new ArrayList<>(tableCounts.keySet());
        for (TiDBTable tiDBTable : tablesWithData) {
            withClause
                    .append(generateLagCte(integrationType, tiDBTable, sortieNumber))
                    .append(generateProcessedCte(integrationType, tiDBTable));
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
        return withClause;
    }
}
