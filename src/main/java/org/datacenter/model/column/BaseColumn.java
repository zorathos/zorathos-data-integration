package org.datacenter.model.column;

import lombok.Data;
import org.datacenter.model.base.TiDBTable;

import java.util.List;
import java.util.Set;

/**
 * @author : [wangminan]
 * @description : 基础列
 */
@Data
public abstract class BaseColumn {

    protected TiDBTable table;

    /**
     * 需要引入的列
     */
    protected List<String> columns;

    public String getColumnsSql(Set<String> usedColumns) {
        // 用逗号连接列名
        StringBuilder sb = new StringBuilder();
        for (String column : columns) {
            if (!usedColumns.contains(column)) {
                sb.append(table.getName())
                        .append("_processed.")
                        .append(column)
                        .append(", ");
            }
        }
        // 去掉最后一个逗号
        if (!sb.isEmpty()) {
            sb.deleteCharAt(sb.length() - 2);
        }
        return sb.toString();
    }
}
