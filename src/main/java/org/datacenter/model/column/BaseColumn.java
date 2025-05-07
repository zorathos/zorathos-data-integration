package org.datacenter.model.column;

import lombok.Data;
import org.datacenter.model.base.TiDBTable;

import java.util.List;

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
}
