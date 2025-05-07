package org.datacenter.model.cte.simulation;

import lombok.Data;
import org.datacenter.model.base.TiDBTable;
import org.datacenter.model.cte.TableCteDefinition;

import java.util.HashMap;
import java.util.Map;

/**
 * @author : [wangminan]
 * @description : [只是需要一个缓存基类]
 */
@Data
public class BaseCteCache {
    protected final Map<TiDBTable, TableCteDefinition> cteCache = new HashMap<>();
}
