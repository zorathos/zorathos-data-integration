package org.datacenter.model.cte;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @author : [wangminan]
 * @description : [一句话描述该类的功能]
 */
@Data
@AllArgsConstructor
public class TableCteDefinition {
    private String lagCteTemplate;
    private String processedCteTemplate;
}
