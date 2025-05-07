package org.datacenter.model.cte;

import org.datacenter.model.IntegrationType;
import org.datacenter.model.base.TiDBTable;
import org.datacenter.model.cte.simulation.missile.MissileTableCteCache;
import org.datacenter.model.cte.simulation.plane.PlaneStateTableCteCache;

import java.util.Map;

/**
 * @author : [wangminan]
 * @description : CTE工厂
 */
public class CteCacheFactory {

    public static Map<TiDBTable, TableCteDefinition> getCteCache(IntegrationType integrationType) {
        switch (integrationType) {
            case SIMULATION_MISSILE -> {
                return new MissileTableCteCache().getCteCache();
            }
            case SIMULATION_PLANE_STATE ->  {
                return new PlaneStateTableCteCache().getCteCache();
            }
            default -> {
                throw new IllegalArgumentException("Unsupported integration type: " + integrationType);
            }
        }
    }
}
