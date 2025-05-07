package org.datacenter.model.column;

import org.datacenter.exception.ZorathosException;
import org.datacenter.model.base.TiDBTable;
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

import java.util.EnumMap;
import java.util.Map;

/**
 * @author : [wangminan]
 * @description : [列工厂]
 */
public class ColumnFactory {
    private static final Map<TiDBTable, BaseColumn> COLUMN_CACHE = new EnumMap<>(TiDBTable.class);

    public static BaseColumn getMissileColumn(TiDBTable table) {
        return COLUMN_CACHE.computeIfAbsent(table, t -> switch (table) {
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
        });
    }
}
