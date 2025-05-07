package org.datacenter.model.column.simulation.plane;

import org.datacenter.model.base.TiDBTable;
import org.datacenter.model.column.BaseColumn;

import java.util.ArrayList;

/**
 * @author : [wangminan]
 * @description : TSPI表的列
 */
public class TspiColumn extends BaseColumn {
    public TspiColumn() {
        this.table = TiDBTable.TSPI;
        this.columns = new ArrayList<>();
        columns.add("sortie_number");
        columns.add("aircraft_id");
        columns.add("aircraft_type");
        columns.add("message_time");
        columns.add("satellite_guidance_time");
        columns.add("local_time");
        columns.add("message_sequence_number");

        columns.add("longitude");
        columns.add("latitude");
        columns.add("pressure_altitude");
        columns.add("roll");
        columns.add("pitch");
        columns.add("heading");
        columns.add("satellite_altitude");
        columns.add("training_status");
        columns.add("chaff");
        columns.add("afterburner");
        columns.add("north_velocity");
        columns.add("vertical_velocity");
        columns.add("east_velocity");

        columns.add("event_ts");
    }
}
