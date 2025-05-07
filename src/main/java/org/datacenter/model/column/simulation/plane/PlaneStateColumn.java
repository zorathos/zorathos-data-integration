package org.datacenter.model.column.simulation.plane;

import org.datacenter.model.base.TiDBTable;
import org.datacenter.model.column.BaseColumn;

import java.util.ArrayList;

/**
 * @author : [wangminan]
 * @description : PLANE_STATE表的列
 */
public class PlaneStateColumn extends BaseColumn {
    public PlaneStateColumn() {
        this.table = TiDBTable.PLANE_STATE;
        this.columns = new ArrayList<>();
        columns.add("sortie_number");
        columns.add("aircraft_id");
        columns.add("message_time");
        columns.add("satellite_guidance_time");
        columns.add("local_time");
        columns.add("message_sequence_number");

        columns.add("true_angle_of_attack");
        columns.add("mach_number");
        columns.add("normal_load_factor");
        columns.add("indicated_airspeed");
        columns.add("field_elevation");
        columns.add("radio_altitude");
        columns.add("remaining_fuel");
        columns.add("scenario");
        columns.add("manual_respawn");
        columns.add("parameter_setting_status");
        columns.add("encryption_status");

        columns.add("event_ts");
    }
}
