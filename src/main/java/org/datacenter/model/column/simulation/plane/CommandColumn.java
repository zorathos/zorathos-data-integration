package org.datacenter.model.column.simulation.plane;

import org.datacenter.model.base.TiDBTable;
import org.datacenter.model.column.BaseColumn;

import java.util.ArrayList;

/**
 * @author : [wangminan]
 * @description : COMMAND表的列
 */
public class CommandColumn extends BaseColumn {
    public CommandColumn() {
        this.table = TiDBTable.COMMAND;
        this.columns = new ArrayList<>();
        columns.add("sortie_number");
        columns.add("aircraft_id");
        columns.add("message_time");
        columns.add("satellite_guidance_time");
        columns.add("local_time");
        columns.add("message_sequence_number");
        columns.add("command_type");
        columns.add("command_id");
        columns.add("command_content");
        columns.add("response_sequence_number");
        columns.add("event_ts");
    }
}
