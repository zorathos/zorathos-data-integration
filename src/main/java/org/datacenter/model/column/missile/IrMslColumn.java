package org.datacenter.model.column.missile;

import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.datacenter.model.base.TiDBTable;
import org.datacenter.model.column.BaseColumn;

import java.util.ArrayList;

/**
 * @author : [wangminan]
 * @description : IR_MSL的列
 */
@EqualsAndHashCode(callSuper = true)
@Data
@Builder
public class IrMslColumn extends BaseColumn {
    public IrMslColumn() {
        this.table = TiDBTable.IR_MSL;
        this.columns = new ArrayList<>();

        // 基础字段
        columns.add("sortie_number");
        columns.add("aircraft_id");
        columns.add("message_time");
        columns.add("satellite_guidance_time");
        columns.add("local_time");
        columns.add("message_sequence_number");
        columns.add("weapon_type");

        // 导引头相关字段
        columns.add("seeker_azimuth");
        columns.add("seeker_elevation");

        // 截获相关字段
        columns.add("interception_flag");
        columns.add("event_ts");
    }
}
