package org.datacenter.model.column.missile;

import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.datacenter.model.base.TiDBTable;
import org.datacenter.model.column.BaseColumn;

import java.util.ArrayList;

/**
 * @author : [wangminan]
 * @description : PL17_RTSN的列
 */
@EqualsAndHashCode(callSuper = true)
@Data
@Builder
public class Pl17RtsnColumn extends BaseColumn {
    public Pl17RtsnColumn() {
        this.table = TiDBTable.PL17_RTSN;
        this.columns = new ArrayList<>();

        // 基础字段
        columns.add("sortie_number");
        columns.add("aircraft_id");
        columns.add("message_time");
        columns.add("satellite_guidance_time");
        columns.add("local_time");
        columns.add("message_sequence_number");
        columns.add("target_id");
        columns.add("intercepted_weapon_id");
        columns.add("target_real_or_virtual");
        columns.add("weapon_id");
        columns.add("pylon_id");
        columns.add("weapon_type");

        // 轨迹相关字段
        columns.add("trajectory_type");
        columns.add("missile_attack_mode");

        columns.add("event_ts");
    }
}
