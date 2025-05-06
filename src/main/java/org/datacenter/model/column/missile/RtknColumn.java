package org.datacenter.model.column.missile;

import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.datacenter.model.base.TiDBTable;
import org.datacenter.model.column.BaseColumn;

import java.util.ArrayList;

/**
 * @author : [wangminan]
 * @description : RTKN的列
 */
@EqualsAndHashCode(callSuper = true)
@Data
@Builder
public class RtknColumn extends BaseColumn {
    public RtknColumn() {
        this.table = TiDBTable.RTKN;
        this.columns = new ArrayList<>();

        // 基础字段
        columns.add("sortie_number");
        columns.add("aircraft_id");
        columns.add("message_time");
        columns.add("satellite_guidance_time");
        columns.add("local_time");
        columns.add("message_sequence_number");
        columns.add("weapon_id");
        columns.add("weapon_type");
        columns.add("target_id");

        // 目标相关字段
        columns.add("intercepted_weapon_id");

        // 命中相关字段
        columns.add("hit_result");
        columns.add("miss_reason");
        columns.add("miss_distance");
        columns.add("matching_failure_reason");

        // 防御设备相关字段
        columns.add("ground_defense_equipment_type");
        columns.add("ground_defense_equipment_id");
        columns.add("ground_defense_equipment_type1");
        columns.add("ground_defense_equipment_id1");
        columns.add("ground_defense_equipment_type2");
        columns.add("ground_defense_equipment_id2");
        columns.add("ground_defense_equipment_type3");
        columns.add("ground_defense_equipment_id3");

        // 干扰和状态相关字段
        columns.add("jamming_effective");
        columns.add("jamming");
        columns.add("afterburner");
        columns.add("head_on");

        // 姿态相关字段
        columns.add("heading");
        columns.add("pitch");

        columns.add("event_ts");
    }
}
