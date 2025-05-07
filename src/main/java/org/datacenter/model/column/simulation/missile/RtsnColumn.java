package org.datacenter.model.column.simulation.missile;

import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.datacenter.model.base.TiDBTable;
import org.datacenter.model.column.BaseColumn;

import java.util.ArrayList;

/**
 * @author : [wangminan]
 * @description : RTSN的列
 */
@EqualsAndHashCode(callSuper = true)
@Data
@Builder
public class RtsnColumn extends BaseColumn {
    public RtsnColumn() {
        this.table = TiDBTable.RTSN;
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
        columns.add("pylon_id");

        // 目标相关字段
        columns.add("target_real_or_virtual");
        columns.add("intercepted_weapon_id");

        // 轨迹相关字段
        columns.add("trajectory_type");
        columns.add("missile_attack_mode");

        columns.add("event_ts");
    }
}
