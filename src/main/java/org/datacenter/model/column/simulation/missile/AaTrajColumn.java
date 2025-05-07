package org.datacenter.model.column.simulation.missile;

import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.datacenter.model.base.TiDBTable;
import org.datacenter.model.column.BaseColumn;

import java.util.ArrayList;

/**
 * @author : [wangminan]
 * @description : aaTraj的列
 */
@EqualsAndHashCode(callSuper = true)
@Data
@Builder
public class AaTrajColumn extends BaseColumn {
    public AaTrajColumn() {
        this.table = TiDBTable.AA_TRAJ;
        this.columns = new ArrayList<>();
        // 基础字段
        columns.add("sortie_number");
        columns.add("event_ts");
        columns.add("aircraft_id");
        columns.add("message_time");
        columns.add("satellite_guidance_time");
        columns.add("local_time");
        columns.add("message_sequence_number");
        columns.add("weapon_id");
        columns.add("weapon_type");
        columns.add("target_id");
        columns.add("pylon_id");

        // 位置相关字段
        columns.add("longitude");
        columns.add("latitude");
        columns.add("altitude");

        // 速度相关字段
        columns.add("missile_speed");

        // 目标相关字段
        columns.add("missile_target_distance");
        columns.add("target_tspi_status");

        // 导引头相关字段
        columns.add("seeker_azimuth");
        columns.add("seeker_elevation");

        // 截获/命中相关字段
        columns.add("interception_status");
        columns.add("non_interception_reason");

        // 其他状态和标志
        columns.add("command_machine_status");
        columns.add("ground_angle_satisfaction_flag");
        columns.add("zero_crossing_flag");
    }
}
