package org.datacenter.model.column.missile;

import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.datacenter.model.base.TiDBTable;
import org.datacenter.model.column.BaseColumn;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

/**
 * @author : [wangminan]
 * @description : AG_TRAJ的列
 */
@EqualsAndHashCode(callSuper = true)
@Data
@Builder
public class AgTrajColumn extends BaseColumn {
    public AgTrajColumn() {
        this.table = TiDBTable.AG_TRAJ;
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

        // 位置相关字段
        columns.add("longitude");
        columns.add("latitude");
        columns.add("altitude");
        columns.add("heading");
        columns.add("pitch");

        // 速度相关字段
        columns.add("north_speed");
        columns.add("sky_speed");
        columns.add("east_speed");

        // 导引头相关字段
        columns.add("seeker_id");
        columns.add("seeker_azimuth_center");
        columns.add("seeker_pitch_center");

        // 截获相关字段
        columns.add("interception_flag");
        columns.add("termination_flag");
        columns.add("intercepting_member_id");
        columns.add("intercepting_equipment_id");
        columns.add("intercepting_equipment_type");
        columns.add("launcher_id");

        // 目标相关字段
        columns.add("missile_target_distance");
        columns.add("event_ts");
    }

    public static void main(String[] args) {
        AaTrajColumn aaTrajColumn = new AaTrajColumn();
        Set<String> usedColumns = new HashSet<>();
        usedColumns.addAll(aaTrajColumn.getColumns());
        System.out.println(aaTrajColumn.getColumnsSql(usedColumns));
    }
}
