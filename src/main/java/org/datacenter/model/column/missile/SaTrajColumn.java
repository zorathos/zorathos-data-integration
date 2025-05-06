package org.datacenter.model.column.missile;

import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.datacenter.model.base.TiDBTable;
import org.datacenter.model.column.BaseColumn;

import java.util.ArrayList;

/**
 * @author : [wangminan]
 * @description : SA_TRAJ的列
 */
@EqualsAndHashCode(callSuper = true)
@Data
@Builder
public class SaTrajColumn extends BaseColumn {
    public SaTrajColumn() {
        this.table = TiDBTable.SA_TRAJ;
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

        // 位置相关字段
        columns.add("longitude");
        columns.add("latitude");
        columns.add("altitude");

        // 目标相关字段
        columns.add("missile_target_distance");
        columns.add("missile_speed");
        columns.add("intercepted_weapon_id");

        // 状态相关字段
        columns.add("interception_status");
        columns.add("non_interception_reason");
        columns.add("seeker_azimuth");
        columns.add("seeker_elevation");
        columns.add("target_tspi_status");
        columns.add("command_machine_status");

        columns.add("event_ts");
    }
}
