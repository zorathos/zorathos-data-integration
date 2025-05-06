package org.datacenter.model.column.missile;

import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.datacenter.model.base.TiDBTable;
import org.datacenter.model.column.BaseColumn;

import java.util.ArrayList;

/**
 * @author : [wangminan]
 * @description : AG_RTSN的列
 */
@EqualsAndHashCode(callSuper = true)
@Data
@Builder
public class AgRtsnColumn extends BaseColumn {
    public AgRtsnColumn() {
        this.table = TiDBTable.AG_RTSN;
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

        // 位置相关字段
        columns.add("target_longitude");
        columns.add("target_latitude");
        columns.add("target_altitude");
        columns.add("heading");
        columns.add("roll");
        columns.add("pitch");

        // 速度相关字段
        columns.add("aircraft_ground_speed");
        columns.add("aircraft_north_speed");
        columns.add("aircraft_vertical_speed");
        columns.add("aircraft_east_speed");
        columns.add("north_wind_speed");
        columns.add("vertical_wind_speed");
        columns.add("east_wind_speed");

        // 目标相关字段
        columns.add("target_distance");
        columns.add("target_elevation_angle");
        columns.add("target_azimuth_angle");
        columns.add("target_coordinate_validity");

        // 其他相关字段
        columns.add("seeker_head_number");
        columns.add("impact_angle_validity");
        columns.add("entry_angle");
        columns.add("impact_angle");
        columns.add("direction_validity");
        columns.add("aircraft_angle_of_attack");
        columns.add("event_ts");
    }
}
