package org.datacenter.model.cte.simulation.plane;

import org.datacenter.model.base.TiDBTable;
import org.datacenter.model.cte.TableCteDefinition;
import org.datacenter.model.cte.simulation.BaseCteCache;

/**
 * @author : [wangminan]
 * @description : 飞机状态表CTE模板缓存
 */
public class PlaneStateTableCteCache extends BaseCteCache {
    public PlaneStateTableCteCache() {
        // 初始化缓存
        cteCache.put(TiDBTable.CD_DRONE_TSPI, new TableCteDefinition(
                """
                        cd_drone_tspi_lag AS (
                        SELECT sortie_number, aircraft_id, aircraft_type, message_time, satellite_guidance_time, local_time,
                              message_sequence_number, longitude, latitude, pressure_altitude, roll, pitch, heading,
                              satellite_altitude, training_status, chaff, afterburner, north_velocity,
                              vertical_velocity, east_velocity, delay_status,
                              LAG(satellite_guidance_time) OVER (PARTITION BY sortie_number ORDER BY message_sequence_number) AS prev_sgt
                        FROM `tidb_catalog`.`simulation`.`cd_drone_tspi`
                        WHERE sortie_number = '%s'
                        ),
                        """,
                """
                        cd_drone_tspi_processed AS (
                        SELECT sortie_number, aircraft_id, aircraft_type, message_time, satellite_guidance_time, local_time,
                              message_sequence_number, longitude, latitude, pressure_altitude, roll, pitch, heading,
                              satellite_altitude, training_status, chaff, afterburner, north_velocity,
                              vertical_velocity, east_velocity, delay_status,
                              CAST(satellite_guidance_time AS TIMESTAMP(3)) +
                              INTERVAL '1' DAY * SUM(CASE WHEN satellite_guidance_time < prev_sgt THEN 1 ELSE 0 END)
                                OVER (PARTITION BY sortie_number ORDER BY message_sequence_number ROWS UNBOUNDED PRECEDING)
                              AS event_ts,
                              'cd_drone_tspi' AS source_table
                        FROM cd_drone_tspi_lag
                        ),
                        """
        ));

        cteCache.put(TiDBTable.PLANE_STATE, new TableCteDefinition(
                """
                        plane_state_lag AS (
                        SELECT sortie_number, aircraft_id, message_time, satellite_guidance_time, local_time,
                              message_sequence_number, true_angle_of_attack, mach_number, normal_load_factor,
                              indicated_airspeed, field_elevation, radio_altitude, remaining_fuel,
                              scenario, manual_respawn, parameter_setting_status, encryption_status,
                              LAG(satellite_guidance_time) OVER (PARTITION BY sortie_number ORDER BY message_sequence_number) AS prev_sgt
                        FROM `tidb_catalog`.`simulation`.`plane_state`
                        WHERE sortie_number = '%s'
                        ),
                        """,
                """
                        plane_state_processed AS (
                        SELECT sortie_number, aircraft_id, message_time, satellite_guidance_time, local_time,
                              message_sequence_number, true_angle_of_attack, mach_number, normal_load_factor,
                              indicated_airspeed, field_elevation, radio_altitude, remaining_fuel,
                              scenario, manual_respawn, parameter_setting_status, encryption_status,
                              CAST(satellite_guidance_time AS TIMESTAMP(3)) +
                              INTERVAL '1' DAY * SUM(CASE WHEN satellite_guidance_time < prev_sgt THEN 1 ELSE 0 END)
                                OVER (PARTITION BY sortie_number ORDER BY message_sequence_number ROWS UNBOUNDED PRECEDING)
                              AS event_ts,
                              'plane_state' AS source_table
                        FROM plane_state_lag
                        ),
                        """
        ));

        cteCache.put(TiDBTable.TSPI, new TableCteDefinition(
                """
                        tspi_lag AS (
                        SELECT sortie_number, aircraft_id, aircraft_type, message_time, satellite_guidance_time, local_time,
                              message_sequence_number, longitude, latitude, pressure_altitude, roll, pitch, heading,
                              satellite_altitude, training_status, chaff, afterburner, north_velocity,
                              vertical_velocity, east_velocity,
                              LAG(satellite_guidance_time) OVER (PARTITION BY sortie_number ORDER BY message_sequence_number) AS prev_sgt
                        FROM `tidb_catalog`.`simulation`.`tspi`
                        WHERE sortie_number = '%s'
                        ),
                        """,
                """
                        tspi_processed AS (
                        SELECT sortie_number, aircraft_id, aircraft_type, message_time, satellite_guidance_time, local_time,
                              message_sequence_number, longitude, latitude, pressure_altitude, roll, pitch, heading,
                              satellite_altitude, training_status, chaff, afterburner, north_velocity,
                              vertical_velocity, east_velocity,
                              CAST(satellite_guidance_time AS TIMESTAMP(3)) +
                              INTERVAL '1' DAY * SUM(CASE WHEN satellite_guidance_time < prev_sgt THEN 1 ELSE 0 END)
                                OVER (PARTITION BY sortie_number ORDER BY message_sequence_number ROWS UNBOUNDED PRECEDING)
                              AS event_ts,
                              'tspi' AS source_table
                        FROM tspi_lag
                        ),
                        """
        ));

        cteCache.put(TiDBTable.CD_DRONE_PLANE_STATE, new TableCteDefinition(
                """
                        cd_drone_plane_state_lag AS (
                        SELECT sortie_number, aircraft_id, message_time, satellite_guidance_time, local_time,
                              message_sequence_number, true_angle_of_attack, mach_number, normal_load_factor,
                              indicated_airspeed, field_elevation, radio_altitude, remaining_fuel,
                              manual_respawn, parameter_setting_status,
                              LAG(satellite_guidance_time) OVER (PARTITION BY sortie_number ORDER BY message_sequence_number) AS prev_sgt
                        FROM `tidb_catalog`.`simulation`.`cd_drone_plane_state`
                        WHERE sortie_number = '%s'
                        ),
                        """,
                """
                        cd_drone_plane_state_processed AS (
                        SELECT sortie_number, aircraft_id, message_time, satellite_guidance_time, local_time,
                              message_sequence_number, true_angle_of_attack, mach_number, normal_load_factor,
                              indicated_airspeed, field_elevation, radio_altitude, remaining_fuel,
                              manual_respawn, parameter_setting_status,
                              CAST(satellite_guidance_time AS TIMESTAMP(3)) +
                              INTERVAL '1' DAY * SUM(CASE WHEN satellite_guidance_time < prev_sgt THEN 1 ELSE 0 END)
                                OVER (PARTITION BY sortie_number ORDER BY message_sequence_number ROWS UNBOUNDED PRECEDING)
                              AS event_ts,
                              'cd_drone_plane_state' AS source_table
                        FROM cd_drone_plane_state_lag
                        ),
                        """
        ));

        cteCache.put(TiDBTable.HJ_PLANE_DATA, new TableCteDefinition(
                """
                        hj_plane_data_lag AS (
                        SELECT sortie_number, batch_number, device_number, flight_control_number,
                              local_time, message_time, message_sequence_number,
                              longitude, latitude, altitude, ground_speed,
                              vertical_speed, heading,
                              LAG(message_time) OVER (PARTITION BY sortie_number ORDER BY message_sequence_number) AS prev_msg_time
                        FROM `tidb_catalog`.`simulation`.`hj_plane_data`
                        WHERE sortie_number = '%s'
                        ),
                        """,
                """
                        hj_plane_data_processed AS (
                        SELECT sortie_number, batch_number, device_number, flight_control_number,
                              local_time, message_time, message_sequence_number,
                              longitude, latitude, altitude, ground_speed,
                              vertical_speed, heading,
                              CAST(message_time AS TIMESTAMP(3)) +
                              INTERVAL '1' DAY * SUM(CASE WHEN message_time < prev_msg_time THEN 1 ELSE 0 END)
                                OVER (PARTITION BY sortie_number ORDER BY message_sequence_number ROWS UNBOUNDED PRECEDING)
                              AS event_ts,
                              'hj_plane_data' AS source_table
                        FROM hj_plane_data_lag
                        ),
                        """
        ));
    }
}
