package org.datacenter.model.cte.missile;

import org.datacenter.model.base.TiDBTable;
import org.datacenter.model.cte.TableCteDefinition;

import java.util.HashMap;
import java.util.Map;

/**
 * @author : [wangminan]
 * @description : 表CTE模板缓存
 */
public class MissileTableCteCache {
    public static final Map<TiDBTable, TableCteDefinition> CTE_CACHE = new HashMap<>();

    static {
        // 初始化缓存
        CTE_CACHE.put(TiDBTable.AA_TRAJ, new TableCteDefinition(
                """
                        aa_traj_lag AS (
                        SELECT sortie_number, aircraft_id, message_time, satellite_guidance_time, local_time,
                              message_sequence_number, weapon_id, pylon_id, weapon_type, target_id,
                              longitude, latitude, altitude, missile_target_distance, missile_speed,
                              interception_status, non_interception_reason, seeker_azimuth, seeker_elevation,
                              target_tspi_status, command_machine_status, ground_angle_satisfaction_flag, zero_crossing_flag,
                              LAG(satellite_guidance_time) OVER (PARTITION BY sortie_number ORDER BY message_sequence_number) AS prev_sgt
                        FROM `tidb_catalog`.`simulation`.`aa_traj`
                        WHERE sortie_number = '%s'
                        ),
                        """,
                """
                        aa_traj_processed AS (
                        SELECT sortie_number, aircraft_id, message_time, satellite_guidance_time, local_time,
                             message_sequence_number, weapon_id, pylon_id, weapon_type, target_id,
                             longitude, latitude, altitude, missile_target_distance, missile_speed,
                             interception_status, non_interception_reason, seeker_azimuth, seeker_elevation,
                             target_tspi_status, command_machine_status, ground_angle_satisfaction_flag, zero_crossing_flag,
                             CAST(satellite_guidance_time AS TIMESTAMP(3)) +
                             INTERVAL '1' DAY * SUM(CASE WHEN satellite_guidance_time < prev_sgt THEN 1 ELSE 0 END)
                               OVER (PARTITION BY sortie_number ORDER BY message_sequence_number ROWS UNBOUNDED PRECEDING)
                             AS event_ts,
                             'aa_traj' AS source_table
                        FROM aa_traj_lag
                        ),
                        """
        ));
        CTE_CACHE.put(TiDBTable.AG_RTSN, new TableCteDefinition(
                """
                        ag_rtsn_lag AS (
                        SELECT sortie_number, aircraft_id, message_time, satellite_guidance_time, local_time,
                             message_sequence_number, weapon_id, weapon_pylon_id AS pylon_id, weapon_type,
                             number_of_missiles_released, aircraft_ground_speed, aircraft_longitude AS longitude,
                             aircraft_latitude AS latitude, aircraft_altitude AS altitude, aircraft_heading AS heading,
                             aircraft_pitch AS pitch, aircraft_roll AS roll, aircraft_angle_of_attack,
                             aircraft_north_speed, aircraft_vertical_speed, aircraft_east_speed,
                             north_wind_speed, vertical_wind_speed, east_wind_speed,
                             target_longitude, target_latitude, target_altitude, target_distance,
                             seeker_head_number, target_coordinate_validity, target_azimuth_elevation_validity,
                             target_elevation_angle, target_azimuth_angle, impact_angle_validity, entry_angle,
                             impact_angle, direction_validity,
                             LAG(satellite_guidance_time) OVER (PARTITION BY sortie_number ORDER BY message_sequence_number) AS prev_sgt
                        FROM `tidb_catalog`.`simulation`.`ag_rtsn`
                        WHERE sortie_number = '%s'
                        ),
                        """,
                """
                        ag_rtsn_processed AS (
                        SELECT sortie_number, aircraft_id, message_time, satellite_guidance_time, local_time,
                              message_sequence_number, weapon_id, pylon_id, weapon_type,
                              number_of_missiles_released, aircraft_ground_speed, longitude,
                              latitude, altitude, heading, pitch, roll, aircraft_angle_of_attack,
                              aircraft_north_speed, aircraft_vertical_speed, aircraft_east_speed,
                              north_wind_speed, vertical_wind_speed, east_wind_speed,
                              target_longitude, target_latitude, target_altitude, target_distance,
                              seeker_head_number, target_coordinate_validity, target_azimuth_elevation_validity,
                              target_elevation_angle, target_azimuth_angle, impact_angle_validity, entry_angle,
                              impact_angle, direction_validity,
                              CAST(satellite_guidance_time AS TIMESTAMP(3)) +
                              INTERVAL '1' DAY * SUM(CASE WHEN satellite_guidance_time < prev_sgt THEN 1 ELSE 0 END)
                                OVER (PARTITION BY sortie_number ORDER BY message_sequence_number ROWS UNBOUNDED PRECEDING)
                              AS event_ts,
                              'ag_rtsn' AS source_table
                        FROM ag_rtsn_lag
                        ),
                        """
        ));

        CTE_CACHE.put(TiDBTable.AG_TRAJ, new TableCteDefinition(
                """
                        ag_traj_lag AS (
                        SELECT sortie_number, aircraft_id, message_time, satellite_guidance_time, local_time,
                              message_sequence_number, weapon_id, weapon_type, longitude, latitude, altitude,
                              heading, pitch, north_speed, sky_speed, east_speed, seeker_id, interception_flag,
                              termination_flag, intercepting_member_id, intercepting_equipment_id,
                              intercepting_equipment_type, launcher_id, seeker_azimuth_center, seeker_pitch_center,
                              target_id, missile_target_distance,
                              LAG(satellite_guidance_time) OVER (PARTITION BY sortie_number ORDER BY message_sequence_number) AS prev_sgt
                        FROM `tidb_catalog`.`simulation`.`ag_traj`
                        WHERE sortie_number = '%s'
                        ),
                        """,
                """
                        ag_traj_processed AS (
                        SELECT sortie_number, aircraft_id, message_time, satellite_guidance_time, local_time,
                             message_sequence_number, weapon_id, weapon_type, longitude, latitude, altitude,
                             heading, pitch, north_speed, sky_speed, east_speed, seeker_id, interception_flag,
                             termination_flag, intercepting_member_id, intercepting_equipment_id,
                             intercepting_equipment_type, launcher_id, seeker_azimuth_center, seeker_pitch_center,
                             target_id, missile_target_distance,
                             CAST(satellite_guidance_time AS TIMESTAMP(3)) +
                             INTERVAL '1' DAY * SUM(CASE WHEN satellite_guidance_time < prev_sgt THEN 1 ELSE 0 END)
                               OVER (PARTITION BY sortie_number ORDER BY message_sequence_number ROWS UNBOUNDED PRECEDING)
                             AS event_ts,
                             'ag_traj' AS source_table
                        FROM ag_traj_lag
                        ),
                        """
        ));

        CTE_CACHE.put(TiDBTable.IR_MSL, new TableCteDefinition(
                """
                        ir_msl_lag AS (
                        SELECT sortie_number, aircraft_id, message_time, satellite_guidance_time, local_time,
                              message_sequence_number, seeker_azimuth, seeker_elevation, weapon_type, interception_flag,
                              LAG(satellite_guidance_time) OVER (PARTITION BY sortie_number ORDER BY message_sequence_number) AS prev_sgt
                        FROM `tidb_catalog`.`simulation`.`ir_msl`
                        WHERE sortie_number = '%s'
                        ),
                        """,
                """
                        ir_msl_processed AS (
                        SELECT sortie_number, aircraft_id, message_time, satellite_guidance_time, local_time,
                              message_sequence_number, seeker_azimuth, seeker_elevation, weapon_type, interception_flag,
                              CAST(satellite_guidance_time AS TIMESTAMP(3)) +
                              INTERVAL '1' DAY * SUM(CASE WHEN satellite_guidance_time < prev_sgt THEN 1 ELSE 0 END)
                                OVER (PARTITION BY sortie_number ORDER BY message_sequence_number ROWS UNBOUNDED PRECEDING)
                              AS event_ts,
                              'ir_msl' AS source_table
                        FROM ir_msl_lag
                        ),
                        """
        ));

        CTE_CACHE.put(TiDBTable.PL17_RTKN, new TableCteDefinition(
                """
                        pl17_rtkn_lag AS (
                        SELECT sortie_number, aircraft_id, message_time, satellite_guidance_time, local_time,
                              message_sequence_number, weapon_id, weapon_type, target_id, target_real_or_virtual,
                              hit_result, miss_reason, miss_distance, matching_failure_reason, jamming_effective,
                              jamming, afterburner, head_on, heading, pitch,
                              LAG(satellite_guidance_time) OVER (PARTITION BY sortie_number ORDER BY message_sequence_number) AS prev_sgt
                        FROM `tidb_catalog`.`simulation`.`pl17_rtkn`
                        WHERE sortie_number = '%s'
                        ),
                        """,
                """
                        pl17_rtkn_processed AS (
                        SELECT sortie_number, aircraft_id, message_time, satellite_guidance_time, local_time,
                              message_sequence_number, weapon_id, weapon_type, target_id, target_real_or_virtual,
                              hit_result, miss_reason, miss_distance, matching_failure_reason, jamming_effective,
                              jamming, afterburner, head_on, heading, pitch,
                              CAST(satellite_guidance_time AS TIMESTAMP(3)) +
                              INTERVAL '1' DAY * SUM(CASE WHEN satellite_guidance_time < prev_sgt THEN 1 ELSE 0 END)
                                OVER (PARTITION BY sortie_number ORDER BY message_sequence_number ROWS UNBOUNDED PRECEDING)
                              AS event_ts,
                              'pl17_rtkn' AS source_table
                        FROM pl17_rtkn_lag
                        ),
                        """
        ));

        CTE_CACHE.put(TiDBTable.PL17_RTSN, new TableCteDefinition(
                """
                        pl17_rtsn_lag AS (
                        SELECT sortie_number, aircraft_id, message_time, satellite_guidance_time, local_time,
                              message_sequence_number, target_id, intercepted_weapon_id, target_real_or_virtual,
                              weapon_id, pylon_id, weapon_type, trajectory_type, missile_attack_mode,
                              LAG(satellite_guidance_time) OVER (PARTITION BY sortie_number ORDER BY message_sequence_number) AS prev_sgt
                        FROM `tidb_catalog`.`simulation`.`pl17_rtsn`
                        WHERE sortie_number = '%s'
                        ),
                        """,
                """
                        pl17_rtsn_processed AS (
                        SELECT sortie_number, aircraft_id, message_time, satellite_guidance_time, local_time,
                              message_sequence_number, target_id, intercepted_weapon_id, target_real_or_virtual,
                              weapon_id, pylon_id, weapon_type, trajectory_type, missile_attack_mode,
                              CAST(satellite_guidance_time AS TIMESTAMP(3)) +
                              INTERVAL '1' DAY * SUM(CASE WHEN satellite_guidance_time < prev_sgt THEN 1 ELSE 0 END)
                                OVER (PARTITION BY sortie_number ORDER BY message_sequence_number ROWS UNBOUNDED PRECEDING)
                              AS event_ts,
                              'pl17_rtsn' AS source_table
                        FROM pl17_rtsn_lag
                        ),
                        """
        ));

        CTE_CACHE.put(TiDBTable.PL17_TRAJ, new TableCteDefinition(
                """
                        pl17_traj_lag AS (
                        SELECT sortie_number, aircraft_id, message_time, satellite_guidance_time, local_time,
                              message_sequence_number, weapon_id, pylon_id, weapon_type, target_id,
                              longitude, latitude, altitude, missile_target_distance, missile_speed,
                              interception_status, non_interception_reason, seeker_azimuth, seeker_elevation,
                              target_tspi_status, command_machine_status, ground_angle_satisfaction_flag,
                              zero_crossing_flag, distance_interception_flag, speed_interception_flag, angle_interception_flag,
                              LAG(satellite_guidance_time) OVER (PARTITION BY sortie_number ORDER BY message_sequence_number) AS prev_sgt
                        FROM `tidb_catalog`.`simulation`.`pl17_traj`
                        WHERE sortie_number = '%s'
                        ),
                        """,
                """
                        pl17_traj_processed AS (
                        SELECT sortie_number, aircraft_id, message_time, satellite_guidance_time, local_time,
                              message_sequence_number, weapon_id, pylon_id, weapon_type, target_id,
                              longitude, latitude, altitude, missile_target_distance, missile_speed,
                              interception_status, non_interception_reason, seeker_azimuth, seeker_elevation,
                              target_tspi_status, command_machine_status, ground_angle_satisfaction_flag,
                              zero_crossing_flag, distance_interception_flag, speed_interception_flag, angle_interception_flag,
                              CAST(satellite_guidance_time AS TIMESTAMP(3)) +
                              INTERVAL '1' DAY * SUM(CASE WHEN satellite_guidance_time < prev_sgt THEN 1 ELSE 0 END)
                                OVER (PARTITION BY sortie_number ORDER BY message_sequence_number ROWS UNBOUNDED PRECEDING)
                              AS event_ts,
                              'pl17_traj' AS source_table
                        FROM pl17_traj_lag
                        ),
                        """
        ));

        CTE_CACHE.put(TiDBTable.RTSN, new TableCteDefinition(
                """
                        rtsn_lag AS (
                        SELECT sortie_number, aircraft_id, message_time, satellite_guidance_time, local_time,
                             message_sequence_number, target_id, intercepted_weapon_id, target_real_or_virtual,
                             weapon_id, pylon_id, weapon_type, trajectory_type, missile_attack_mode,
                             LAG(satellite_guidance_time) OVER (PARTITION BY sortie_number ORDER BY message_sequence_number) AS prev_sgt
                        FROM `tidb_catalog`.`simulation`.`rtsn`
                        WHERE sortie_number = '%s'
                        ),
                        """,
                """
                        rtsn_processed AS (
                        SELECT sortie_number, aircraft_id, message_time, satellite_guidance_time, local_time,
                              message_sequence_number, target_id, intercepted_weapon_id, target_real_or_virtual,
                              weapon_id, pylon_id, weapon_type, trajectory_type, missile_attack_mode,
                              CAST(satellite_guidance_time AS TIMESTAMP(3)) +
                              INTERVAL '1' DAY * SUM(CASE WHEN satellite_guidance_time < prev_sgt THEN 1 ELSE 0 END)
                                OVER (PARTITION BY sortie_number ORDER BY message_sequence_number ROWS UNBOUNDED PRECEDING)
                              AS event_ts,
                              'rtsn' AS source_table
                        FROM rtsn_lag
                        ),
                        """
        ));

        CTE_CACHE.put(TiDBTable.RTKN, new TableCteDefinition(
                """
                        rtkn_lag AS (
                        SELECT sortie_number, aircraft_id, message_time, satellite_guidance_time, local_time,
                              message_sequence_number, weapon_id, weapon_type, target_id, intercepted_weapon_id,
                              hit_result, miss_reason, miss_distance, matching_failure_reason,
                              ground_defense_equipment_type, ground_defense_equipment_id,
                              ground_defense_equipment_type1, ground_defense_equipment_id1,
                              ground_defense_equipment_type2, ground_defense_equipment_id2,
                              ground_defense_equipment_type3, ground_defense_equipment_id3,
                              jamming_effective, jamming, afterburner, head_on, heading, pitch,
                              LAG(satellite_guidance_time) OVER (PARTITION BY sortie_number ORDER BY message_sequence_number) AS prev_sgt
                        FROM `tidb_catalog`.`simulation`.`rtkn`
                        WHERE sortie_number = '%s'
                        ),
                        """,
                """
                        rtkn_processed AS (
                        SELECT sortie_number, aircraft_id, message_time, satellite_guidance_time, local_time,
                              message_sequence_number, weapon_id, weapon_type, target_id, intercepted_weapon_id,
                              hit_result, miss_reason, miss_distance, matching_failure_reason,
                              ground_defense_equipment_type, ground_defense_equipment_id,
                              ground_defense_equipment_type1, ground_defense_equipment_id1,
                              ground_defense_equipment_type2, ground_defense_equipment_id2,
                              ground_defense_equipment_type3, ground_defense_equipment_id3,
                              jamming_effective, jamming, afterburner, head_on, heading, pitch,
                              CAST(satellite_guidance_time AS TIMESTAMP(3)) +
                              INTERVAL '1' DAY * SUM(CASE WHEN satellite_guidance_time < prev_sgt THEN 1 ELSE 0 END)
                                OVER (PARTITION BY sortie_number ORDER BY message_sequence_number ROWS UNBOUNDED PRECEDING)
                              AS event_ts,
                              'rtkn' AS source_table
                        FROM rtkn_lag
                        ),
                        """
        ));

        CTE_CACHE.put(TiDBTable.SA_TRAJ, new TableCteDefinition(
                """
                        sa_traj_lag AS (
                        SELECT sortie_number, aircraft_id, message_time, satellite_guidance_time, local_time,
                              message_sequence_number, weapon_id, pylon_id, weapon_type, target_id,
                              intercepted_weapon_id, longitude, latitude, altitude, missile_target_distance,
                              missile_speed, interception_status, non_interception_reason, seeker_azimuth,
                              seeker_elevation, target_tspi_status, command_machine_status,
                              LAG(satellite_guidance_time) OVER (PARTITION BY sortie_number ORDER BY message_sequence_number) AS prev_sgt
                        FROM `tidb_catalog`.`simulation`.`sa_traj`
                        WHERE sortie_number = '%s'
                        ),
                        """,
                """
                        sa_traj_processed AS (
                        SELECT sortie_number, aircraft_id, message_time, satellite_guidance_time, local_time,
                              message_sequence_number, weapon_id, pylon_id, weapon_type, target_id,
                              intercepted_weapon_id, longitude, latitude, altitude, missile_target_distance,
                              missile_speed, interception_status, non_interception_reason, seeker_azimuth,
                              seeker_elevation, target_tspi_status, command_machine_status,
                              CAST(satellite_guidance_time AS TIMESTAMP(3)) +
                              INTERVAL '1' DAY * SUM(CASE WHEN satellite_guidance_time < prev_sgt THEN 1 ELSE 0 END)
                                OVER (PARTITION BY sortie_number ORDER BY message_sequence_number ROWS UNBOUNDED PRECEDING)
                              AS event_ts,
                              'sa_traj' AS source_table
                        FROM sa_traj_lag
                        ),
                        """
        ));
    }
}
