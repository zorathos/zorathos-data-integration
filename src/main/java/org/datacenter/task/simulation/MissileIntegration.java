package org.datacenter.task.simulation;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.datacenter.config.HumanMachineConfig;
import org.datacenter.config.integration.simulation.SimulationIntegrationConfig;
import org.datacenter.config.keys.HumanMachineIntegrationConfigKey;
import org.datacenter.model.base.TiDBDatabase;
import org.datacenter.util.DataIntegrationUtil;

import java.util.Arrays;

import static org.datacenter.config.keys.HumanMachineSysConfigKey.TIDB_PASSWORD;
import static org.datacenter.config.keys.HumanMachineSysConfigKey.TIDB_URL_PREFIX;
import static org.datacenter.config.keys.HumanMachineSysConfigKey.TIDB_USERNAME;

/**
 * @author wangminan
 * @description 仿真导弹数据整合
 */
@Slf4j
public class MissileIntegration {

    public static void main(String[] args) {
        HumanMachineConfig config = new HumanMachineConfig();
        config.loadConfig();

        ParameterTool param = ParameterTool.fromArgs(args);
        log.info("Params: {}", param.toMap());

        SimulationIntegrationConfig integrationConfig = SimulationIntegrationConfig.builder()
                .sortieNumber(param.getRequired(HumanMachineIntegrationConfigKey.SORTIE_NUMBER.getKeyForParamsMap()))
                .build();

        // 在创建表环境前设置运行模式
        StreamExecutionEnvironment env = DataIntegrationUtil.prepareStreamEnv();
        env.setParallelism(1);
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 1. 创建JDBC Catalog
        tEnv.executeSql("""
                CREATE CATALOG tidb_catalog WITH (
                    'type' = 'jdbc',
                    'default-database' = '%s',
                    'username' = '%s',
                    'password' = '%s',
                    'base-url' = '%s')
                """.formatted(
                TiDBDatabase.SIMULATION.getName(),
                HumanMachineConfig.getProperty(TIDB_USERNAME),
                HumanMachineConfig.getProperty(TIDB_PASSWORD),
                HumanMachineConfig.getProperty(TIDB_URL_PREFIX)
        ));
        log.info("Create JDBC Catalog complete.");

        // 2. 再使用创建的Catalog
        log.info("Start using JDBC Catalog");
        tEnv.executeSql("USE CATALOG tidb_catalog");
        log.info("Successfully change current catalog to tidb_catalog");

        log.info("Current catalogs: {}", Arrays.asList(tEnv.listCatalogs()));
        log.info("Available databases: {}", Arrays.asList(tEnv.listDatabases()));
        log.info("Available tables: {}", Arrays.asList(tEnv.listTables()));

        String insertSql = """
        INSERT INTO `tidb_catalog`.`simulation_integration`.`missile_integration`
        WITH
            -- AA_TRAJ处理
            aa_traj_lag AS (
                SELECT
                    sortie_number, aircraft_id, message_time, satellite_guidance_time, local_time,
                    message_sequence_number, weapon_id, pylon_id, weapon_type, target_id,
                    longitude, latitude, altitude, missile_target_distance, missile_speed,
                    interception_status, non_interception_reason, seeker_azimuth, seeker_elevation,
                    target_tspi_status, command_machine_status, ground_angle_satisfaction_flag, zero_crossing_flag,
                    LAG(satellite_guidance_time) OVER (PARTITION BY sortie_number ORDER BY message_sequence_number) AS prev_sgt
                FROM `tidb_catalog`.`simulation`.`aa_traj`
                WHERE sortie_number = '%s'
            ),
            aa_traj_processed AS (
                SELECT
                    sortie_number, aircraft_id, message_time, satellite_guidance_time, local_time,
                    message_sequence_number, weapon_id, pylon_id, weapon_type, target_id,
                    longitude, latitude, altitude, missile_target_distance, missile_speed,
                    interception_status, non_interception_reason, seeker_azimuth, seeker_elevation,
                    target_tspi_status, command_machine_status, ground_angle_satisfaction_flag, zero_crossing_flag,
                    CAST(satellite_guidance_time AS TIMESTAMP(3))
                    + INTERVAL '1' DAY *
                    SUM(CASE WHEN satellite_guidance_time < prev_sgt THEN 1 ELSE 0 END)
                        OVER (PARTITION BY sortie_number ORDER BY message_sequence_number ROWS UNBOUNDED PRECEDING)
                    AS event_ts,
                    'aa_traj' AS source_table
                FROM aa_traj_lag
            ),
        
            -- AG_RTSN处理
            ag_rtsn_lag AS (
                SELECT
                    sortie_number, aircraft_id, message_time, satellite_guidance_time, local_time,
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
            ag_rtsn_processed AS (
                SELECT 
                    sortie_number, aircraft_id, message_time, satellite_guidance_time, local_time,
                    message_sequence_number, weapon_id, pylon_id, weapon_type,
                    number_of_missiles_released, aircraft_ground_speed, longitude, 
                    latitude, altitude, heading, pitch, roll, aircraft_angle_of_attack,
                    aircraft_north_speed, aircraft_vertical_speed, aircraft_east_speed,
                    north_wind_speed, vertical_wind_speed, east_wind_speed,
                    target_longitude, target_latitude, target_altitude, target_distance,
                    seeker_head_number, target_coordinate_validity, target_azimuth_elevation_validity,
                    target_elevation_angle, target_azimuth_angle, impact_angle_validity, entry_angle,
                    impact_angle, direction_validity,
                    CAST(satellite_guidance_time AS TIMESTAMP(3))
                    + INTERVAL '1' DAY *
                    SUM(CASE WHEN satellite_guidance_time < prev_sgt THEN 1 ELSE 0 END)
                        OVER (PARTITION BY sortie_number ORDER BY message_sequence_number ROWS UNBOUNDED PRECEDING)
                    AS event_ts,
                    'ag_rtsn' AS source_table
                FROM ag_rtsn_lag
            ),
        
            -- AG_TRAJ处理
            ag_traj_lag AS (
                SELECT 
                    sortie_number, aircraft_id, message_time, satellite_guidance_time, local_time,
                    message_sequence_number, weapon_id, weapon_type, longitude, latitude, altitude,
                    heading, pitch, north_speed, sky_speed, east_speed, seeker_id, interception_flag,
                    termination_flag, intercepting_member_id, intercepting_equipment_id, 
                    intercepting_equipment_type, launcher_id, seeker_azimuth_center, seeker_pitch_center,
                    target_id, missile_target_distance,
                    LAG(satellite_guidance_time) OVER (PARTITION BY sortie_number ORDER BY message_sequence_number) AS prev_sgt
                FROM `tidb_catalog`.`simulation`.`ag_traj`
                WHERE sortie_number = '%s'
            ),
            ag_traj_processed AS (
                SELECT 
                    sortie_number, aircraft_id, message_time, satellite_guidance_time, local_time,
                    message_sequence_number, weapon_id, weapon_type, longitude, latitude, altitude,
                    heading, pitch, north_speed, sky_speed, east_speed, seeker_id, interception_flag,
                    termination_flag, intercepting_member_id, intercepting_equipment_id, 
                    intercepting_equipment_type, launcher_id, seeker_azimuth_center, seeker_pitch_center,
                    target_id, missile_target_distance,
                    CAST(satellite_guidance_time AS TIMESTAMP(3))
                    + INTERVAL '1' DAY *
                    SUM(CASE WHEN satellite_guidance_time < prev_sgt THEN 1 ELSE 0 END)
                        OVER (PARTITION BY sortie_number ORDER BY message_sequence_number ROWS UNBOUNDED PRECEDING)
                    AS event_ts,
                    'ag_traj' AS source_table
                FROM ag_traj_lag
            ),
        
            -- 继续处理其他表...
            ir_msl_lag AS (
                SELECT 
                    sortie_number, aircraft_id, message_time, satellite_guidance_time, local_time,
                    message_sequence_number, seeker_azimuth, seeker_elevation, weapon_type, interception_flag,
                    LAG(satellite_guidance_time) OVER (PARTITION BY sortie_number ORDER BY message_sequence_number) AS prev_sgt
                FROM `tidb_catalog`.`simulation`.`ir_msl`
                WHERE sortie_number = '%s'
            ),
            ir_msl_processed AS (
                SELECT 
                    sortie_number, aircraft_id, message_time, satellite_guidance_time, local_time,
                    message_sequence_number, seeker_azimuth, seeker_elevation, weapon_type, interception_flag,
                    CAST(satellite_guidance_time AS TIMESTAMP(3))
                    + INTERVAL '1' DAY *
                    SUM(CASE WHEN satellite_guidance_time < prev_sgt THEN 1 ELSE 0 END)
                        OVER (PARTITION BY sortie_number ORDER BY message_sequence_number ROWS UNBOUNDED PRECEDING)
                    AS event_ts,
                    'ir_msl' AS source_table
                FROM ir_msl_lag
            ),
        
            -- PL17_RTKN处理
            pl17_rtkn_lag AS (
                SELECT 
                    sortie_number, aircraft_id, message_time, satellite_guidance_time, local_time,
                    message_sequence_number, weapon_id, weapon_type, target_id, target_real_or_virtual,
                    hit_result, miss_reason, miss_distance, matching_failure_reason, jamming_effective,
                    jamming, afterburner, head_on, heading, pitch,
                    LAG(satellite_guidance_time) OVER (PARTITION BY sortie_number ORDER BY message_sequence_number) AS prev_sgt
                FROM `tidb_catalog`.`simulation`.`pl17_rtkn`
                WHERE sortie_number = '%s'
            ),
            pl17_rtkn_processed AS (
                SELECT 
                    sortie_number, aircraft_id, message_time, satellite_guidance_time, local_time,
                    message_sequence_number, weapon_id, weapon_type, target_id, target_real_or_virtual,
                    hit_result, miss_reason, miss_distance, matching_failure_reason, jamming_effective,
                    jamming, afterburner, head_on, heading, pitch,
                    CAST(satellite_guidance_time AS TIMESTAMP(3))
                    + INTERVAL '1' DAY *
                    SUM(CASE WHEN satellite_guidance_time < prev_sgt THEN 1 ELSE 0 END)
                        OVER (PARTITION BY sortie_number ORDER BY message_sequence_number ROWS UNBOUNDED PRECEDING)
                    AS event_ts,
                    'pl17_rtkn' AS source_table
                FROM pl17_rtkn_lag
            ),
        
            -- 剩余表处理...
            pl17_rtsn_lag AS (
                SELECT 
                    sortie_number, aircraft_id, message_time, satellite_guidance_time, local_time,
                    message_sequence_number, target_id, intercepted_weapon_id, target_real_or_virtual,
                    weapon_id, pylon_id, weapon_type, trajectory_type, missile_attack_mode,
                    LAG(satellite_guidance_time) OVER (PARTITION BY sortie_number ORDER BY message_sequence_number) AS prev_sgt
                FROM `tidb_catalog`.`simulation`.`pl17_rtsn`
                WHERE sortie_number = '%s'
            ),
            pl17_rtsn_processed AS (
                SELECT 
                    sortie_number, aircraft_id, message_time, satellite_guidance_time, local_time,
                    message_sequence_number, target_id, intercepted_weapon_id, target_real_or_virtual,
                    weapon_id, pylon_id, weapon_type, trajectory_type, missile_attack_mode,
                    CAST(satellite_guidance_time AS TIMESTAMP(3))
                    + INTERVAL '1' DAY *
                    SUM(CASE WHEN satellite_guidance_time < prev_sgt THEN 1 ELSE 0 END)
                        OVER (PARTITION BY sortie_number ORDER BY message_sequence_number ROWS UNBOUNDED PRECEDING)
                    AS event_ts,
                    'pl17_rtsn' AS source_table
                FROM pl17_rtsn_lag
            ),
        
            -- PL17_TRAJ处理
            pl17_traj_lag AS (
                SELECT 
                    sortie_number, aircraft_id, message_time, satellite_guidance_time, local_time,
                    message_sequence_number, weapon_id, pylon_id, weapon_type, target_id,
                    longitude, latitude, altitude, missile_target_distance, missile_speed,
                    interception_status, non_interception_reason, seeker_azimuth, seeker_elevation,
                    target_tspi_status, command_machine_status, ground_angle_satisfaction_flag,
                    zero_crossing_flag, distance_interception_flag, speed_interception_flag, angle_interception_flag,
                    LAG(satellite_guidance_time) OVER (PARTITION BY sortie_number ORDER BY message_sequence_number) AS prev_sgt
                FROM `tidb_catalog`.`simulation`.`pl17_traj`
                WHERE sortie_number = '%s'
            ),
            pl17_traj_processed AS (
                SELECT 
                    sortie_number, aircraft_id, message_time, satellite_guidance_time, local_time,
                    message_sequence_number, weapon_id, pylon_id, weapon_type, target_id,
                    longitude, latitude, altitude, missile_target_distance, missile_speed,
                    interception_status, non_interception_reason, seeker_azimuth, seeker_elevation,
                    target_tspi_status, command_machine_status, ground_angle_satisfaction_flag,
                    zero_crossing_flag, distance_interception_flag, speed_interception_flag, angle_interception_flag,
                    CAST(satellite_guidance_time AS TIMESTAMP(3))
                    + INTERVAL '1' DAY *
                    SUM(CASE WHEN satellite_guidance_time < prev_sgt THEN 1 ELSE 0 END)
                        OVER (PARTITION BY sortie_number ORDER BY message_sequence_number ROWS UNBOUNDED PRECEDING)
                    AS event_ts,
                    'pl17_traj' AS source_table
                FROM pl17_traj_lag
            ),
        
            -- RTSN处理
            rtsn_lag AS (
                SELECT 
                    sortie_number, aircraft_id, message_time, satellite_guidance_time, local_time,
                    message_sequence_number, target_id, intercepted_weapon_id, target_real_or_virtual,
                    weapon_id, pylon_id, weapon_type, trajectory_type, missile_attack_mode,
                    LAG(satellite_guidance_time) OVER (PARTITION BY sortie_number ORDER BY message_sequence_number) AS prev_sgt
                FROM `tidb_catalog`.`simulation`.`rtsn`
                WHERE sortie_number = '%s'
            ),
            rtsn_processed AS (
                SELECT 
                    sortie_number, aircraft_id, message_time, satellite_guidance_time, local_time,
                    message_sequence_number, target_id, intercepted_weapon_id, target_real_or_virtual,
                    weapon_id, pylon_id, weapon_type, trajectory_type, missile_attack_mode,
                    CAST(satellite_guidance_time AS TIMESTAMP(3))
                    + INTERVAL '1' DAY *
                    SUM(CASE WHEN satellite_guidance_time < prev_sgt THEN 1 ELSE 0 END)
                        OVER (PARTITION BY sortie_number ORDER BY message_sequence_number ROWS UNBOUNDED PRECEDING)
                    AS event_ts,
                    'rtsn' AS source_table
                FROM rtsn_lag
            ),
        
            -- RTKN处理
            rtkn_lag AS (
                SELECT 
                    sortie_number, aircraft_id, message_time, satellite_guidance_time, local_time,
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
            rtkn_processed AS (
                SELECT 
                    sortie_number, aircraft_id, message_time, satellite_guidance_time, local_time,
                    message_sequence_number, weapon_id, weapon_type, target_id, intercepted_weapon_id,
                    hit_result, miss_reason, miss_distance, matching_failure_reason,
                    ground_defense_equipment_type, ground_defense_equipment_id,
                    ground_defense_equipment_type1, ground_defense_equipment_id1,
                    ground_defense_equipment_type2, ground_defense_equipment_id2,
                    ground_defense_equipment_type3, ground_defense_equipment_id3,
                    jamming_effective, jamming, afterburner, head_on, heading, pitch,
                    CAST(satellite_guidance_time AS TIMESTAMP(3))
                    + INTERVAL '1' DAY *
                    SUM(CASE WHEN satellite_guidance_time < prev_sgt THEN 1 ELSE 0 END)
                        OVER (PARTITION BY sortie_number ORDER BY message_sequence_number ROWS UNBOUNDED PRECEDING)
                    AS event_ts,
                    'rtkn' AS source_table
                FROM rtkn_lag
            ),
        
            -- SA_TRAJ处理
            sa_traj_lag AS (
                SELECT 
                    sortie_number, aircraft_id, message_time, satellite_guidance_time, local_time,
                    message_sequence_number, weapon_id, pylon_id, weapon_type, target_id,
                    intercepted_weapon_id, longitude, latitude, altitude, missile_target_distance,
                    missile_speed, interception_status, non_interception_reason, seeker_azimuth,
                    seeker_elevation, target_tspi_status, command_machine_status,
                    LAG(satellite_guidance_time) OVER (PARTITION BY sortie_number ORDER BY message_sequence_number) AS prev_sgt
                FROM `tidb_catalog`.`simulation`.`sa_traj`
                WHERE sortie_number = '%s'
            ),
            sa_traj_processed AS (
                SELECT 
                    sortie_number, aircraft_id, message_time, satellite_guidance_time, local_time,
                    message_sequence_number, weapon_id, pylon_id, weapon_type, target_id,
                    intercepted_weapon_id, longitude, latitude, altitude, missile_target_distance,
                    missile_speed, interception_status, non_interception_reason, seeker_azimuth,
                    seeker_elevation, target_tspi_status, command_machine_status,
                    CAST(satellite_guidance_time AS TIMESTAMP(3))
                    + INTERVAL '1' DAY *
                    SUM(CASE WHEN satellite_guidance_time < prev_sgt THEN 1 ELSE 0 END)
                        OVER (PARTITION BY sortie_number ORDER BY message_sequence_number ROWS UNBOUNDED PRECEDING)
                    AS event_ts,
                    'sa_traj' AS source_table
                FROM sa_traj_lag
            ),
        
            -- 使用JOIN逻辑合并表数据
            joined_data AS (
                SELECT
                    -- 基础字段（从aa_traj表）
                    aa.sortie_number,
                    aa.event_ts,
                    aa.aircraft_id,
                    aa.message_time,
                    aa.satellite_guidance_time,
                    aa.local_time,
                    aa.message_sequence_number,
                    aa.weapon_id,
                    aa.weapon_type,
                    aa.target_id,
                    aa.pylon_id,
                    
                    -- 位置相关字段
                    aa.longitude,
                    aa.latitude, 
                    aa.altitude,
                    COALESCE(ag_r.heading, p17r.heading, rtk.heading) AS heading,
                    COALESCE(ag_r.pitch, p17r.pitch, rtk.pitch) AS pitch,
                    ag_r.roll,
                    
                    -- 速度相关字段
                    aa.missile_speed,
                    ag_r.aircraft_ground_speed,
                    COALESCE(ag_t.north_speed, null) AS north_speed,
                    COALESCE(ag_t.sky_speed, null) AS sky_speed,
                    COALESCE(ag_t.east_speed, null) AS east_speed,
                    ag_r.north_wind_speed,
                    ag_r.vertical_wind_speed,
                    ag_r.east_wind_speed,
                    ag_r.aircraft_north_speed,
                    ag_r.aircraft_vertical_speed,
                    ag_r.aircraft_east_speed,
                    
                    -- 目标相关字段
                    aa.missile_target_distance,
                    ag_r.target_distance,
                    ag_r.target_longitude,
                    ag_r.target_latitude,
                    ag_r.target_altitude,
                    ag_r.target_elevation_angle,
                    ag_r.target_azimuth_angle,
                    aa.target_tspi_status,
                    COALESCE(p17r.target_real_or_virtual, null) AS target_real_or_virtual,
                    ag_r.target_coordinate_validity,
                    COALESCE(p17s.intercepted_weapon_id, rts.intercepted_weapon_id, rtk.intercepted_weapon_id, null) AS intercepted_weapon_id,
                    
                    -- 导引头相关字段
                    COALESCE(aa.seeker_azimuth, ir.seeker_azimuth, null) AS seeker_azimuth,
                    COALESCE(aa.seeker_elevation, ir.seeker_elevation, null) AS seeker_elevation,
                    ag_t.seeker_id,
                    ag_r.seeker_head_number,
                    ag_t.seeker_azimuth_center,
                    ag_t.seeker_pitch_center,
                    
                    -- 截获/命中相关字段
                    aa.interception_status,
                    COALESCE(ag_t.interception_flag, ir.interception_flag, null) AS interception_flag,
                    aa.non_interception_reason,
                    COALESCE(p17r.hit_result, rtk.hit_result, null) AS hit_result,
                    COALESCE(p17r.miss_reason, rtk.miss_reason, null) AS miss_reason,
                    COALESCE(p17r.miss_distance, rtk.miss_distance, null) AS miss_distance,
                    COALESCE(p17r.matching_failure_reason, rtk.matching_failure_reason, null) AS matching_failure_reason,
                    ag_t.termination_flag,
                    p17t.distance_interception_flag,
                    p17t.speed_interception_flag,
                    p17t.angle_interception_flag,
                    
                    -- 其他状态和标志
                    aa.command_machine_status,
                    aa.ground_angle_satisfaction_flag,
                    aa.zero_crossing_flag,
                    ag_r.aircraft_angle_of_attack,
                    ag_r.impact_angle_validity,
                    ag_r.entry_angle,
                    ag_r.impact_angle,
                    ag_r.direction_validity,
                    p17s.missile_attack_mode,
                    COALESCE(p17s.trajectory_type, rts.trajectory_type, null) AS trajectory_type,
                    COALESCE(p17r.jamming_effective, rtk.jamming_effective, null) AS jamming_effective,
                    COALESCE(p17r.jamming, rtk.jamming, null) AS jamming,
                    COALESCE(p17r.afterburner, rtk.afterburner, null) AS afterburner,
                    COALESCE(p17r.head_on, rtk.head_on, null) AS head_on,
                    
                    -- 设备相关字段
                    ag_t.intercepting_member_id,
                    ag_t.intercepting_equipment_id,
                    ag_t.intercepting_equipment_type,
                    ag_t.launcher_id,
                    rtk.ground_defense_equipment_type,
                    rtk.ground_defense_equipment_id,
                    
                    -- 来源标记
                    'missile_integration' AS source_table
                FROM aa_traj_processed aa
                LEFT JOIN ag_rtsn_processed ag_r
                    ON aa.sortie_number = ag_r.sortie_number
                    AND ag_r.event_ts BETWEEN aa.event_ts - INTERVAL '1' SECOND AND aa.event_ts + INTERVAL '1' SECOND
                LEFT JOIN ag_traj_processed ag_t
                    ON aa.sortie_number = ag_t.sortie_number  
                    AND ag_t.event_ts BETWEEN aa.event_ts - INTERVAL '1' SECOND AND aa.event_ts + INTERVAL '1' SECOND
                LEFT JOIN ir_msl_processed ir
                    ON aa.sortie_number = ir.sortie_number
                    AND ir.event_ts BETWEEN aa.event_ts - INTERVAL '1' SECOND AND aa.event_ts + INTERVAL '1' SECOND
                LEFT JOIN pl17_rtkn_processed p17r
                    ON aa.sortie_number = p17r.sortie_number
                    AND p17r.event_ts BETWEEN aa.event_ts - INTERVAL '1' SECOND AND aa.event_ts + INTERVAL '1' SECOND
                LEFT JOIN pl17_rtsn_processed p17s
                    ON aa.sortie_number = p17s.sortie_number
                    AND p17s.event_ts BETWEEN aa.event_ts - INTERVAL '1' SECOND AND aa.event_ts + INTERVAL '1' SECOND
                LEFT JOIN pl17_traj_processed p17t
                    ON aa.sortie_number = p17t.sortie_number
                    AND p17t.event_ts BETWEEN aa.event_ts - INTERVAL '1' SECOND AND aa.event_ts + INTERVAL '1' SECOND
                LEFT JOIN rtsn_processed rts
                    ON aa.sortie_number = rts.sortie_number
                    AND rts.event_ts BETWEEN aa.event_ts - INTERVAL '1' SECOND AND aa.event_ts + INTERVAL '1' SECOND
                LEFT JOIN rtkn_processed rtk
                    ON aa.sortie_number = rtk.sortie_number
                    AND rtk.event_ts BETWEEN aa.event_ts - INTERVAL '1' SECOND AND aa.event_ts + INTERVAL '1' SECOND
                LEFT JOIN sa_traj_processed sa
                    ON aa.sortie_number = sa.sortie_number
                    AND sa.event_ts BETWEEN aa.event_ts - INTERVAL '1' SECOND AND aa.event_ts + INTERVAL '1' SECOND
            )
            
            -- 选取数据
            SELECT
                sortie_number,
                event_ts,
                aircraft_id,
                message_time,
                satellite_guidance_time,
                local_time,
                message_sequence_number,
                weapon_id,
                weapon_type,
                target_id,
                pylon_id,
                longitude,
                latitude,
                altitude,
                heading,
                pitch,
                roll,
                missile_speed,
                aircraft_ground_speed,
                north_speed,
                sky_speed,
                east_speed,
                north_wind_speed,
                vertical_wind_speed,
                east_wind_speed,
                aircraft_north_speed,
                aircraft_vertical_speed,
                aircraft_east_speed,
                missile_target_distance,
                target_distance,
                target_longitude,
                target_latitude,
                target_altitude,
                target_elevation_angle,
                target_azimuth_angle,
                target_tspi_status,
                target_real_or_virtual,
                target_coordinate_validity,
                intercepted_weapon_id,
                seeker_azimuth,
                seeker_elevation,
                seeker_id,
                seeker_head_number,
                seeker_azimuth_center,
                seeker_pitch_center,
                interception_status,
                interception_flag,
                non_interception_reason,
                hit_result,
                miss_reason,
                miss_distance,
                matching_failure_reason,
                termination_flag,
                distance_interception_flag,
                speed_interception_flag,
                angle_interception_flag,
                command_machine_status,
                ground_angle_satisfaction_flag,
                zero_crossing_flag,
                aircraft_angle_of_attack,
                impact_angle_validity,
                entry_angle,
                impact_angle,
                direction_validity,
                missile_attack_mode,
                trajectory_type,
                jamming_effective,
                jamming,
                afterburner,
                head_on,
                intercepting_member_id,
                intercepting_equipment_id,
                intercepting_equipment_type,
                launcher_id,
                ground_defense_equipment_type,
                ground_defense_equipment_id,
                source_table
            FROM joined_data
            ORDER BY event_ts;
        """.formatted(
                integrationConfig.getSortieNumber(),
                integrationConfig.getSortieNumber(),
                integrationConfig.getSortieNumber(),
                integrationConfig.getSortieNumber(),
                integrationConfig.getSortieNumber(),
                integrationConfig.getSortieNumber(),
                integrationConfig.getSortieNumber(),
                integrationConfig.getSortieNumber(),
                integrationConfig.getSortieNumber(),
                integrationConfig.getSortieNumber()
        );

        log.info("Start inserting data into tidb_catalog.simulation_integration.missile_integration");
        tEnv.executeSql(insertSql);
        log.info("Data insertion completed");
    }
}
