package org.datacenter.task.simulation;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.datacenter.config.HumanMachineConfig;
import org.datacenter.config.integration.simalation.SimulationIntegrationConfig;
import org.datacenter.config.keys.HumanMachineIntegrationConfigKey;
import org.datacenter.model.base.TiDBDatabase;
import org.datacenter.model.base.TiDBTable;
import org.datacenter.util.DataIntegrationUtil;
import org.datacenter.util.JdbcSinkUtil;

import static org.datacenter.config.keys.HumanMachineSysConfigKey.TIDB_MYSQL_DRIVER_NAME;
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

        log.info("开始创建目标表");
        String missileMerge = """
                CREATE TABLE missile_integration (
                    sortie_number              STRING comment '架次号',
                    event_ts                   TIMESTAMP(3) comment '事件时间戳',
                    
                    -- 共用基础字段
                    aircraft_id                STRING comment '飞机ID',
                    message_time               TIME(3) comment '消息时间',
                    satellite_guidance_time    TIME(3) comment '卫导时间',
                    local_time                 TIME(3) comment '本地时间',
                    message_sequence_number    BIGINT comment '消息序列号',
                    weapon_id                  STRING comment '武器ID',
                    weapon_type                STRING comment '武器类型',
                    target_id                  STRING comment '目标ID',
                    pylon_id                   STRING comment '挂架ID',
                    
                    -- 位置相关字段
                    longitude                  STRING comment '经度',
                    latitude                   STRING comment '纬度',
                    altitude                   STRING comment '高度',
                    heading                    STRING comment '航向',
                    pitch                      STRING comment '俯仰',
                    roll                       STRING comment '横滚',
                    
                    -- 速度相关
                    missile_speed              STRING comment '弹速度',
                    aircraft_ground_speed      STRING comment '载机地速',
                    north_speed                STRING comment '北速',
                    sky_speed                  STRING comment '天速',
                    east_speed                 STRING comment '东速',
                    north_wind_speed           STRING comment '北向风速',
                    vertical_wind_speed        STRING comment '天向风速',
                    east_wind_speed            STRING comment '东向风速',
                    aircraft_north_speed       STRING comment '载机北速',
                    aircraft_vertical_speed    STRING comment '载机天速',
                    aircraft_east_speed        STRING comment '载机东速',
                    
                    -- 目标相关
                    missile_target_distance    STRING comment '弹目距离',
                    target_distance            STRING comment '目标距离',
                    target_longitude           STRING comment '目标经度',
                    target_latitude            STRING comment '目标纬度',
                    target_altitude            STRING comment '目标高度',
                    target_elevation_angle     STRING comment '目标俯仰角',
                    target_azimuth_angle       STRING comment '目标方位角',
                    target_tspi_status         STRING comment '目标TSPI状态',
                    target_real_or_virtual     STRING comment '目标实虚属性',
                    target_coordinate_validity STRING comment '目标经纬高有效标识',
                    intercepted_weapon_id      STRING comment '被拦截武器ID',
                    
                    -- 导引头相关
                    seeker_azimuth             STRING comment '导引头视线方位角',
                    seeker_elevation           STRING comment '导引头视线俯仰角',
                    seeker_id                  STRING comment '导引头号',
                    seeker_head_number         STRING comment '导引头号',
                    seeker_azimuth_center      STRING comment '导引头方位中心',
                    seeker_pitch_center        STRING comment '导引头俯仰中心',
                    
                    -- 截获/命中相关
                    interception_status        STRING comment '截获状态',
                    interception_flag          STRING comment '截获标志',
                    non_interception_reason    STRING comment '未截获原因',
                    hit_result                 STRING comment '命中结果',
                    miss_reason                STRING comment '未命中原因',
                    miss_distance              STRING comment '脱靶量',
                    matching_failure_reason    STRING comment '匹配失败原因',
                    termination_flag           STRING comment '终止标志',
                    distance_interception_flag STRING comment '距离截获标志',
                    speed_interception_flag    STRING comment '速度截获标志',
                    angle_interception_flag    STRING comment '角度截获标志',
                    
                    -- 其他状态和标志
                    command_machine_status     STRING comment '指令机状态',
                    ground_angle_satisfaction_flag STRING comment '擦地角满足标志',
                    zero_crossing_flag         STRING comment '过零标志',
                    aircraft_angle_of_attack   STRING comment '载机攻角',
                    impact_angle_validity      STRING comment '落角有效性',
                    entry_angle                STRING comment '进入角',
                    impact_angle               STRING comment '落角',
                    direction_validity         STRING comment '方向有效性',
                    missile_attack_mode        STRING comment '导弹攻击模式',
                    trajectory_type            STRING comment '弹道类型',
                    jamming_effective          STRING comment '干扰是否有效',
                    jamming                    STRING comment '干扰',
                    afterburner                STRING comment '加力',
                    head_on                    STRING comment '迎头',
                    
                    -- 设备相关
                    intercepting_member_id     STRING comment '截获成员ID',
                    intercepting_equipment_id  STRING comment '截获装备ID',
                    intercepting_equipment_type STRING comment '截获装备类型',
                    launcher_id                STRING comment '发射方ID',
                    ground_defense_equipment_type STRING comment '地导装备类型',
                    ground_defense_equipment_id STRING comment '地导装备ID',
                    
                    -- 源表标识
                    source_table               STRING comment '数据来源表',
                    
                    PRIMARY KEY (sortie_number, event_ts) NOT ENFORCED
                ) WITH (
                    'connector' = 'jdbc',
                    'url'       = '%s',
                    'driver' = '%s',
                    'table-name'= '%s',
                    'username'  = '%s',
                    'password'  = '%s'
                );
                """.formatted(
                JdbcSinkUtil.TIDB_URL_SIMULATION_INTEGRATION,
                HumanMachineConfig.getProperty(TIDB_MYSQL_DRIVER_NAME),
                TiDBTable.MISSILE_INTEGRATION.getName(),
                HumanMachineConfig.getProperty(TIDB_USERNAME),
                HumanMachineConfig.getProperty(TIDB_PASSWORD)
        );

        // 创建整合表
        tEnv.executeSql(missileMerge);
        log.info("目标表创建完成");

        // 1. 创建JDBC Catalog
        tEnv.executeSql("""
                CREATE CATALOG simulation_catalog WITH (
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
        log.info("JDBC Catalog创建完成");

        // 2. 再使用创建的Catalog
        log.info("开始使用JDBC Catalog");
        tEnv.executeSql("USE CATALOG simulation_catalog");
        log.info("成功切换到simulation_catalog");

        log.info("开始执行插入SQL");
        String insertSql = """
        INSERT INTO missile_integration
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
                FROM `simulation_catalog`.`simulation`.`aa_traj`
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
                FROM `simulation_catalog`.`simulation`.`ag_rtsn`
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
                FROM `simulation_catalog`.`simulation`.`ag_traj`
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
                FROM `simulation_catalog`.`simulation`.`ir_msl`
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
                FROM `simulation_catalog`.`simulation`.`pl17_rtkn`
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
                FROM `simulation_catalog`.`simulation`.`pl17_rtsn`
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
                FROM `simulation_catalog`.`simulation`.`pl17_traj`
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
                FROM `simulation_catalog`.`simulation`.`rtsn`
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
                FROM `simulation_catalog`.`simulation`.`rtkn`
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
                FROM `simulation_catalog`.`simulation`.`sa_traj`
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
        
            -- 合并所有表数据
            all_missile_data AS (
                SELECT * FROM aa_traj_processed
                UNION ALL SELECT * FROM ag_rtsn_processed
                UNION ALL SELECT * FROM ag_traj_processed
                UNION ALL SELECT * FROM ir_msl_processed
                UNION ALL SELECT * FROM pl17_rtkn_processed
                UNION ALL SELECT * FROM pl17_rtsn_processed
                UNION ALL SELECT * FROM pl17_traj_processed
                UNION ALL SELECT * FROM rtsn_processed
                UNION ALL SELECT * FROM rtkn_processed
                UNION ALL SELECT * FROM sa_traj_processed
            ),
        
            -- 按时间窗口分组，将1秒内的数据合并
            grouped_data AS (
                SELECT 
                    *,
                    ROW_NUMBER() OVER (
                        PARTITION BY sortie_number, 
                        -- 1秒窗口
                        FLOOR(EXTRACT(EPOCH FROM event_ts)) 
                        ORDER BY message_sequence_number
                    ) AS rn
                FROM all_missile_data
            )
        
        -- 选取每个时间窗口的第一条记录
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
        FROM grouped_data
        WHERE rn = 1
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

        log.info("开始执行插入操作");
        tEnv.executeSql(insertSql);
        log.info("插入操作完成");
    }
}
