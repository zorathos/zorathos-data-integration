package org.datacenter.task.simulation;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.datacenter.config.HumanMachineConfig;
import org.datacenter.model.base.TiDBTable;
import org.datacenter.util.DataIntegrationUtil;
import org.datacenter.util.JdbcSinkUtil;

import static org.datacenter.config.keys.HumanMachineSysConfigKey.TIDB_MYSQL_DRIVER_NAME;
import static org.datacenter.config.keys.HumanMachineSysConfigKey.TIDB_PASSWORD;
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

//        SimulationIntegrationConfig integrationConfig = SimulationIntegrationConfig.builder()
//                .sortieNumber(param.getRequired(HumanMachineIntegrationConfigKey.SORTIE_NUMBER.getKeyForParamsMap()))
//                .build();

        // 在创建表环境前设置运行模式
        StreamExecutionEnvironment env = DataIntegrationUtil.prepareStreamEnv();
        env.setParallelism(1);
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        String agRtsnSrc = """
                CREATE TABLE ag_rtsn_src (
                sortie_number                     STRING comment '架次号',
                aircraft_id                       STRING comment '飞机ID',
                message_time                      TIME(3) comment '消息时间（精确到毫秒）',
                satellite_guidance_time           TIME(3) comment '卫导时间（精确到毫秒）',
                local_time                        TIME(3) comment '本地时间（精确到毫秒）',
                message_sequence_number           BIGINT comment '消息序列号',
                weapon_id                         STRING comment '武器ID',
                weapon_pylon_id                   STRING comment '武器挂架ID',
                weapon_type                       STRING comment '武器类型',
                number_of_missiles_released       STRING comment '投放弹数',
                aircraft_ground_speed             STRING comment '载机地速',
                aircraft_longitude                STRING comment '载机经度',
                aircraft_latitude                 STRING comment '载机纬度',
                aircraft_altitude                 STRING comment '载机高度',
                aircraft_heading                  STRING comment '载机航向',
                aircraft_pitch                    STRING comment '载机俯仰',
                aircraft_roll                     STRING comment '载机横滚',
                aircraft_angle_of_attack          STRING comment '载机攻角',
                aircraft_north_speed              STRING comment '载机北速',
                aircraft_vertical_speed           STRING comment '载机天速',
                aircraft_east_speed               STRING comment '载机东速',
                north_wind_speed                  STRING comment '北向风速',
                vertical_wind_speed               STRING comment '天向风速',
                east_wind_speed                   STRING comment '东向风速',
                target_longitude                  STRING comment '目标经度',
                target_latitude                   STRING comment '目标纬度',
                target_altitude                   STRING comment '目标高度',
                target_distance                   STRING comment '目标距离',
                seeker_head_number                STRING comment '导引头号',
                target_coordinate_validity        STRING comment '目标经纬高有效标识',
                target_azimuth_elevation_validity STRING comment '目标方位俯仰有效标识',
                target_elevation_angle            STRING comment '目标俯仰角(惯性侧滑角)',
                target_azimuth_angle              STRING comment '目标方位角(真空速)',
                impact_angle_validity             STRING comment '落角有效性',
                entry_angle                       STRING comment '进入角',
                impact_angle                      STRING comment '落角',
                direction_validity                STRING comment '方向有效性'
                ) WITH (
                    'connector' = 'jdbc',
                    'url'       = '%s',
                    'driver' = '%s',
                    'table-name'= '%s',
                    'username'  = '%s',
                    'password'  = '%s'
                );
                """.formatted(
                JdbcSinkUtil.TIDB_URL_SIMULATION,
                HumanMachineConfig.getProperty(TIDB_MYSQL_DRIVER_NAME),
                TiDBTable.AG_RTSN.getName(),
                HumanMachineConfig.getProperty(TIDB_USERNAME),
                HumanMachineConfig.getProperty(TIDB_PASSWORD)
        );

        String aaTrajSrc = """
                CREATE TABLE aa_traj_src (
                sortie_number                  STRING comment '架次号',
                aircraft_id                    STRING comment '飞机ID',
                message_time                   TIME(3) comment '消息时间（精确到毫秒）',
                satellite_guidance_time        TIME(3) comment '卫导时间（精确到毫秒）',
                local_time                     TIME(3) comment '本地时间（精确到毫秒）',
                message_sequence_number        BIGINT comment '消息序列号',
                weapon_id                      STRING comment '武器ID',
                pylon_id                       STRING comment '挂架ID',
                weapon_type                    STRING comment '武器类型',
                target_id                      STRING comment '目标ID',
                longitude                      STRING comment '经度',
                latitude                       STRING comment '纬度',
                altitude                       STRING comment '高度',
                missile_target_distance        STRING comment '弹目距离',
                missile_speed                  STRING comment '弹速度',
                interception_status            STRING comment '截获状态',
                non_interception_reason        STRING comment '未截获原因',
                seeker_azimuth                 STRING comment '导引头视线方位角',
                seeker_elevation               STRING comment '导引头视线俯仰角',
                target_tspi_status             STRING comment '目标TSPI状态',
                command_machine_status         STRING comment '指令机状态',
                ground_angle_satisfaction_flag STRING comment '擦地角满足标志',
                zero_crossing_flag             STRING comment '过零标志'
                ) WITH (
                    'connector' = 'jdbc',
                    'url'       = '%s',
                    'driver' = '%s',
                    'table-name'= '%s',
                    'username'  = '%s',
                    'password'  = '%s'
                );
                """.formatted(
                JdbcSinkUtil.TIDB_URL_SIMULATION,
                HumanMachineConfig.getProperty(TIDB_MYSQL_DRIVER_NAME),
                TiDBTable.AA_TRAJ.getName(),
                HumanMachineConfig.getProperty(TIDB_USERNAME),
                HumanMachineConfig.getProperty(TIDB_PASSWORD)
        );

        String missileMerge = """
                CREATE TABLE missile_integration (
                    sortie_number              STRING comment '架次号',
                    event_ts                   TIMESTAMP(3) comment '事件时间戳',
                    -- 保留的共用字段
                    weapon_type                STRING comment '武器类型',
                    aircraft_id                STRING comment '飞机ID',
                    weapon_id                  STRING comment '武器ID',
                    -- 位置相关字段
                    longitude                  STRING comment '经度',
                    latitude                   STRING comment '纬度',
                    altitude                   STRING comment '高度',
                    -- AG_RTSN特有字段
                    aircraft_ground_speed      STRING comment '载机地速',
                    target_distance            STRING comment '目标距离',
                    target_longitude           STRING comment '目标经度',
                    target_latitude            STRING comment '目标纬度',
                    -- AA_TRAJ特有字段
                    missile_target_distance    STRING comment '弹目距离',
                    missile_speed              STRING comment '弹速度',
                    interception_status        STRING comment '截获状态',
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

        // 1. 执行上面的 DDL 字符串
        tEnv.executeSql(agRtsnSrc);
        tEnv.executeSql(aaTrajSrc);
        tEnv.executeSql(missileMerge);

        // 确认表已创建
        try {
            tEnv.executeSql("DESCRIBE missile_integration").print();
        } catch (Exception e) {
            log.error("无法描述missile_integration表: {}", e.getMessage());
            throw e;
        }

        String insertSql = """
                INSERT INTO missile_integration
                    WITH
                    -- 1) AG导弹表先算 prev_sgt
                    ag_lag AS (
                    SELECT
                      sortie_number,
                      aircraft_id,
                      weapon_id,
                      satellite_guidance_time,
                      message_sequence_number,
                      weapon_type,
                      aircraft_ground_speed,
                      target_distance,
                      target_longitude,
                      target_latitude,
                      aircraft_altitude AS altitude,
                      aircraft_latitude AS latitude,
                      aircraft_longitude AS longitude,
                      LAG(satellite_guidance_time)
                        OVER (PARTITION BY sortie_number ORDER BY message_sequence_number)
                      AS prev_sgt
                    FROM ag_rtsn_src
                ),
                
                -- 2) 再算 day_shift 和 event_ts
                ag_shift AS (
                  SELECT
                    sortie_number,
                    aircraft_id,
                    weapon_id,
                    satellite_guidance_time,
                    message_sequence_number,
                    SUM(CASE
                          WHEN satellite_guidance_time < prev_sgt THEN 1
                          ELSE 0
                        END)
                      OVER (
                        PARTITION BY sortie_number
                        ORDER BY message_sequence_number
                        ROWS UNBOUNDED PRECEDING
                      )
                    AS day_shift,
                    CAST(satellite_guidance_time AS TIMESTAMP(3))
                      + INTERVAL '1' DAY *
                        SUM(CASE WHEN satellite_guidance_time < prev_sgt THEN 1 ELSE 0 END)
                          OVER (PARTITION BY sortie_number
                                ORDER BY message_sequence_number
                                ROWS UNBOUNDED PRECEDING)
                    AS event_ts,
                    -- 保留AG导弹字段
                    weapon_type,
                    aircraft_ground_speed,
                    target_distance,
                    target_longitude,
                    target_latitude,
                    altitude,
                    latitude,
                    longitude
                  FROM ag_lag
                ),
                
                -- 3) AA导弹表同理算出 prev_sgt/ day_shift/ event_ts
                aa_lag AS (
                  SELECT
                    sortie_number,
                    aircraft_id,
                    weapon_id,
                    weapon_type,
                    satellite_guidance_time,
                    message_sequence_number,
                    missile_target_distance,
                    missile_speed,
                    interception_status,
                    altitude,
                    longitude,
                    latitude,
                    LAG(satellite_guidance_time)
                      OVER (PARTITION BY sortie_number ORDER BY message_sequence_number)
                    AS prev_sgt
                  FROM aa_traj_src
                ),
                aa_shift AS (
                  SELECT
                    sortie_number,
                    aircraft_id,
                    weapon_id,
                    weapon_type,
                    satellite_guidance_time,
                    message_sequence_number,
                    SUM(CASE
                          WHEN satellite_guidance_time < prev_sgt THEN 1
                          ELSE 0
                        END)
                      OVER (
                        PARTITION BY sortie_number
                        ORDER BY message_sequence_number
                        ROWS UNBOUNDED PRECEDING
                      )
                    AS day_shift,
                    CAST(satellite_guidance_time AS TIMESTAMP(3))
                      + INTERVAL '1' DAY *
                        SUM(CASE WHEN satellite_guidance_time < prev_sgt THEN 1 ELSE 0 END)
                          OVER (PARTITION BY sortie_number
                                ORDER BY message_sequence_number
                                ROWS UNBOUNDED PRECEDING)
                    AS event_ts,
                    -- AA导弹特有字段
                    missile_target_distance,
                    missile_speed,
                    interception_status,
                    altitude,
                    longitude,
                    latitude
                  FROM aa_lag
                ),
                
                -- 4) 最后做 interval join + 最近一条去重
                matched AS (
                  SELECT
                    r.sortie_number,
                    r.event_ts,
                    COALESCE(r.weapon_type, t.weapon_type) AS weapon_type,
                    COALESCE(r.aircraft_id, t.aircraft_id) AS aircraft_id,
                    COALESCE(r.weapon_id, t.weapon_id) AS weapon_id,
                    -- 位置相关字段
                    COALESCE(t.longitude, r.longitude) AS longitude,
                    COALESCE(t.latitude, r.latitude) AS latitude,
                    COALESCE(t.altitude, r.altitude) AS altitude,
                    -- AG_RTSN特有字段
                    r.aircraft_ground_speed,
                    r.target_distance,
                    r.target_longitude,
                    r.target_latitude,
                    -- AA_TRAJ特有字段
                    t.missile_target_distance,
                    t.missile_speed,
                    t.interception_status,
                    ROW_NUMBER() OVER (
                     PARTITION BY r.sortie_number, r.message_sequence_number
                     ORDER BY ABS(
                       -- r 的毫秒时间戳
                       (1000 * EXTRACT(EPOCH FROM r.event_ts))
                       + EXTRACT(MILLISECOND FROM r.event_ts)
                       --
                       - (1000 * EXTRACT(EPOCH FROM t.event_ts))
                       - EXTRACT(MILLISECOND FROM t.event_ts)
                     )
                   ) AS rn
                  FROM ag_shift AS r
                  JOIN aa_shift AS t
                    ON r.sortie_number = t.sortie_number
                   AND t.event_ts BETWEEN r.event_ts - INTERVAL '0.05' SECOND
                                      AND r.event_ts + INTERVAL '0.05' SECOND
                )
                
                -- 5) 插入最终结果
                SELECT
                  sortie_number,
                  event_ts,
                  weapon_type,
                  aircraft_id,
                  weapon_id,
                  -- 位置相关字段
                  longitude,
                  latitude,
                  altitude,
                  -- AG导弹特有字段
                  aircraft_ground_speed,
                  target_distance,
                  target_longitude,
                  target_latitude,
                  -- AA导弹特有字段
                  missile_target_distance,
                  missile_speed,
                  interception_status
                FROM matched
                WHERE rn = 1;
                """;

        tEnv.executeSql(insertSql);
    }
}
