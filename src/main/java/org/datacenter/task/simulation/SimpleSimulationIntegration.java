package org.datacenter.task.simulation;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.datacenter.config.HumanMachineConfig;
import org.datacenter.util.DataIntegrationUtil;

public class SimpleSimulationIntegration {

    /**
     * TODO
     * 1. 修改数据源和整合源至与数据模型匹配
     * 2. 修改数据源配置引入配置化参数 参考org.datacenter.receiver.equipment.EquipmentCodeCdcReceiver
     * 3. 修改insertSql至与数据源匹配
     */

    public static void main(String[] args) {
        HumanMachineConfig config = new HumanMachineConfig();
        config.loadConfig();

        // 在创建表环境前设置运行模式
        StreamExecutionEnvironment env = DataIntegrationUtil.prepareStreamEnv();
        env.setParallelism(1);
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        String agRtsnSrc = """
                    CREATE TABLE ag_rtsn_src (
                    sortie_number                     STRING,
                    aircraft_id                       STRING,
                    message_time                      STRING,
                    satellite_guidance_time           TIME(3),
                    local_time                        STRING,
                    message_sequence_number           BIGINT,
                    weapon_id                         STRING,
                    weapon_pylon_id                   STRING,
                    weapon_type                       STRING,
                    number_of_missiles_released       STRING,
                    aircraft_ground_speed             STRING,
                    aircraft_longitude                STRING,
                    aircraft_latitude                 STRING,
                    aircraft_altitude                 STRING,
                    aircraft_heading                  STRING,
                    aircraft_pitch                    STRING,
                    aircraft_roll                     STRING,
                    aircraft_angle_of_attack          STRING,
                    aircraft_north_speed              STRING,
                    aircraft_vertical_speed           STRING,
                    aircraft_east_speed               STRING,
                    north_wind_speed                  STRING,
                    vertical_wind_speed               STRING,
                    east_wind_speed                   STRING,
                    target_longitude                  STRING,
                    target_latitude                   STRING,
                    target_altitude                   STRING,
                    target_distance                   STRING,
                    seeker_head_number                STRING,
                    target_coordinate_validity        STRING,
                    target_azimuth_elevation_validity STRING,
                    target_elevation_angle            STRING,
                    target_azimuth_angle              STRING,
                    impact_angle_validity             STRING,
                    entry_angle                       STRING,
                    impact_angle                      STRING,
                    direction_validity                STRING
                ) WITH (
                    'connector' = 'jdbc',
                    'url'       = 'jdbc:mysql://10.68.20.38:4000/simulation',
                    'driver' = 'com.mysql.cj.jdbc.Driver',
                    'table-name'= 'ag_rtsn',
                    'username'  = '%s',
                    'password'  = '%s'
                );
                """.formatted(
                "root",
                "Lab418Server!"
        );

        String aaTrajSrc = """
                CREATE TABLE aa_traj_src (
                sortie_number               STRING,
                aircraft_id                 STRING,
                message_time                STRING,
                satellite_guidance_time     TIME(3),
                local_time                  STRING,
                message_sequence_number     BIGINT,
                weapon_id                   STRING,
                weapon_type                 STRING,
                longitude                   STRING,
                latitude                    STRING,
                altitude                    STRING,
                heading                     STRING,
                pitch                       STRING,
                north_speed                 STRING,
                sky_speed                   STRING,
                east_speed                  STRING,
                seeker_id                   STRING,
                interception_flag           STRING,
                termination_flag            STRING,
                intercepting_member_id      STRING,
                intercepting_equipment_id   STRING,
                intercepting_equipment_type STRING,
                launcher_id                 STRING,
                seeker_azimuth_center       STRING,
                seeker_pitch_center         STRING,
                target_id                   STRING,
                missile_target_distance     STRING
                ) WITH (
                    'connector' = 'jdbc',
                    'url'       = 'jdbc:mysql://10.68.20.38:4000/simulation',
                    'driver' = 'com.mysql.cj.jdbc.Driver',
                    'table-name'= 'aa_traj',
                    'username'  = '%s',
                    'password'  = '%s'
                );
                """.formatted(
                "root",
                "Lab418Server!"
        );

        String mergedFlight = """
                CREATE TABLE merged_flight (
                    sortie_number   STRING,
                    event_ts        TIMESTAMP(3),
                    -- ↓ 低频字段 (保留) -------------
                    weapon_type     STRING,
                    aircraft_ground_speed STRING,
                    target_distance STRING,
                    -- ↓ 高频字段示例 (重命名或丢弃) --
                    traj_longitude  STRING,
                    traj_latitude   STRING,
                    PRIMARY KEY (sortie_number, event_ts) NOT ENFORCED
                ) WITH (
                    'connector' = 'jdbc',
                    'url'       = 'jdbc:mysql://10.68.20.38:4000/simulation_integration',
                    'driver' = 'com.mysql.cj.jdbc.Driver',
                    'table-name'= 'merged_flight',
                    'username'  = '%s',
                    'password'  = '%s'
                );
                """.formatted(
                "root",
                "Lab418Server!"
        );

        // 1. 执行上面的 DDL 字符串
        tEnv.executeSql(agRtsnSrc);
        tEnv.executeSql(aaTrajSrc);
        tEnv.executeSql(mergedFlight);

        String insertSql = """
                INSERT INTO merged_flight
                    WITH
                    -- 1) 低频表先算 prev_sgt
                    ag_lag AS (
                    SELECT
                      sortie_number,
                      satellite_guidance_time,
                      message_sequence_number,
                      weapon_type,
                      aircraft_ground_speed,
                      target_distance,
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
                    -- 保留其它低频字段：
                    weapon_type,
                    aircraft_ground_speed,
                    target_distance,
                    latitude,
                    longitude
                  FROM ag_lag
                ),
                
                -- 3) 高频表同理算出 prev_sgt/ day_shift/ event_ts
                aa_lag AS (
                  SELECT
                    sortie_number,
                    satellite_guidance_time,
                    message_sequence_number,
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
                    -- 高频只保留要用到的列：
                    longitude,
                    latitude
                  FROM aa_lag
                ),
                
                -- 4) 最后做 interval join + 最近一条去重
                matched AS (
                  SELECT
                    r.sortie_number,
                    r.event_ts,
                    r.weapon_type,
                    r.aircraft_ground_speed,
                    r.target_distance,
                    t.longitude  AS traj_longitude,
                    t.latitude   AS traj_latitude,
                    ROW_NUMBER() OVER (
                     PARTITION BY r.sortie_number, r.message_sequence_number
                     ORDER BY ABS(
                       -- r 的毫秒时间戳
                       (1000 * EXTRACT(EPOCH   FROM r.event_ts))
                     + EXTRACT(MILLISECOND FROM r.event_ts)
                       --
                     - (1000 * EXTRACT(EPOCH   FROM t.event_ts))
                     -  EXTRACT(MILLISECOND FROM t.event_ts)
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
                  aircraft_ground_speed,
                  target_distance,
                  traj_longitude,
                  traj_latitude
                FROM matched
                WHERE rn = 1;
                """;

        tEnv.executeSql(insertSql);
    }
}
