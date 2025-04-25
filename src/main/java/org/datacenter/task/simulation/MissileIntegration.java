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

        // 2. 使用创建的Catalog
        tEnv.executeSql("USE CATALOG simulation_catalog");

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

        tEnv.executeSql(missileMerge);

        String insertSql = """
                INSERT INTO missile_integration
                WITH
                    -- 1) AG_RTSN表先算 prev_sgt
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
                    FROM `simulation_catalog`.`simulation`.`ag_rtsn`
                    WHERE sortie_number = '%s'
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
                        -- 保留AG_RTSN字段
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
                
                    -- 3) AA_TRAJ表同理算出 prev_sgt/ day_shift/ event_ts
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
                    FROM `simulation_catalog`.`simulation`.`aa_traj`
                    WHERE sortie_number = '%s'
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
                        -- AA_TRAJ特有字段
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
                    AND t.event_ts BETWEEN r.event_ts - INTERVAL '1' SECOND
                                   AND r.event_ts + INTERVAL '1' SECOND
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
                        -- AG_RTSN特有字段
                        aircraft_ground_speed,
                        target_distance,
                        target_longitude,
                        target_latitude,
                        -- AA_TRAJ特有字段
                        missile_target_distance,
                        missile_speed,
                        interception_status
                    FROM matched
                    WHERE rn = 1;
                """.formatted(
                integrationConfig.getSortieNumber(),
                integrationConfig.getSortieNumber()
        );

        tEnv.executeSql(insertSql);
    }
}
