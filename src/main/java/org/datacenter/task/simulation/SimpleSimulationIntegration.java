package org.datacenter.task.simulation;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.source.JdbcSource;
import org.apache.flink.connector.jdbc.split.CompositeJdbcParameterValuesProvider;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.datacenter.config.HumanMachineConfig;
import org.datacenter.config.integration.simalation.SimulationIntegrationConfig;
import org.datacenter.model.base.TiDBTable;
import org.datacenter.model.simulation.*;
import org.datacenter.util.DataIntegrationUtil;

import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Collections;

/**
 * @author : [wangminan]
 * @description : 简单的仿真数据整合
 */
@Slf4j
public class SimpleSimulationIntegration {

    private static Long basePlaneStateSeq;
    private static LocalTime basePlaneStateSatelliteGuidanceTime;

    private static Long basecommandSeq;

    private static LocalTime basecommandSeqliteGuidanceTime;
    private static Long baseCdDronePlaneStateSeq;
    private static LocalTime baseCdDronePlaneStateTime;
    private static Long baseCdDroneTspiSeq;
    private static LocalTime baseCdDroneTspiTime;
    private static Long baseTspiSeq;
    private static LocalTime baseTspiTime;

    public static void main(String[] args) {

        HumanMachineConfig humanMachineSysConfig = new HumanMachineConfig();
        humanMachineSysConfig.loadConfig();
        ParameterTool params = ParameterTool.fromArgs(args);

        log.info("Parameters: {}", params.toMap());
        SimulationIntegrationConfig simulationIntegrationConfig = SimulationIntegrationConfig.builder()
                .sortieNumber(params.getRequired("sortieNumber"))
                .build();
        StreamExecutionEnvironment env = DataIntegrationUtil.prepareStreamEnv();
        String sortieNumber = params.get("sortieNumber", "default_sortie_number");

        // 2. 创建TiDB数据源（使用最新API）
        DataStream<PlaneState> planeStream = env.fromSource(
                createJdbcSource(TiDBTable.PLANE_STATE, PlaneState.class,sortieNumber),
                WatermarkStrategy
                        .<PlaneState>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((planeState, timestamp) -> {
                            if (basePlaneStateSeq == null) {
                                basePlaneStateSeq = planeState.getMessageSequenceNumber();
                            }
                            if (basePlaneStateSatelliteGuidanceTime == null) {
                                basePlaneStateSatelliteGuidanceTime = planeState.getSatelliteGuidanceTime();
                            }

                            String dateStr = simulationIntegrationConfig.getSortieNumber().split("_")[0];
                            DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyyMMdd");
                            LocalDate baseDate = LocalDate.parse(dateStr, dateFormatter); // ✅ 转成 LocalDate

                            LocalTime satelliteGuidanceTime = planeState.getSatelliteGuidanceTime();

                            // 位次号小但时间大，更新日期为 +1
                            if (basePlaneStateSeq < planeState.getMessageSequenceNumber()
                                    && basePlaneStateSatelliteGuidanceTime.isAfter(satelliteGuidanceTime)) {
                                baseDate = baseDate.plusDays(1); // ✅ 用的是 LocalDate
                            }

                            long eventTimestamp = baseDate
                                    .atTime(satelliteGuidanceTime) // ✅ LocalDate + LocalTime
                                    .toInstant(ZoneOffset.ofHours(8))
                                    .toEpochMilli();

                            return eventTimestamp;
                        }),
                "TiDB PlaneState Source"
        );


        DataStream<Command> commandStream = env.fromSource(
                createJdbcSource(TiDBTable.COMMAND, Command.class, sortieNumber),
                WatermarkStrategy
                        .<Command>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((command, timestamp) -> {
                            if (basecommandSeq == null) {
                                basecommandSeq = command.getMessageSequenceNumber();
                            }
                            if (basecommandSeqliteGuidanceTime == null) {
                                basecommandSeqliteGuidanceTime = command.getSatelliteGuidanceTime();
                            }

                            String dateStr = simulationIntegrationConfig.getSortieNumber().split("_")[0];
                            DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyyMMdd");
                            LocalDate baseDate = LocalDate.parse(dateStr, dateFormatter); // ✅ 转成 LocalDate

                            LocalTime satelliteGuidanceTime = command.getSatelliteGuidanceTime();

                            // 位次号小但时间大，更新日期为 +1
                            if (basecommandSeq < command.getMessageSequenceNumber()
                                    && basecommandSeqliteGuidanceTime.isAfter(satelliteGuidanceTime)) {
                                baseDate = baseDate.plusDays(1); // ✅ 用的是 LocalDate
                            }

                            long eventTimestamp = baseDate
                                    .atTime(satelliteGuidanceTime) // ✅ LocalDate + LocalTime
                                    .toInstant(ZoneOffset.ofHours(8))
                                    .toEpochMilli();

                            return eventTimestamp;
                        }),
                "TiDB Command Source"
        );

        DataStream<CdDronePlaneState> cdDronePlaneStateStream = env.fromSource(
                createJdbcSource(TiDBTable.CD_DRONE_PLANE_STATE, CdDronePlaneState.class,sortieNumber),
                WatermarkStrategy
                        .<CdDronePlaneState>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((state, timestamp) -> {
                            if (baseCdDronePlaneStateSeq == null) {
                                baseCdDronePlaneStateSeq = state.getMessageSequenceNumber();
                            }
                            if (baseCdDronePlaneStateTime == null) {
                                baseCdDronePlaneStateTime = state.getSatelliteGuidanceTime();
                            }
                            String dateStr = simulationIntegrationConfig.getSortieNumber().split("_")[0];
                            LocalDate baseDate = LocalDate.parse(dateStr, DateTimeFormatter.ofPattern("yyyyMMdd"));
                            LocalTime time = state.getSatelliteGuidanceTime();
                            if (baseCdDronePlaneStateSeq < state.getMessageSequenceNumber()
                                    && baseCdDronePlaneStateTime.isAfter(time)) {
                                baseDate = baseDate.plusDays(1);
                            }
                            return baseDate.atTime(time).toInstant(ZoneOffset.ofHours(8)).toEpochMilli();
                        }),
                "TiDB cd_drone_plane_state Source"
        );


        DataStream<CdDroneTspi> cdDroneTspiStream = env.fromSource(
                createJdbcSource(TiDBTable.CD_DRONE_TSPI, CdDroneTspi.class,sortieNumber),
                WatermarkStrategy
                        .<CdDroneTspi>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((tspi, timestamp) -> {
                            if (baseCdDroneTspiSeq == null) {
                                baseCdDroneTspiSeq = tspi.getMessageSequenceNumber();
                            }
                            if (baseCdDroneTspiTime == null) {
                                baseCdDroneTspiTime = tspi.getSatelliteGuidanceTime();
                            }
                            String dateStr = simulationIntegrationConfig.getSortieNumber().split("_")[0];
                            LocalDate baseDate = LocalDate.parse(dateStr, DateTimeFormatter.ofPattern("yyyyMMdd"));
                            LocalTime time = tspi.getSatelliteGuidanceTime();
                            if (baseCdDroneTspiSeq < tspi.getMessageSequenceNumber()
                                    && baseCdDroneTspiTime.isAfter(time)) {
                                baseDate = baseDate.plusDays(1);
                            }
                            return baseDate.atTime(time).toInstant(ZoneOffset.ofHours(8)).toEpochMilli();
                        }),
                "TiDB cd_drone_tspi Source"
        );

        DataStream<Tspi> tspiStream = env.fromSource(
                createJdbcSource(TiDBTable.TSPI, Tspi.class,sortieNumber),
                WatermarkStrategy
                        .<Tspi>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((tspi, timestamp) -> {
                            if (baseTspiSeq == null) {
                                baseTspiSeq = tspi.getMessageSequenceNumber();
                            }
                            if (baseTspiTime == null) {
                                baseTspiTime = tspi.getSatelliteGuidanceTime();
                            }
                            String dateStr = simulationIntegrationConfig.getSortieNumber().split("_")[0];
                            LocalDate baseDate = LocalDate.parse(dateStr, DateTimeFormatter.ofPattern("yyyyMMdd"));
                            LocalTime time = tspi.getSatelliteGuidanceTime();
                            if (baseTspiSeq < tspi.getMessageSequenceNumber()
                                    && baseTspiTime.isAfter(time)) {
                                baseDate = baseDate.plusDays(1);
                            }
                            return baseDate.atTime(time).toInstant(ZoneOffset.ofHours(8)).toEpochMilli();
                        }),
                "TiDB tspi Source"
        );


// 过滤每个数据流，使用sortieNumber作为依据
        DataStream<CdDronePlaneState> filteredCdDronePlaneStateStream = cdDronePlaneStateStream
                .filter(state -> state.getSortieNumber().equals(sortieNumber));

        DataStream<CdDroneTspi> filteredCdDroneTspiStream = cdDroneTspiStream
                .filter(tspi -> tspi.getSortieNumber().equals(sortieNumber));

        DataStream<Command> filteredCommandStream = commandStream
                .filter(command -> command.getSortieNumber().equals(sortieNumber));

        DataStream<PlaneState> filteredPlaneStateStream = planeStream
                .filter(planeState -> planeState.getSortieNumber().equals(sortieNumber));

        DataStream<Tspi> filteredTspiStream = tspiStream
                .filter(tspi -> tspi.getSortieNumber().equals(sortieNumber));

// 然后，你可以选择不同的连接策略来合并这些流。例如，基于 `sortieNumber` 做连接：
        DataStream<MergedRecord> mergedStream = filteredCdDronePlaneStateStream
                .join(filteredCdDroneTspiStream)
                .where(state -> state.getSortieNumber())
                .equalTo(tspi -> tspi.getSortieNumber())
                .window(TumblingEventTimeWindows.of(Time.seconds(10))) // 你可以根据需求调整窗口大小
                .apply(new JoinFunction<CdDronePlaneState, CdDroneTspi, MergedRecord>() {
                    @Override
                    public MergedRecord join(CdDronePlaneState state, CdDroneTspi tspi) {
                        return new MergedRecord(state, tspi); // 合并为一个新的记录
                    }
                });

// 如果需要，可以继续用类似的方式连接其他表（Command, PlaneState, Tspi）
// 最终合并所有流
        mergedStream = mergedStream
                .keyBy(record -> record.getSortieNumber()) // 按照 `sortieNumber` 做键控
                .window(TumblingEventTimeWindows.of(Time.seconds(10))) // 按时间窗口处理
                .reduce(new ReduceFunction<MergedRecord>() {
                    @Override
                    public MergedRecord reduce(MergedRecord value1, MergedRecord value2) {
                        return value1.merge(value2); // 定义如何合并记录
                    }
                });

        // 执行作业
        try {
            env.execute("Stream Data Integration");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static <T> JdbcSource <T> createJdbcSource(TiDBTable table, Class <T> type , String sortieNumber) {
        return JdbcSource.<T>builder()
                .setSql("SELECT * FROM " + table.getName() + " WHERE sortie_number = ?")
                .setJdbcParameterValuesProvider(() -> new Serializable[][] {
                        { sortieNumber }   // ✅ 返回 Serializable[][]，每一行是一个参数组
                })

                .setResultExtractor((ResultSet rs) -> {
                    try {
                        if (type == PlaneState.class) {
                            // 使用无参构造函数创建对象
                            PlaneState planeState = new PlaneState();
                            // 设置字段值
                            planeState.setSortieNumber(rs.getString("sortie_number"));
                            planeState.setAircraftId(rs.getString("aircraft_id"));

                            // 获取并设置 LocalTime 类型的字段
                            planeState.setMessageTime(rs.getObject("message_time", LocalTime.class));
                            planeState.setSatelliteGuidanceTime(rs.getObject("satellite_guidance_time", LocalTime.class));
                            planeState.setLocalTime(rs.getObject("local_time", LocalTime.class));

                            // 设置 Long 类型字段
                            planeState.setMessageSequenceNumber(rs.getLong("message_sequence_number"));

                            // 设置其他 String 类型字段
                            planeState.setTrueAngleOfAttack(rs.getString("true_angle_of_attack"));
                            planeState.setMachNumber(rs.getString("mach_number"));
                            planeState.setNormalLoadFactor(rs.getString("normal_load_factor"));
                            planeState.setIndicatedAirspeed(rs.getString("indicated_airspeed"));
                            planeState.setFieldElevation(rs.getString("field_elevation"));
                            planeState.setRadioAltitude(rs.getString("radio_altitude"));
                            planeState.setRemainingFuel(rs.getString("remaining_fuel"));
                            planeState.setScenario(rs.getString("scenario"));
                            planeState.setManualRespawn(rs.getString("manual_respawn"));
                            planeState.setParameterSettingStatus(rs.getString("parameter_setting_status"));
                            planeState.setEncryptionStatus(rs.getString("encryption_status"));

                            // 其他字段可以在这里设置
                            return (PlaneState) planeState;
                        } else if (type == Tspi.class) {
                            // 使用无参构造函数创建对象
                            Tspi tspi = new Tspi();
                            // 设置字段值
                            tspi.setSortieNumber(rs.getString("sortie_number"));
                            tspi.setAircraftId(rs.getString("aircraft_id"));
                            tspi.setAircraftType(rs.getString("aircraft_type"));

                            // 获取并设置 LocalTime 类型的字段
                            tspi.setMessageTime(rs.getObject("message_time", LocalTime.class));
                            tspi.setSatelliteGuidanceTime(rs.getObject("satellite_guidance_time", LocalTime.class));
                            tspi.setLocalTime(rs.getObject("local_time", LocalTime.class));

                            // 设置 Long 类型字段
                            tspi.setMessageSequenceNumber(rs.getLong("message_sequence_number"));

                            // 设置 String 类型字段
                            tspi.setLongitude(rs.getString("longitude"));
                            tspi.setLatitude(rs.getString("latitude"));
                            tspi.setPressureAltitude(rs.getString("pressure_altitude"));
                            tspi.setRoll(rs.getString("roll"));
                            tspi.setPitch(rs.getString("pitch"));
                            tspi.setHeading(rs.getString("heading"));
                            tspi.setSatelliteAltitude(rs.getString("satellite_altitude"));
                            tspi.setTrainingStatus(rs.getString("training_status"));
                            tspi.setChaff(rs.getString("chaff"));
                            tspi.setAfterburner(rs.getString("afterburner"));
                            tspi.setNorthVelocity(rs.getString("north_velocity"));
                            tspi.setVerticalVelocity(rs.getString("vertical_velocity"));
                            tspi.setEastVelocity(rs.getString("east_velocity"));

                            return (Tspi) tspi;
                        } else if (type == CdDroneTspi.class) {
                            CdDroneTspi cdDroneTspi = new CdDroneTspi();

                            cdDroneTspi.setSortieNumber(rs.getString("sortie_number"));
                            cdDroneTspi.setAircraftId(rs.getString("aircraft_id"));
                            cdDroneTspi.setAircraftType(rs.getString("aircraft_type"));

                            cdDroneTspi.setMessageTime(rs.getObject("message_time", LocalTime.class));
                            cdDroneTspi.setSatelliteGuidanceTime(rs.getObject("satellite_guidance_time", LocalTime.class));
                            cdDroneTspi.setLocalTime(rs.getObject("local_time", LocalTime.class));

                            cdDroneTspi.setMessageSequenceNumber(rs.getLong("message_sequence_number"));

                            cdDroneTspi.setLongitude(rs.getString("longitude"));
                            cdDroneTspi.setLatitude(rs.getString("latitude"));
                            cdDroneTspi.setPressureAltitude(rs.getString("pressure_altitude"));
                            cdDroneTspi.setRoll(rs.getString("roll"));
                            cdDroneTspi.setPitch(rs.getString("pitch"));
                            cdDroneTspi.setHeading(rs.getString("heading"));
                            cdDroneTspi.setSatelliteAltitude(rs.getString("satellite_altitude"));
                            cdDroneTspi.setTrainingStatus(rs.getString("training_status"));
                            cdDroneTspi.setChaff(rs.getString("chaff"));
                            cdDroneTspi.setAfterburner(rs.getString("afterburner"));
                            cdDroneTspi.setNorthVelocity(rs.getString("north_velocity"));
                            cdDroneTspi.setVerticalVelocity(rs.getString("vertical_velocity"));
                            cdDroneTspi.setEastVelocity(rs.getString("east_velocity"));
                            cdDroneTspi.setDelayStatus(rs.getString("delay_status"));

                            return (CdDroneTspi) cdDroneTspi;

                        }else if (type == Command.class){
                            Command command = new Command();

                            command.setSortieNumber(rs.getString("sortie_number"));
                            command.setAircraftId(rs.getString("aircraft_id"));

                            command.setMessageTime(rs.getObject("message_time", LocalTime.class));
                            command.setSatelliteGuidanceTime(rs.getObject("satellite_guidance_time", LocalTime.class));
                            command.setLocalTime(rs.getObject("local_time", LocalTime.class));

                            command.setMessageSequenceNumber(rs.getLong("message_sequence_number"));

                            command.setCommandType(rs.getString("command_type"));
                            command.setCommandId(rs.getString("command_id"));
                            command.setCommandContent(rs.getString("command_content"));
                            command.setResponseSequenceNumber(rs.getLong("response_sequence_number"));

                            return (Command) command;
                        }
                        }else if(type == CdDronePlaneState.class){
                        CdDronePlaneState state = new CdDronePlaneState();

                        state.setSortieNumber(rs.getString("sortie_number"));
                        state.setAircraftId(rs.getString("aircraft_id"));

                        state.setMessageTime(rs.getObject("message_time", LocalTime.class));
                        state.setSatelliteGuidanceTime(rs.getObject("satellite_guidance_time", LocalTime.class));
                        state.setLocalTime(rs.getObject("local_time", LocalTime.class));

                        state.setMessageSequenceNumber(rs.getLong("message_sequence_number"));

                        state.setTrueAngleOfAttack(rs.getString("true_angle_of_attack"));
                        state.setMachNumber(rs.getString("mach_number"));
                        state.setNormalLoadFactor(rs.getString("normal_load_factor"));
                        state.setIndicatedAirspeed(rs.getString("indicated_airspeed"));
                        state.setFieldElevation(rs.getString("field_elevation"));
                        state.setRadioAltitude(rs.getString("radio_altitude"));
                        state.setRemainingFuel(rs.getString("remaining_fuel"));
                        state.setManualRespawn(rs.getString("manual_respawn"));
                        state.setParameterSettingStatus(rs.getString("parameter_setting_status"));

                        return ( CdDronePlaneState) state;

                    }

                        else {
                            throw new IllegalArgumentException("Unsupported type: " + type);
                        }
                    } catch (SQLException e) {
                    throw new IllegalArgumentException("Unsupported type: " + type);
                })
                .setTypeInformation(TypeInformation.of(type))
                //
                .setConnectionOptions(IntxxxUtil.getTiDBJdbcExecutionOptions())
                // getTiDBJdbcExecutionOptions
                .setExecutionOptions(JdbcExecutionOptions.builder()
                        .withBatchSize(1000)
                        .withBatchIntervalMs(200)
                        .build())
                .build();
    }
}
