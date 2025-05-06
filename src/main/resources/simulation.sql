# ---------------------------------------- 模拟飞行数据库 ----------------------------------------
CREATE TABLE IF NOT EXISTS `simulation`.`aa_traj`
(
    `sortie_number`                  VARCHAR(50) COMMENT '架次号',
    `aircraft_id`                    VARCHAR(50) COMMENT '飞机ID',
    `message_time`                   TIME(3) COMMENT '消息时间（精确到毫秒）',
    `satellite_guidance_time`        TIME(3) COMMENT '卫导时间（精确到毫秒）',
    `local_time`                     TIME(3) COMMENT '本地时间（精确到毫秒）',
    `message_sequence_number`        BIGINT COMMENT '消息序列号',
    `weapon_id`                      VARCHAR(50) COMMENT '武器ID',
    `pylon_id`                       VARCHAR(50) COMMENT '挂架ID',
    `weapon_type`                    VARCHAR(50) COMMENT '武器类型',
    `target_id`                      VARCHAR(50) COMMENT '目标ID',
    `longitude`                      VARCHAR(50) COMMENT '经度',
    `latitude`                       VARCHAR(50) COMMENT '纬度',
    `altitude`                       VARCHAR(50) COMMENT '高度',
    `missile_target_distance`        VARCHAR(50) COMMENT '弹目距离',
    `missile_speed`                  VARCHAR(50) COMMENT '弹速度',
    `interception_status`            VARCHAR(50) COMMENT '截获状态',
    `non_interception_reason`        VARCHAR(50) COMMENT '未截获原因',
    `seeker_azimuth`                 VARCHAR(50) COMMENT '导引头视线方位角',
    `seeker_elevation`               VARCHAR(50) COMMENT '导引头视线俯仰角',
    `target_tspi_status`             VARCHAR(50) COMMENT '目标TSPI状态',
    `command_machine_status`         VARCHAR(50) COMMENT '指令机状态',
    `ground_angle_satisfaction_flag` VARCHAR(50) COMMENT '擦地角满足标志',
    `zero_crossing_flag`             VARCHAR(50) COMMENT '过零标志',
    INDEX idx_sortie_number (`sortie_number`)
) COMMENT = 'Aatraj' PARTITION BY KEY (`sortie_number`) PARTITIONS 1;

CREATE TABLE IF NOT EXISTS `simulation`.`ag_rtsn`
(
    `sortie_number`                     VARCHAR(50) COMMENT '架次号',
    `aircraft_id`                       VARCHAR(50) COMMENT '飞机ID',
    `message_time`                      TIME(3) COMMENT '消息时间（精确到毫秒）',
    `satellite_guidance_time`           TIME(3) COMMENT '卫导时间（精确到毫秒）',
    `local_time`                        TIME(3) COMMENT '本地时间（精确到毫秒）',
    `message_sequence_number`           BIGINT COMMENT '消息序列号',
    `weapon_id`                         VARCHAR(50) COMMENT '武器ID',
    `weapon_pylon_id`                   VARCHAR(50) COMMENT '武器挂架ID',
    `weapon_type`                       VARCHAR(50) COMMENT '武器类型',
    `number_of_missiles_released`       VARCHAR(50) COMMENT '投放弹数',
    `aircraft_ground_speed`             VARCHAR(50) COMMENT '载机地速',
    `aircraft_longitude`                VARCHAR(50) COMMENT '载机经度',
    `aircraft_latitude`                 VARCHAR(50) COMMENT '载机纬度',
    `aircraft_altitude`                 VARCHAR(50) COMMENT '载机高度',
    `aircraft_heading`                  VARCHAR(50) COMMENT '载机航向',
    `aircraft_pitch`                    VARCHAR(50) COMMENT '载机俯仰',
    `aircraft_roll`                     VARCHAR(50) COMMENT '载机横滚',
    `aircraft_angle_of_attack`          VARCHAR(50) COMMENT '载机攻角',
    `aircraft_north_speed`              VARCHAR(50) COMMENT '载机北速',
    `aircraft_vertical_speed`           VARCHAR(50) COMMENT '载机天速',
    `aircraft_east_speed`               VARCHAR(50) COMMENT '载机东速',
    `north_wind_speed`                  VARCHAR(50) COMMENT '北向风速',
    `vertical_wind_speed`               VARCHAR(50) COMMENT '天向风速',
    `east_wind_speed`                   VARCHAR(50) COMMENT '东向风速',
    `target_longitude`                  VARCHAR(50) COMMENT '目标经度',
    `target_latitude`                   VARCHAR(50) COMMENT '目标纬度',
    `target_altitude`                   VARCHAR(50) COMMENT '目标高度',
    `target_distance`                   VARCHAR(50) COMMENT '目标距离',
    `seeker_head_number`                VARCHAR(50) COMMENT '导引头号',
    `target_coordinate_validity`        VARCHAR(50) COMMENT '目标经纬高有效标识',
    `target_azimuth_elevation_validity` VARCHAR(50) COMMENT '目标方位俯仰有效标识',
    `target_elevation_angle`            VARCHAR(50) COMMENT '目标俯仰角(惯性侧滑角)',
    `target_azimuth_angle`              VARCHAR(50) COMMENT '目标方位角(真空速)',
    `impact_angle_validity`             VARCHAR(50) COMMENT '落角有效性',
    `entry_angle`                       VARCHAR(50) COMMENT '进入角',
    `impact_angle`                      VARCHAR(50) COMMENT '落角',
    `direction_validity`                VARCHAR(50) COMMENT '方向有效性',
    INDEX idx_sortie_number (`sortie_number`)
) COMMENT = 'AgRtsn' PARTITION BY KEY (`sortie_number`) PARTITIONS 1;

CREATE TABLE IF NOT EXISTS `simulation`.`ag_traj`
(
    `sortie_number`               VARCHAR(50) COMMENT '架次号',
    `aircraft_id`                 VARCHAR(50) COMMENT '飞机ID',
    `message_time`                TIME(3) COMMENT '消息时间（精确到毫秒）',
    `satellite_guidance_time`     TIME(3) COMMENT '卫导时间（精确到毫秒）',
    `local_time`                  TIME(3) COMMENT '本地时间（精确到毫秒）',
    `message_sequence_number`     BIGINT COMMENT '消息序列号',
    `weapon_id`                   VARCHAR(50) COMMENT '武器ID',
    `weapon_type`                 VARCHAR(50) COMMENT '武器类型',
    `longitude`                   VARCHAR(50) COMMENT '经度',
    `latitude`                    VARCHAR(50) COMMENT '纬度',
    `altitude`                    VARCHAR(50) COMMENT '高度',
    `heading`                     VARCHAR(50) COMMENT '航向',
    `pitch`                       VARCHAR(50) COMMENT '俯仰',
    `north_speed`                 VARCHAR(50) COMMENT '北速',
    `sky_speed`                   VARCHAR(50) COMMENT '天速',
    `east_speed`                  VARCHAR(50) COMMENT '东速',
    `seeker_id`                   VARCHAR(50) COMMENT '导引头号',
    `interception_flag`           VARCHAR(50) COMMENT '截获标志',
    `termination_flag`            VARCHAR(50) COMMENT '终止标志',
    `intercepting_member_id`      VARCHAR(50) COMMENT '截获成员ID',
    `intercepting_equipment_id`   VARCHAR(50) COMMENT '截获装备ID',
    `intercepting_equipment_type` VARCHAR(50) COMMENT '截获装备类型',
    `launcher_id`                 VARCHAR(50) COMMENT '发射方ID',
    `seeker_azimuth_center`       VARCHAR(50) COMMENT '导引头方位中心',
    `seeker_pitch_center`         VARCHAR(50) COMMENT '导引头俯仰中心',
    `target_id`                   VARCHAR(50) COMMENT '目标ID',
    `missile_target_distance`     VARCHAR(50) COMMENT '弹目距离',
    INDEX idx_sortie_number (`sortie_number`)
) COMMENT = 'AgTraj' PARTITION BY KEY (`sortie_number`) PARTITIONS 1;

CREATE TABLE IF NOT EXISTS `simulation`.`cd_drone_plane_state`
(
    `sortie_number`            VARCHAR(50) COMMENT '架次号',
    `aircraft_id`              VARCHAR(50) COMMENT '飞机ID',
    `message_time`             TIME(3) COMMENT '消息时间（精确到毫秒）',
    `satellite_guidance_time`  TIME(3) COMMENT '卫导时间（精确到毫秒）',
    `local_time`               TIME(3) COMMENT '本地时间（精确到毫秒）',
    `message_sequence_number`  BIGINT COMMENT '消息序列号',
    `true_angle_of_attack`     VARCHAR(50) COMMENT '真攻角',
    `mach_number`              VARCHAR(50) COMMENT '马赫数',
    `normal_load_factor`       VARCHAR(50) COMMENT '法向过载',
    `indicated_airspeed`       VARCHAR(50) COMMENT '表速(km/h)',
    `field_elevation`          VARCHAR(50) COMMENT '场高',
    `radio_altitude`           VARCHAR(50) COMMENT '无线电高度',
    `remaining_fuel`           VARCHAR(50) COMMENT '余油量',
    `manual_respawn`           VARCHAR(50) COMMENT '手动复活',
    `parameter_setting_status` VARCHAR(50) COMMENT '参数设置状态',
    INDEX idx_sortie_number (`sortie_number`)
) COMMENT = 'CdDronePlaneState' PARTITION BY KEY (`sortie_number`) PARTITIONS 1;

CREATE TABLE IF NOT EXISTS `simulation`.`cd_drone_tspi`
(
    `sortie_number`           VARCHAR(50) COMMENT '架次号',
    `aircraft_id`             VARCHAR(50) COMMENT '飞机ID',
    `aircraft_type`           VARCHAR(50) COMMENT '飞机类型',
    `message_time`            TIME(3) COMMENT '消息时间（精确到毫秒）',
    `satellite_guidance_time` TIME(3) COMMENT '卫导时间（精确到毫秒）',
    `local_time`              TIME(3) COMMENT '本地时间（精确到毫秒）',
    `message_sequence_number` BIGINT COMMENT '消息序列号',
    `longitude`               VARCHAR(50) COMMENT '经度',
    `latitude`                VARCHAR(50) COMMENT '纬度',
    `pressure_altitude`       VARCHAR(50) COMMENT '气压高度',
    `roll`                    VARCHAR(50) COMMENT '横滚',
    `pitch`                   VARCHAR(50) COMMENT '俯仰',
    `heading`                 VARCHAR(50) COMMENT '航向',
    `satellite_altitude`      VARCHAR(50) COMMENT '卫星高度',
    `training_status`         VARCHAR(50) COMMENT '训练状态',
    `chaff`                   VARCHAR(50) COMMENT '干扰弹',
    `afterburner`             VARCHAR(50) COMMENT '加力',
    `north_velocity`          VARCHAR(50) COMMENT '北向速度',
    `vertical_velocity`       VARCHAR(50) COMMENT '天向速度',
    `east_velocity`           VARCHAR(50) COMMENT '东向速度',
    `delay_status`            VARCHAR(50) COMMENT '延迟状态',
    INDEX idx_sortie_number (`sortie_number`)
) COMMENT = 'CdDroneTspi' PARTITION BY KEY (`sortie_number`) PARTITIONS 1;

CREATE TABLE IF NOT EXISTS `simulation`.`cmb_power`
(
    `sortie_number`                  VARCHAR(50) COMMENT '架次号',
    `ground_defense_id`              VARCHAR(50) COMMENT '地防ID',
    `message_time`                   TIME(3) COMMENT '消息时间（精确到毫秒）',
    `satellite_guidance_time`        TIME(3) COMMENT '卫导时间（精确到毫秒）',
    `local_time`                     TIME(3) COMMENT '本地时间（精确到毫秒）',
    `message_sequence_number`        BIGINT COMMENT '消息序列号',
    `available_missile_count`        VARCHAR(50) COMMENT '可用导弹数量',
    `available_target_channel_count` VARCHAR(50) COMMENT '可用目标通道数量',
    `training_mode`                  VARCHAR(50) COMMENT '训练模式',
    `allow_missile_reset`            VARCHAR(50) COMMENT '允许重置导弹',
    `auto_respawn`                   VARCHAR(50) COMMENT '自主复活',
    INDEX idx_sortie_number (`sortie_number`)
) COMMENT = 'CmbPower' PARTITION BY KEY (`sortie_number`) PARTITIONS 1;

CREATE TABLE IF NOT EXISTS `simulation`.`command`
(
    `sortie_number`            VARCHAR(50) COMMENT '架次号',
    `aircraft_id`              VARCHAR(50) COMMENT '飞机ID',
    `message_time`             TIME(3) COMMENT '消息时间（精确到毫秒）',
    `satellite_guidance_time`  TIME(3) COMMENT '卫导时间（精确到毫秒）',
    `local_time`               TIME(3) COMMENT '本地时间（精确到毫秒）',
    `message_sequence_number`  BIGINT COMMENT '消息序列号',
    `command_type`             VARCHAR(50) COMMENT '导调/导调回复',
    `command_id`               VARCHAR(50) COMMENT '命令ID',
    `command_content`          VARCHAR(50) COMMENT '命令内容',
    `response_sequence_number` BIGINT COMMENT '回复序列号',
    INDEX idx_sortie_number (`sortie_number`)
) COMMENT = 'Command' PARTITION BY KEY (`sortie_number`) PARTITIONS 1;

CREATE TABLE IF NOT EXISTS `simulation`.`eo_state`
(
    `sortie_number`           VARCHAR(50) COMMENT '架次号',
    `aircraft_id`             VARCHAR(50) COMMENT '飞机ID',
    `message_time`            TIME(3) COMMENT '消息时间（精确到毫秒）',
    `satellite_guidance_time` TIME(3) COMMENT '卫导时间（精确到毫秒）',
    `local_time`              TIME(3) COMMENT '本地时间（精确到毫秒）',
    `message_sequence_number` BIGINT COMMENT '消息序列号',
    `working_mode`            VARCHAR(50) COMMENT '工作模式',
    `power_status`            VARCHAR(50) COMMENT '开机状态',
    `standby_status`          VARCHAR(50) COMMENT '待机状态',
    INDEX idx_sortie_number (`sortie_number`)
) COMMENT = 'EoState' PARTITION BY KEY (`sortie_number`) PARTITIONS 1;

CREATE TABLE IF NOT EXISTS `simulation`.`error_data`
(
    `sortie_number`           VARCHAR(50) COMMENT '架次号',
    `sender_id`               VARCHAR(50) COMMENT '发送方ID',
    `message_time`            TIME(3) COMMENT '消息时间（精确到毫秒）',
    `local_time`              TIME(3) COMMENT '本地时间（精确到毫秒）',
    `message_sequence_number` BIGINT COMMENT '消息序列号',
    `message_id`              VARCHAR(50) COMMENT '消息标识',
    `message_length`          VARCHAR(50) COMMENT '消息长度',
    `error_message`           VARCHAR(50) COMMENT '错误信息',
    INDEX idx_sortie_number (`sortie_number`)
) COMMENT = 'ErrorData' PARTITION BY KEY (`sortie_number`) PARTITIONS 1;

CREATE TABLE IF NOT EXISTS `simulation`.`ews`
(
    `sortie_number`           VARCHAR(50) COMMENT '架次号',
    `aircraft_id`             VARCHAR(50) COMMENT '飞机ID',
    `message_time`            TIME(3) COMMENT '消息时间（精确到毫秒）',
    `satellite_guidance_time` TIME(3) COMMENT '卫导时间（精确到毫秒）',
    `local_time`              TIME(3) COMMENT '本地时间（精确到毫秒）',
    `message_sequence_number` BIGINT COMMENT '消息序列号',
    `left_front_jamming`      VARCHAR(50) COMMENT '左前干扰',
    `right_front_jamming`     VARCHAR(50) COMMENT '右前干扰',
    `left_rear_jamming`       VARCHAR(50) COMMENT '左后干扰',
    `right_rear_jamming`      VARCHAR(50) COMMENT '右后干扰',
    `pod_forward_jamming`     VARCHAR(50) COMMENT '吊舱前向干扰',
    `pod_rearward_jamming`    VARCHAR(50) COMMENT '吊舱后向干扰',
    INDEX idx_sortie_number (`sortie_number`)
) COMMENT = 'Ews' PARTITION BY KEY (`sortie_number`) PARTITIONS 1;

CREATE TABLE IF NOT EXISTS `simulation`.`ews_y8g`
(
    `sortie_number`                VARCHAR(50) COMMENT '架次号',
    `aircraft_id`                  VARCHAR(50) COMMENT '飞机ID',
    `message_time`                 TIME(3) COMMENT '消息时间（精确到毫秒）',
    `satellite_guidance_time`      TIME(3) COMMENT '卫导时间（精确到毫秒）',
    `local_time`                   TIME(3) COMMENT '本地时间（精确到毫秒）',
    `message_sequence_number`      BIGINT COMMENT '消息序列号',
    `x_band_jamming_elevation`     VARCHAR(50) COMMENT 'X波段干扰俯仰',
    `c_band_jamming_elevation`     VARCHAR(50) COMMENT 'C波段干扰俯仰',
    `c_band_jamming_azimuth`       VARCHAR(50) COMMENT 'C波段干扰方位',
    `s_band_jamming_elevation`     VARCHAR(50) COMMENT 'S波段干扰俯仰',
    `s_band_jamming_azimuth`       VARCHAR(50) COMMENT 'S波段干扰方位',
    `l_band_jamming_elevation`     VARCHAR(50) COMMENT 'L波段干扰俯仰',
    `l_band_jamming_azimuth`       VARCHAR(50) COMMENT 'L波段干扰方位',
    `u_band_jamming_azimuth`       VARCHAR(50) COMMENT 'U波段干扰方位',
    `jamming_status`               VARCHAR(50) COMMENT '干扰状态',
    `x_band_jamming_azimuth_angle` VARCHAR(50) COMMENT 'X波段干扰方位角',
    INDEX idx_sortie_number (`sortie_number`)
) COMMENT = 'EwsY8G' PARTITION BY KEY (`sortie_number`) PARTITIONS 1;

CREATE TABLE IF NOT EXISTS `simulation`.`ews_y9t`
(
    `sortie_number`           VARCHAR(50) COMMENT '架次号',
    `aircraft_id`             VARCHAR(50) COMMENT '飞机ID',
    `message_time`            TIME(3) COMMENT '消息时间（精确到毫秒）',
    `satellite_guidance_time` TIME(3) COMMENT '卫导时间（精确到毫秒）',
    `local_time`              TIME(3) COMMENT '本地时间（精确到毫秒）',
    `message_sequence_number` BIGINT COMMENT '消息序列号',
    `jamming_azimuth`         VARCHAR(50) COMMENT '干扰方位角',
    `jamming_frequency_count` VARCHAR(50) COMMENT '干扰频点数量',
    `jamming_type`            VARCHAR(50) COMMENT '干扰类型',
    `jamming_band`            VARCHAR(50) COMMENT '干扰波段',
    `jamming_direction`       VARCHAR(50) COMMENT '干扰方向',
    `jamming_status`          VARCHAR(50) COMMENT '干扰状态',
    `jamming_elevation`       VARCHAR(50) COMMENT '干扰俯仰角',
    `jamming_start_frequency` VARCHAR(50) COMMENT '干扰开始频率',
    `jamming_end_frequency`   VARCHAR(50) COMMENT '干扰终止频率',
    INDEX idx_sortie_number (`sortie_number`)
) COMMENT = 'EwsY9T' PARTITION BY KEY (`sortie_number`) PARTITIONS 1;

CREATE TABLE IF NOT EXISTS `simulation`.`ews_yz8`
(
    `sortie_number`                VARCHAR(50) COMMENT '架次号',
    `aircraft_id`                  VARCHAR(50) COMMENT '飞机ID',
    `message_time`                 TIME(3) COMMENT '消息时间（精确到毫秒）',
    `satellite_guidance_time`      TIME(3) COMMENT '卫导时间（精确到毫秒）',
    `local_time`                   TIME(3) COMMENT '本地时间（精确到毫秒）',
    `message_sequence_number`      BIGINT COMMENT '消息序列号',
    `working_status`               VARCHAR(50) COMMENT '工作状态',
    `omnidirectional_detection_q1` VARCHAR(50) COMMENT '全向探测Q1频段',
    `omnidirectional_detection_q2` VARCHAR(50) COMMENT '全向探测Q2频段',
    `precise_direction_finding_j1` VARCHAR(50) COMMENT '精测向J1频段',
    `precise_direction_finding_j2` VARCHAR(50) COMMENT '精测向J2频段',
    `precise_direction_finding_j3` VARCHAR(50) COMMENT '精测向J3频段',
    `high_gain_g1`                 VARCHAR(50) COMMENT '高增益G1频段',
    `high_gain_g2`                 VARCHAR(50) COMMENT '高增益G2频段',
    INDEX idx_sortie_number (`sortie_number`)
) COMMENT = 'EwsYZ8' PARTITION BY KEY (`sortie_number`) PARTITIONS 1;

CREATE TABLE IF NOT EXISTS `simulation`.`g_tspi`
(
    `sortie_number`           VARCHAR(50) COMMENT '架次号',
    `ground_defense_id`       VARCHAR(50) COMMENT '地防ID',
    `message_time`            TIME(3) COMMENT '消息时间（精确到毫秒）',
    `satellite_guidance_time` TIME(3) COMMENT '卫导时间（精确到毫秒）',
    `local_time`              TIME(3) COMMENT '本地时间（精确到毫秒）',
    `message_sequence_number` BIGINT COMMENT '消息序列号',
    `camp_type`               VARCHAR(50) COMMENT '营地类型',
    `equipment_type`          VARCHAR(50) COMMENT '装备类型',
    `equipment_id`            VARCHAR(50) COMMENT '装备ID',
    `status`                  VARCHAR(50) COMMENT '状态',
    `north_angle`             VARCHAR(50) COMMENT '指北角',
    `longitude`               VARCHAR(50) COMMENT '经度',
    `latitude`                VARCHAR(50) COMMENT '纬度',
    `altitude`                VARCHAR(50) COMMENT '高度',
    `decoy_position_id`       VARCHAR(50) COMMENT '假阵地编号',
    INDEX idx_sortie_number (`sortie_number`)
) COMMENT = 'GTspi' PARTITION BY KEY (`sortie_number`) PARTITIONS 1;

CREATE TABLE IF NOT EXISTS `simulation`.`ews_kj500`
(
    `sortie_number`           VARCHAR(50) COMMENT '架次号',
    `aircraft_id`             VARCHAR(50) COMMENT '飞机ID',
    `message_time`            TIME(3) COMMENT '消息时间（精确到毫秒）',
    `satellite_guidance_time` TIME(3) COMMENT '卫导时间（精确到毫秒）',
    `local_time`              TIME(3) COMMENT '本地时间（精确到毫秒）',
    `message_sequence_number` BIGINT COMMENT '消息序列号',
    `azimuth_start_angle`     VARCHAR(50) COMMENT '方位起始角',
    `sector_reference`        VARCHAR(50) COMMENT '扇区相对基准',
    `sector_type`             VARCHAR(50) COMMENT '扇区类型',
    `sector_number`           VARCHAR(50) COMMENT '扇区号',
    `action_code`             VARCHAR(50) COMMENT '动作代码',
    `azimuth_end_angle`       VARCHAR(50) COMMENT '范围终止角',
    INDEX idx_sortie_number (`sortie_number`)
) COMMENT = 'EwsKJ500' PARTITION BY KEY (`sortie_number`) PARTITIONS 1;

CREATE TABLE IF NOT EXISTS `simulation`.`g_tspi_back`
(
    `sortie_number`                    VARCHAR(50) COMMENT '架次号',
    `ground_defense_id`                VARCHAR(50) COMMENT '地防ID',
    `message_time`                     TIME(3) COMMENT '消息时间（精确到毫秒）',
    `satellite_guidance_time`          TIME(3) COMMENT '卫导时间（精确到毫秒）',
    `local_time`                       TIME(3) COMMENT '本地时间（精确到毫秒）',
    `message_sequence_number`          BIGINT COMMENT '消息序列号',
    `response_message_sequence_number` BIGINT COMMENT '回复消息序列号',
    INDEX idx_sortie_number (`sortie_number`)
) COMMENT = 'GTspiBack' PARTITION BY KEY (`sortie_number`) PARTITIONS 1;

CREATE TABLE IF NOT EXISTS `simulation`.`hit_result`
(
    `sortie_number` VARCHAR(50) COMMENT '架次号',
    `launcher_id`   VARCHAR(50) COMMENT '发射方ID',
    `target_id`     VARCHAR(50) COMMENT '目标ID',
    `weapon_type`   VARCHAR(50) COMMENT '武器类型',
    `weapon_id`     VARCHAR(50) COMMENT '武器ID',
    `launch_time`   VARCHAR(50) COMMENT '发射时间',
    `end_time`      TIME(3) COMMENT '结束时间（精确到毫秒）',
    `hit_result`    VARCHAR(50) COMMENT '命中结果',
    INDEX idx_sortie_number (`sortie_number`)
) COMMENT = 'HitResult' PARTITION BY KEY (`sortie_number`) PARTITIONS 1;

CREATE TABLE IF NOT EXISTS `simulation`.`hj_plane_data`
(
    `sortie_number`           VARCHAR(50) COMMENT '架次号',
    `batch_number`            VARCHAR(50) COMMENT '批次号',
    `device_number`           VARCHAR(50) COMMENT '设备号',
    `flight_control_number`   VARCHAR(50) COMMENT '航管号',
    `local_time`              TIME(3) COMMENT '本地时间（精确到毫秒）',
    `message_time`            TIME(3) COMMENT '消息时间（精确到毫秒）',
    `message_sequence_number` BIGINT COMMENT '消息序列号',
    `longitude`               VARCHAR(50) COMMENT '经度',
    `latitude`                VARCHAR(50) COMMENT '纬度',
    `altitude`                VARCHAR(50) COMMENT '高度',
    `ground_speed`            VARCHAR(50) COMMENT '地速',
    `vertical_speed`          VARCHAR(50) COMMENT '垂直速度',
    `heading`                 VARCHAR(50) COMMENT '航向',
    INDEX idx_sortie_number (`sortie_number`)
) COMMENT = 'HJPlaneData' PARTITION BY KEY (`sortie_number`) PARTITIONS 1;

CREATE TABLE IF NOT EXISTS `simulation`.`ir_msl`
(
    `sortie_number`           VARCHAR(50) COMMENT '架次号',
    `aircraft_id`             VARCHAR(50) COMMENT '飞机ID',
    `message_time`            TIME(3) COMMENT '消息时间（精确到毫秒）',
    `satellite_guidance_time` TIME(3) COMMENT '卫导时间（精确到毫秒）',
    `local_time`              TIME(3) COMMENT '本地时间（精确到毫秒）',
    `message_sequence_number` BIGINT COMMENT '消息序列号',
    `seeker_azimuth`          VARCHAR(50) COMMENT '导引头方位角',
    `seeker_elevation`        VARCHAR(50) COMMENT '导引头俯仰角',
    `weapon_type`             VARCHAR(50) COMMENT '武器类型',
    `interception_flag`       VARCHAR(50) COMMENT '截获标识',
    INDEX idx_sortie_number (`sortie_number`)
) COMMENT = 'IrMsl' PARTITION BY KEY (`sortie_number`) PARTITIONS 1;

CREATE TABLE IF NOT EXISTS `simulation`.`pl17_rtkn`
(
    `sortie_number`           VARCHAR(50) COMMENT '架次号',
    `aircraft_id`             VARCHAR(50) COMMENT '飞机ID',
    `message_time`            TIME(3) COMMENT '消息时间（精确到毫秒）',
    `satellite_guidance_time` TIME(3) COMMENT '卫导时间（精确到毫秒）',
    `local_time`              TIME(3) COMMENT '本地时间（精确到毫秒）',
    `message_sequence_number` BIGINT COMMENT '消息序列号',
    `weapon_id`               VARCHAR(50) COMMENT '武器ID',
    `weapon_type`             VARCHAR(50) COMMENT '武器类型',
    `target_id`               VARCHAR(50) COMMENT '目标ID',
    `target_real_or_virtual`  VARCHAR(50) COMMENT '目标机实虚属性',
    `hit_result`              VARCHAR(50) COMMENT '命中结果',
    `miss_reason`             VARCHAR(50) COMMENT '未命中原因',
    `miss_distance`           VARCHAR(50) COMMENT '脱靶量',
    `matching_failure_reason` VARCHAR(50) COMMENT '匹配失败原因',
    `jamming_effective`       VARCHAR(50) COMMENT '干扰是否有效',
    `jamming`                 VARCHAR(50) COMMENT '干扰',
    `afterburner`             VARCHAR(50) COMMENT '加力',
    `head_on`                 VARCHAR(50) COMMENT '迎头',
    `heading`                 VARCHAR(50) COMMENT '航向',
    `pitch`                   VARCHAR(50) COMMENT '俯仰',
    INDEX idx_sortie_number (`sortie_number`)
) COMMENT = 'PL17Rtkn' PARTITION BY KEY (`sortie_number`) PARTITIONS 1;

CREATE TABLE IF NOT EXISTS `simulation`.`pl17_rtsn`
(
    `sortie_number`           VARCHAR(50) COMMENT '架次号',
    `aircraft_id`             VARCHAR(50) COMMENT '飞机ID',
    `message_time`            TIME(3) COMMENT '消息时间（精确到毫秒）',
    `satellite_guidance_time` TIME(3) COMMENT '卫导时间（精确到毫秒）',
    `local_time`              TIME(3) COMMENT '本地时间（精确到毫秒）',
    `message_sequence_number` BIGINT COMMENT '消息序列号',
    `target_id`               VARCHAR(50) COMMENT '目标ID',
    `intercepted_weapon_id`   VARCHAR(50) COMMENT '被拦截武器ID',
    `target_real_or_virtual`  VARCHAR(50) COMMENT '目标实虚属性',
    `weapon_id`               VARCHAR(50) COMMENT '武器ID',
    `pylon_id`                VARCHAR(50) COMMENT '挂架ID',
    `weapon_type`             VARCHAR(50) COMMENT '武器类型',
    `trajectory_type`         VARCHAR(50) COMMENT '弹道类型',
    `missile_attack_mode`     VARCHAR(50) COMMENT '导弹攻击模式',
    INDEX idx_sortie_number (`sortie_number`)
) COMMENT = 'PL17Rtsn' PARTITION BY KEY (`sortie_number`) PARTITIONS 1;

CREATE TABLE IF NOT EXISTS `simulation`.`pl17_traj`
(
    `sortie_number`                  VARCHAR(50) COMMENT '架次号',
    `aircraft_id`                    VARCHAR(50) COMMENT '飞机ID',
    `message_time`                   TIME(3) COMMENT '消息时间（精确到毫秒）',
    `satellite_guidance_time`        TIME(3) COMMENT '卫导时间（精确到毫秒）',
    `local_time`                     TIME(3) COMMENT '本地时间（精确到毫秒）',
    `message_sequence_number`        BIGINT COMMENT '消息序列号',
    `weapon_id`                      VARCHAR(50) COMMENT '武器ID',
    `pylon_id`                       VARCHAR(50) COMMENT '挂架ID',
    `weapon_type`                    VARCHAR(50) COMMENT '武器类型',
    `target_id`                      VARCHAR(50) COMMENT '目标ID',
    `longitude`                      VARCHAR(50) COMMENT '经度',
    `latitude`                       VARCHAR(50) COMMENT '纬度',
    `altitude`                       VARCHAR(50) COMMENT '高度',
    `missile_target_distance`        VARCHAR(50) COMMENT '弹目距离',
    `missile_speed`                  VARCHAR(50) COMMENT '弹速度(m/s)',
    `interception_status`            VARCHAR(50) COMMENT '截获状态',
    `non_interception_reason`        VARCHAR(50) COMMENT '未截获原因',
    `seeker_azimuth`                 VARCHAR(50) COMMENT '导引头视线方位角',
    `seeker_elevation`               VARCHAR(50) COMMENT '导引头视线俯仰角',
    `target_tspi_status`             VARCHAR(50) COMMENT '目标TSPI状态',
    `command_machine_status`         VARCHAR(50) COMMENT '指令机状态',
    `ground_angle_satisfaction_flag` VARCHAR(50) COMMENT '擦地角满足标志',
    `zero_crossing_flag`             VARCHAR(50) COMMENT '过零标志',
    `distance_interception_flag`     VARCHAR(50) COMMENT '距离截获标志',
    `speed_interception_flag`        VARCHAR(50) COMMENT '速度截获标志',
    `angle_interception_flag`        VARCHAR(50) COMMENT '角度截获标志',
    INDEX idx_sortie_number (`sortie_number`)
) COMMENT = 'PL17Traj' PARTITION BY KEY (`sortie_number`) PARTITIONS 1;

CREATE TABLE IF NOT EXISTS `simulation`.`plane_pro`
(
    `sortie_number`        VARCHAR(50) COMMENT '架次号',
    `aircraft_id`          VARCHAR(50) COMMENT '飞机ID',
    `aircraft_callsign`    VARCHAR(50) COMMENT '飞机代字',
    `aircraft_code_name`   VARCHAR(50) COMMENT '飞机代号',
    `red_blue_affiliation` VARCHAR(50) COMMENT '红蓝属性',
    `flight_batch`         VARCHAR(50) COMMENT '飞行批次',
    INDEX idx_sortie_number (`sortie_number`)
) COMMENT = 'PlanePro' PARTITION BY KEY (`sortie_number`) PARTITIONS 1;

CREATE TABLE IF NOT EXISTS `simulation`.`plane_state`
(
    `sortie_number`            VARCHAR(50) COMMENT '架次号',
    `aircraft_id`              VARCHAR(50) COMMENT '飞机ID',
    `message_time`             TIME(3) COMMENT '消息时间（精确到毫秒）',
    `satellite_guidance_time`  TIME(3) COMMENT '卫导时间（精确到毫秒）',
    `local_time`               TIME(3) COMMENT '本地时间（精确到毫秒）',
    `message_sequence_number`  BIGINT COMMENT '消息序列号',
    `true_angle_of_attack`     VARCHAR(50) COMMENT '真攻角',
    `mach_number`              VARCHAR(50) COMMENT '马赫数',
    `normal_load_factor`       VARCHAR(50) COMMENT '法向过载',
    `indicated_airspeed`       VARCHAR(50) COMMENT '表速(km/h)',
    `field_elevation`          VARCHAR(50) COMMENT '场高',
    `radio_altitude`           VARCHAR(50) COMMENT '无线电高度',
    `remaining_fuel`           VARCHAR(50) COMMENT '余油量',
    `scenario`                 VARCHAR(50) COMMENT '场景',
    `manual_respawn`           VARCHAR(50) COMMENT '手动复活',
    `parameter_setting_status` VARCHAR(50) COMMENT '参数设置状态',
    `encryption_status`        VARCHAR(50) COMMENT '加密状态',
    INDEX idx_sortie_number (`sortie_number`)
) COMMENT = 'PlaneState' PARTITION BY KEY (`sortie_number`) PARTITIONS 1;

CREATE TABLE IF NOT EXISTS `simulation`.`rdr_state`
(
    `sortie_number`           VARCHAR(50) COMMENT '架次号',
    `aircraft_id`             VARCHAR(50) COMMENT '飞机ID',
    `message_time`            TIME(3) COMMENT '消息时间（精确到毫秒）',
    `satellite_guidance_time` TIME(3) COMMENT '卫导时间（精确到毫秒）',
    `local_time`              TIME(3) COMMENT '本地时间（精确到毫秒）',
    `message_sequence_number` BIGINT COMMENT '消息序列号',
    `working_mode`            VARCHAR(50) COMMENT '工作模式',
    `air_sea_scan_ratio`      VARCHAR(50) COMMENT '空海扫描比',
    `power_status`            VARCHAR(50) COMMENT '开机状态',
    `emission_status`         VARCHAR(50) COMMENT '辐射状态',
    INDEX idx_sortie_number (`sortie_number`)
) COMMENT = 'RdrState' PARTITION BY KEY (`sortie_number`) PARTITIONS 1;

CREATE TABLE IF NOT EXISTS `simulation`.`rtsn`
(
    `sortie_number`           VARCHAR(50) COMMENT '架次号',
    `aircraft_id`             VARCHAR(50) COMMENT '飞机ID',
    `message_time`            TIME(3) COMMENT '消息时间（精确到毫秒）',
    `satellite_guidance_time` TIME(3) COMMENT '卫导时间（精确到毫秒）',
    `local_time`              TIME(3) COMMENT '本地时间（精确到毫秒）',
    `message_sequence_number` BIGINT COMMENT '消息序列号',
    `target_id`               VARCHAR(50) COMMENT '目标ID',
    `intercepted_weapon_id`   VARCHAR(50) COMMENT '被拦截武器ID',
    `target_real_or_virtual`  VARCHAR(50) COMMENT '目标实虚属性',
    `weapon_id`               VARCHAR(50) COMMENT '武器ID',
    `pylon_id`                VARCHAR(50) COMMENT '挂架ID',
    `weapon_type`             VARCHAR(50) COMMENT '武器类型',
    `trajectory_type`         VARCHAR(50) COMMENT '弹道类型',
    `missile_attack_mode`     VARCHAR(50) COMMENT '导弹攻击模式',
    INDEX idx_sortie_number (`sortie_number`)
) COMMENT = 'Rtsn' PARTITION BY KEY (`sortie_number`) PARTITIONS 1;

CREATE TABLE IF NOT EXISTS `simulation`.`sa_tgt`
(
    `sortie_number`           VARCHAR(50) COMMENT '架次号',
    `ground_defense_id`       VARCHAR(50) COMMENT '地防ID',
    `message_time`            TIME(3) COMMENT '消息时间（精确到毫秒）',
    `satellite_guidance_time` TIME(3) COMMENT '卫导时间（精确到毫秒）',
    `local_time`              TIME(3) COMMENT '本地时间（精确到毫秒）',
    `message_sequence_number` BIGINT COMMENT '消息序列号',
    `equipment_type`          VARCHAR(50) COMMENT '装备类型',
    `equipment_number`        VARCHAR(50) COMMENT '装备编号',
    `target_azimuth`          VARCHAR(50) COMMENT '目标方位角',
    `target_elevation`        VARCHAR(50) COMMENT '目标俯仰角',
    `target_slant_range`      VARCHAR(50) COMMENT '目标斜距',
    `channel_number`          VARCHAR(50) COMMENT '通道号',
    `target_batch_number`     VARCHAR(50) COMMENT '目标批号',
    INDEX idx_sortie_number (`sortie_number`)
) COMMENT = 'SaTgt' PARTITION BY KEY (`sortie_number`) PARTITIONS 1;

CREATE TABLE IF NOT EXISTS `simulation`.`sa_traj`
(
    `sortie_number`           VARCHAR(50) COMMENT '架次号',
    `aircraft_id`             VARCHAR(50) COMMENT '飞机ID',
    `message_time`            TIME(3) COMMENT '消息时间（精确到毫秒）',
    `satellite_guidance_time` TIME(3) COMMENT '卫导时间（精确到毫秒）',
    `local_time`              TIME(3) COMMENT '本地时间（精确到毫秒）',
    `message_sequence_number` BIGINT COMMENT '消息序列号',
    `weapon_id`               VARCHAR(50) COMMENT '武器ID',
    `pylon_id`                VARCHAR(50) COMMENT '挂架ID',
    `weapon_type`             VARCHAR(50) COMMENT '武器类型',
    `target_id`               VARCHAR(50) COMMENT '目标ID',
    `intercepted_weapon_id`   VARCHAR(50) COMMENT '被拦截武器ID',
    `longitude`               VARCHAR(50) COMMENT '经度',
    `latitude`                VARCHAR(50) COMMENT '纬度',
    `altitude`                VARCHAR(50) COMMENT '高度',
    `missile_target_distance` VARCHAR(50) COMMENT '弹目距离',
    `missile_speed`           VARCHAR(50) COMMENT '弹速度(m/s)',
    `interception_status`     VARCHAR(50) COMMENT '截获状态',
    `non_interception_reason` VARCHAR(50) COMMENT '未截获原因',
    `seeker_azimuth`          VARCHAR(50) COMMENT '导引头视线方位角',
    `seeker_elevation`        VARCHAR(50) COMMENT '导引头视线俯仰角',
    `target_tspi_status`      VARCHAR(50) COMMENT '目标TSPI状态',
    `command_machine_status`  VARCHAR(50) COMMENT '指令机状态',
    INDEX idx_sortie_number (`sortie_number`)
) COMMENT = 'SaTraj' PARTITION BY KEY (`sortie_number`) PARTITIONS 1;

CREATE TABLE IF NOT EXISTS `simulation`.`send_to_3d_data`
(
    `sortie_number`        VARCHAR(50) COMMENT '架次号',
    `aircraft_id`          VARCHAR(50) COMMENT '飞机ID',
    `aircraft_callsign`    VARCHAR(50) COMMENT '飞机代字',
    `aircraft_code_name`   VARCHAR(50) COMMENT '飞机代号',
    `red_blue_affiliation` VARCHAR(50) COMMENT '红蓝属性',
    `flight_batch`         VARCHAR(50) COMMENT '飞行批次',
    INDEX idx_sortie_number (`sortie_number`)
) COMMENT = 'Sendto3DData' PARTITION BY KEY (`sortie_number`) PARTITIONS 1;

CREATE TABLE IF NOT EXISTS `simulation`.`rtkn`
(
    `sortie_number`                  VARCHAR(50) COMMENT '架次号',
    `aircraft_id`                    VARCHAR(50) COMMENT '飞机ID',
    `message_time`                   TIME(3) COMMENT '消息时间（精确到毫秒）',
    `satellite_guidance_time`        TIME(3) COMMENT '卫导时间（精确到毫秒）',
    `local_time`                     TIME(3) COMMENT '本地时间（精确到毫秒）',
    `message_sequence_number`        BIGINT COMMENT '消息序列号',
    `weapon_id`                      VARCHAR(50) COMMENT '武器ID',
    `weapon_type`                    VARCHAR(50) COMMENT '武器类型',
    `target_id`                      VARCHAR(50) COMMENT '目标ID',
    `intercepted_weapon_id`          VARCHAR(50) COMMENT '被拦截武器ID',
    `hit_result`                     VARCHAR(50) COMMENT '命中结果',
    `miss_reason`                    VARCHAR(50) COMMENT '未命中原因',
    `miss_distance`                  VARCHAR(50) COMMENT '脱靶量',
    `matching_failure_reason`        VARCHAR(50) COMMENT '匹配失败原因',
    `ground_defense_equipment_type`  VARCHAR(50) COMMENT '地导装备类型',
    `ground_defense_equipment_id`    VARCHAR(50) COMMENT '地导装备ID',
    `ground_defense_equipment_type1` VARCHAR(50) COMMENT '地导装备类型.1',
    `ground_defense_equipment_id1`   VARCHAR(50) COMMENT '地导装备ID.1',
    `ground_defense_equipment_type2` VARCHAR(50) COMMENT '地导装备类型.2',
    `ground_defense_equipment_id2`   VARCHAR(50) COMMENT '地导装备ID.2',
    `ground_defense_equipment_type3` VARCHAR(50) COMMENT '地导装备类型.3',
    `ground_defense_equipment_id3`   VARCHAR(50) COMMENT '地导装备ID.3',
    `jamming_effective`              VARCHAR(50) COMMENT '干扰是否有效',
    `jamming`                        VARCHAR(50) COMMENT '干扰',
    `afterburner`                    VARCHAR(50) COMMENT '加力',
    `head_on`                        VARCHAR(50) COMMENT '迎头',
    `heading`                        VARCHAR(50) COMMENT '航向',
    `pitch`                          VARCHAR(50) COMMENT '俯仰',
    INDEX idx_sortie_number (`sortie_number`)
) COMMENT = 'Rtkn' PARTITION BY KEY (`sortie_number`) PARTITIONS 1;

CREATE TABLE IF NOT EXISTS `simulation`.`srd_state`
(
    `sortie_number`           VARCHAR(50) COMMENT '架次号',
    `ground_defense_id`       VARCHAR(50) COMMENT '地防ID',
    `message_time`            TIME(3) COMMENT '消息时间（精确到毫秒）',
    `satellite_guidance_time` TIME(3) COMMENT '卫导时间（精确到毫秒）',
    `local_time`              TIME(3) COMMENT '本地时间（精确到毫秒）',
    `message_sequence_number` BIGINT COMMENT '消息序列号',
    `camp_type`               VARCHAR(50) COMMENT '营地类型',
    `equipment_type`          VARCHAR(50) COMMENT '装备类型',
    `equipment_number`        VARCHAR(50) COMMENT '装备编号',
    `power_on`                VARCHAR(50) COMMENT '是否开机',
    `emitter_frequency_band`  VARCHAR(50) COMMENT '辐射源频段',
    `decoy_blink_period`      VARCHAR(50) COMMENT '诱饵闪烁周期',
    `antenna_pointing`        VARCHAR(50) COMMENT '天线指向',
    INDEX idx_sortie_number (`sortie_number`)
) COMMENT = 'SrdState' PARTITION BY KEY (`sortie_number`) PARTITIONS 1;

CREATE TABLE IF NOT EXISTS `simulation`.`tgt`
(
    `sortie_number`           VARCHAR(50) COMMENT '架次号',
    `aircraft_id`             VARCHAR(50) COMMENT '飞机ID',
    `message_time`            TIME(3) COMMENT '消息时间（精确到毫秒）',
    `satellite_guidance_time` TIME(3) COMMENT '卫导时间（精确到毫秒）',
    `local_time`              TIME(3) COMMENT '本地时间（精确到毫秒）',
    `message_sequence_number` BIGINT COMMENT '消息序列号',
    `target_count`            VARCHAR(50) COMMENT '目标数量',
    `identifier1`             VARCHAR(50) COMMENT '标识1',
    `sensor1`                 VARCHAR(50) COMMENT '传感器1',
    `pitch1`                  VARCHAR(50) COMMENT '俯仰1',
    `azimuth1`                VARCHAR(50) COMMENT '方位1',
    `slant_range1`            VARCHAR(50) COMMENT '斜距1',
    `identifier2`             VARCHAR(50) COMMENT '标识2',
    `sensor2`                 VARCHAR(50) COMMENT '传感器2',
    `pitch2`                  VARCHAR(50) COMMENT '俯仰2',
    `azimuth2`                VARCHAR(50) COMMENT '方位2',
    `slant_range2`            VARCHAR(50) COMMENT '斜距2',
    `identifier3`             VARCHAR(50) COMMENT '标识3',
    `sensor3`                 VARCHAR(50) COMMENT '传感器3',
    `pitch3`                  VARCHAR(50) COMMENT '俯仰3',
    `azimuth3`                VARCHAR(50) COMMENT '方位3',
    `slant_range3`            VARCHAR(50) COMMENT '斜距3',
    `identifier4`             VARCHAR(50) COMMENT '标识4',
    `sensor4`                 VARCHAR(50) COMMENT '传感器4',
    `pitch4`                  VARCHAR(50) COMMENT '俯仰4',
    `azimuth4`                VARCHAR(50) COMMENT '方位4',
    `slant_range4`            VARCHAR(50) COMMENT '斜距4',
    `identifier5`             VARCHAR(50) COMMENT '标识5',
    `sensor5`                 VARCHAR(50) COMMENT '传感器5',
    `pitch5`                  VARCHAR(50) COMMENT '俯仰5',
    `azimuth5`                VARCHAR(50) COMMENT '方位5',
    `slant_range5`            VARCHAR(50) COMMENT '斜距5',
    `identifier6`             VARCHAR(50) COMMENT '标识6',
    `sensor6`                 VARCHAR(50) COMMENT '传感器6',
    `pitch6`                  VARCHAR(50) COMMENT '俯仰6',
    `azimuth6`                VARCHAR(50) COMMENT '方位6',
    `slant_range6`            VARCHAR(50) COMMENT '斜距6',
    `identifier7`             VARCHAR(50) COMMENT '标识7',
    `sensor7`                 VARCHAR(50) COMMENT '传感器7',
    `pitch7`                  VARCHAR(50) COMMENT '俯仰7',
    `azimuth7`                VARCHAR(50) COMMENT '方位7',
    `slant_range7`            VARCHAR(50) COMMENT '斜距7',
    `identifier8`             VARCHAR(50) COMMENT '标识8',
    `sensor8`                 VARCHAR(50) COMMENT '传感器8',
    `pitch8`                  VARCHAR(50) COMMENT '俯仰8',
    `azimuth8`                VARCHAR(50) COMMENT '方位8',
    `slant_range8`            VARCHAR(50) COMMENT '斜距8',
    INDEX idx_sortie_number (`sortie_number`)
) COMMENT = 'Tgt' PARTITION BY KEY (`sortie_number`) PARTITIONS 1;

CREATE TABLE IF NOT EXISTS `simulation`.`tspi`
(
    `sortie_number`           VARCHAR(50) COMMENT '架次号',
    `aircraft_id`             VARCHAR(50) COMMENT '飞机ID',
    `aircraft_type`           VARCHAR(50) COMMENT '飞机类型',
    `message_time`            TIME(3) COMMENT '消息时间（精确到毫秒）',
    `satellite_guidance_time` TIME(3) COMMENT '卫导时间（精确到毫秒）',
    `local_time`              TIME(3) COMMENT '本地时间（精确到毫秒）',
    `message_sequence_number` BIGINT COMMENT '消息序列号',
    `longitude`               VARCHAR(50) COMMENT '经度',
    `latitude`                VARCHAR(50) COMMENT '纬度',
    `pressure_altitude`       VARCHAR(50) COMMENT '气压高度',
    `roll`                    VARCHAR(50) COMMENT '横滚',
    `pitch`                   VARCHAR(50) COMMENT '俯仰',
    `heading`                 VARCHAR(50) COMMENT '航向',
    `satellite_altitude`      VARCHAR(50) COMMENT '卫星高度',
    `training_status`         VARCHAR(50) COMMENT '训练状态',
    `chaff`                   VARCHAR(50) COMMENT '干扰弹',
    `afterburner`             VARCHAR(50) COMMENT '加力',
    `north_velocity`          VARCHAR(50) COMMENT '北向速度',
    `vertical_velocity`       VARCHAR(50) COMMENT '天向速度',
    `east_velocity`           VARCHAR(50) COMMENT '东向速度',
    INDEX idx_sortie_number (`sortie_number`)
) COMMENT = 'Tspi' PARTITION BY KEY (`sortie_number`) PARTITIONS 1;
