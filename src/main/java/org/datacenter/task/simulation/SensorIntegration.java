package org.datacenter.task.simulation;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;
import org.datacenter.config.integration.simulation.SimulationIntegrationConfig;
import org.datacenter.config.keys.HumanMachineIntegrationConfigKey;
import org.datacenter.task.BaseIntegration;

/**
 * @author : [wangminan]
 * @description : 传感器数据整合
 */
@EqualsAndHashCode(callSuper = true)
@Data
@Slf4j
public class SensorIntegration extends BaseIntegration {

    private SimulationIntegrationConfig config;

    @Override
    public void run() {

    }

    public static void main(String[] args) {
        ParameterTool param = ParameterTool.fromArgs(args);
        String sortieNumber = param.getRequired(
                HumanMachineIntegrationConfigKey.SORTIE_NUMBER.getKeyForParamsMap()
        );
        SensorIntegration sensorIntegration = new SensorIntegration();
        sensorIntegration.setConfig(new SimulationIntegrationConfig(sortieNumber));
        sensorIntegration.start();
    }
}
