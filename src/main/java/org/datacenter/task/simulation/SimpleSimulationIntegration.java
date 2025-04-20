package org.datacenter.task.simulation;

import org.datacenter.config.HumanMachineConfig;

/**
 * @author : [wangminan]
 * @description : 简单的仿真数据整合
 */
public class SimpleSimulationIntegration {
    public static void main(String[] args) {
        HumanMachineConfig humanMachineSysConfig = new HumanMachineConfig();
        humanMachineSysConfig.loadConfig();

    }
}
