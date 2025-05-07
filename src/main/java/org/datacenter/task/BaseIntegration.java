package org.datacenter.task;

import org.datacenter.config.HumanMachineConfig;

/**
 * @author : [wangminan]
 * @description : 抽象的整合基类
 */
public abstract class BaseIntegration {
    public void prepare() {
        HumanMachineConfig config = new HumanMachineConfig();
        config.loadConfig();
    }

    public abstract void run();

    public void start() {
        prepare();
        run();
    }
}
