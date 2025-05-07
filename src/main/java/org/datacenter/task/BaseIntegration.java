package org.datacenter.task;

import lombok.extern.slf4j.Slf4j;
import org.datacenter.config.HumanMachineConfig;

/**
 * @author : [wangminan]
 * @description : 抽象的整合基类
 */
@Slf4j
public abstract class BaseIntegration {
    public void prepare() {
        log.info("Preparing integration");
        HumanMachineConfig config = new HumanMachineConfig();
        config.loadConfig();
    }

    public abstract void run();

    public void start() {
        prepare();
        run();
    }
}
