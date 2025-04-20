package org.datacenter.task.physiological;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.datacenter.util.DataIntegrationUtil;

/**
 * @author : [wangminan]
 * @description : 简单的生理数据整合
 */
public class SimplePhysiologicalIntegration {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = DataIntegrationUtil.prepareStreamEnv();
    }
}
