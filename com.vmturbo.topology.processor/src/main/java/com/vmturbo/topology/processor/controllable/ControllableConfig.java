package com.vmturbo.topology.processor.controllable;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.topology.processor.TopologyProcessorDBConfig;

@Configuration
@Import(TopologyProcessorDBConfig.class)
public class ControllableConfig {
    @Value("${activateOrMoveInProgressRecordExpiredSeconds}")
    private int activateOrMoveInProgressRecordExpiredSeconds;

    @Value("${moveSucceedRecordExpiredSeconds}")
    private int moveSucceedRecordExpiredSeconds;

    @Value("${activateSucceedRecordExpiredSeconds}")
    private int activateSucceedRecordExpiredSeconds;

    @Autowired
    private TopologyProcessorDBConfig topologyProcessorDBConfig;

    @Bean
    public EntityActionDaoImp entityActionDaoImp() {
        return new EntityActionDaoImp(topologyProcessorDBConfig.dsl(), moveSucceedRecordExpiredSeconds,
                activateOrMoveInProgressRecordExpiredSeconds, activateSucceedRecordExpiredSeconds);
    }

    @Bean
    public ControllableManager controllableManager() {
        return new ControllableManager(entityActionDaoImp());
    }
}
