package com.vmturbo.topology.processor.controllable;

import java.time.Clock;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.topology.processor.TopologyProcessorDBConfig;

@Configuration
@Import(TopologyProcessorDBConfig.class)
public class ControllableConfig {
    @Value("${inProgressActionExpiredSeconds:3600}")
    private int inProgressActionExpiredSeconds;

    @Value("${moveSucceedRecordExpiredSeconds:1800}")
    private int moveSucceedRecordExpiredSeconds;

    @Value("${activateSucceedRecordExpiredSeconds:14400}")
    private int activateSucceedRecordExpiredSeconds;

    @Value("${scaleSucceedRecordExpiredSeconds:21600}")
    private int scaleSucceedRecordExpiredSeconds;

    @Value("${resizeSucceedRecordExpiredSeconds:14400}")
    private int resizeSucceedRecordExpiredSeconds;

    @Value("${accountForVendorAutomation:false}")
    private boolean accountForVendorAutomation;

    @Value("${drsMaintenanceProtectionWindow:1800}")
    private int drsMaintenanceProtectionWindow;

    @Autowired
    private TopologyProcessorDBConfig topologyProcessorDBConfig;

    @Bean
    public EntityActionDaoImp entityActionDaoImp() {
        return new EntityActionDaoImp(topologyProcessorDBConfig.dsl(), moveSucceedRecordExpiredSeconds,
                inProgressActionExpiredSeconds, activateSucceedRecordExpiredSeconds,
                scaleSucceedRecordExpiredSeconds, resizeSucceedRecordExpiredSeconds);
    }

    @Bean
    public EntityMaintenanceTimeDao entityMaintenanceTimeDao() {
        return new EntityMaintenanceTimeDao(topologyProcessorDBConfig.dsl(), drsMaintenanceProtectionWindow,
            Clock.systemUTC(), accountForVendorAutomation);
    }

    @Bean
    public ControllableManager controllableManager() {
        return new ControllableManager(entityActionDaoImp(), entityMaintenanceTimeDao(), accountForVendorAutomation);
    }
}
