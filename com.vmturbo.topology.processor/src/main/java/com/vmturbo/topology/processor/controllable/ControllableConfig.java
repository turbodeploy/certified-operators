package com.vmturbo.topology.processor.controllable;

import java.time.Clock;

import com.google.common.annotations.VisibleForTesting;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.action.orchestrator.api.impl.ActionOrchestratorClientConfig;
import com.vmturbo.common.protobuf.action.AffectedEntitiesServiceGrpc;
import com.vmturbo.common.protobuf.action.AffectedEntitiesServiceGrpc.AffectedEntitiesServiceBlockingStub;
import com.vmturbo.topology.processor.TopologyProcessorDBConfig;

/**
 * Configuration related to all pieces responsible for managing controllable flag like
 * EntityActionDao.
 */
@Configuration
@Import({
        TopologyProcessorDBConfig.class,
        ActionOrchestratorClientConfig.class
})
public class ControllableConfig {

    private static final Logger logger = LogManager.getLogger();

    @VisibleForTesting
    @Value("${inProgressActionExpiredSeconds:3600}")
    int inProgressActionExpiredSeconds;

    @VisibleForTesting
    @Value("${moveSucceedRecordExpiredSeconds:1800}")
    int moveSucceedRecordExpiredSeconds;

    @VisibleForTesting
    @Value("${activateSucceedRecordExpiredSeconds:14400}")
    int activateSucceedRecordExpiredSeconds;

    @VisibleForTesting
    @Value("${scaleSucceedRecordExpiredSeconds:21600}")
    int scaleSucceedRecordExpiredSeconds;

    @VisibleForTesting
    @Value("${resizeSucceedRecordExpiredSeconds:14400}")
    int resizeSucceedRecordExpiredSeconds;

    @Value("${accountForVendorAutomation:false}")
    private boolean accountForVendorAutomation;

    @Value("${drsMaintenanceProtectionWindow:1800}")
    private int drsMaintenanceProtectionWindow;

    @Value("${useAffectedEntitiesService:false}")
    private boolean useAffectedEntitiesService;

    @Autowired
    private TopologyProcessorDBConfig topologyProcessorDBConfig;

    @Autowired
    private ActionOrchestratorClientConfig aoClientConfig;

    /**
     * The implementation the updates and reads entity action, affected entities.
     *
     * @return the implementation the updates and reads entity action, affected entities.
     */
    @Bean
    public EntityActionDao entityActionDao() {
        if (useAffectedEntitiesService) {
            logger.info("Using new AffectedEntitiesServiceEntityActionDao");
            return new AffectedEntitiesServiceEntityActionDao(
                    affectedEntitiesServiceBlockingStub(),
                    moveSucceedRecordExpiredSeconds,
                    inProgressActionExpiredSeconds,
                    activateSucceedRecordExpiredSeconds,
                    scaleSucceedRecordExpiredSeconds,
                    resizeSucceedRecordExpiredSeconds);
        } else {
            logger.info("Using old EntityActionDaoImp");
            return new EntityActionDaoImp(topologyProcessorDBConfig.dsl(),
                    moveSucceedRecordExpiredSeconds,
                    inProgressActionExpiredSeconds,
                    activateSucceedRecordExpiredSeconds,
                    scaleSucceedRecordExpiredSeconds,
                    resizeSucceedRecordExpiredSeconds);
        }
    }

    /**
     * The DAO used for entity maintenance.
     *
     * @return the DAO used for entity maintenance.
     */
    @Bean
    public EntityMaintenanceTimeDao entityMaintenanceTimeDao() {
        return new EntityMaintenanceTimeDao(topologyProcessorDBConfig.dsl(), drsMaintenanceProtectionWindow,
            Clock.systemUTC(), accountForVendorAutomation);
    }

    /**
     * The manager for applying the controllable flag.
     *
     * @return the manager for determining the controllable flag.
     */
    @Bean
    public ControllableManager controllableManager() {
        return new ControllableManager(entityActionDao(), entityMaintenanceTimeDao(), accountForVendorAutomation);
    }

    /**
     * The service for getting affected entities info.
     *
     * @return the service for getting affected entities info.
     */
    @Bean
    public AffectedEntitiesServiceBlockingStub affectedEntitiesServiceBlockingStub() {
        return AffectedEntitiesServiceGrpc.newBlockingStub(
                aoClientConfig.actionOrchestratorChannel());
    }
}
