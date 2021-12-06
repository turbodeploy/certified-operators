package com.vmturbo.plan.orchestrator.plan.export;

import java.sql.SQLException;

import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.common.protobuf.plan.PlanExportDTO.PlanExportNotification;
import com.vmturbo.common.protobuf.topology.PlanExportToTargetServiceGrpc;
import com.vmturbo.common.protobuf.topology.PlanExportToTargetServiceGrpc.PlanExportToTargetServiceBlockingStub;
import com.vmturbo.components.api.server.BaseKafkaProducerConfig;
import com.vmturbo.components.api.server.IMessageSender;
import com.vmturbo.plan.orchestrator.DbAccessConfig;
import com.vmturbo.plan.orchestrator.GlobalConfig;
import com.vmturbo.plan.orchestrator.api.impl.PlanOrchestratorClientImpl;
import com.vmturbo.plan.orchestrator.plan.PlanConfig;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.topology.processor.api.impl.TopologyProcessorClientConfig;

/**
 * Spring configuration related to plan destinations.
 */
@Configuration
@Import({DbAccessConfig.class, GlobalConfig.class,
    PlanConfig.class, BaseKafkaProducerConfig.class, TopologyProcessorClientConfig.class})
public class PlanExportConfig {
    @Autowired
    private DbAccessConfig databaseConfig;

    @Autowired
    private GlobalConfig globalConfig;

    @Autowired
    private PlanConfig planConfig;

    @Autowired
    private BaseKafkaProducerConfig kafkaProducerConfig;

    @Autowired
    private TopologyProcessorClientConfig tpClientConfig;

    /**
     * Returns the instance implementing how plan destinations are persisted and retrieved from storage.
     *
     * @return the instance implementing how plans destinations are persisted and retrieved from storage.
     */
    @Bean
    public PlanDestinationDao planDestinationDao() {
        try {
            return new PlanDestinationDaoImpl(databaseConfig.dsl(), globalConfig.identityInitializer());

        } catch (SQLException | UnsupportedDialectException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new BeanCreationException("Failed to create PlanDestinationDao", e);
        }
    }

    /**
     * Returns the external service for creating, updating, and running plans.
     *
     * @return the external service for creating, updating, and running plans.
     */
    @Bean
    public PlanExportRpcService planExportService() {
        PlanExportRpcService planExportRpcService = new PlanExportRpcService(planDestinationDao(),
            planConfig.planDao(), planExportToTargetServiceBlockingStub(),
            planExportNotificationSender());

        globalConfig.tpNotificationClient().addPlanExportToTargetListener(planExportRpcService);

        return planExportRpcService;
    }

    /**
     * Returns a Kafka message sender for plan export notifications.
     *
     * @return a Kafka message sender for plan export notifications.
     */
    @Bean
    public IMessageSender<PlanExportNotification> exportNotificationSender() {
        return kafkaProducerConfig.kafkaMessageSender()
            .messageSender(PlanOrchestratorClientImpl.EXPORT_STATUS_TOPIC);
    }

    /**
     * Returns a notification sender for plan export notifications.
     *
     * @return a notification sender for plan export notifications.
     */
    @Bean
    public PlanExportNotificationSender planExportNotificationSender() {
        return new PlanExportNotificationSender(exportNotificationSender());
    }

    /**
     * Returns the Topology Processor async plan export service client.
     *
     * @return the async client
     */
    @Bean
    public PlanExportToTargetServiceBlockingStub planExportToTargetServiceBlockingStub() {
        return PlanExportToTargetServiceGrpc.newBlockingStub(
            tpClientConfig.topologyProcessorChannel());
    }
}
