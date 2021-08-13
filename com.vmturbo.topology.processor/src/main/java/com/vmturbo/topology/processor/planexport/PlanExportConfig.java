package com.vmturbo.topology.processor.planexport;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.topology.processor.actions.ActionsConfig;
import com.vmturbo.topology.processor.api.server.TopologyProcessorNotificationSender;
import com.vmturbo.topology.processor.operation.OperationConfig;
import com.vmturbo.topology.processor.repository.RepositoryConfig;

/**
 * Spring configuration for services related to Plan Export handling.
 **/
@Configuration
@Import({ActionsConfig.class, OperationConfig.class, RepositoryConfig.class})
public class PlanExportConfig {
    @Autowired
    private ActionsConfig actionsConfig;

    @Autowired
    private OperationConfig operationConfig;

    @Autowired
    private RepositoryConfig repositoryConfig;

    @Autowired
    private TopologyProcessorNotificationSender sender;

    /**
     * Returns the external service for creating, updating, and running plans.
     *
     * @return the external service for creating, updating, and running plans.
     */
    @Bean
    public PlanExportToTargetRpcService planExportToTargetService() {
        return new PlanExportToTargetRpcService(
            operationConfig.operationManager(),
            sender,
            actionsConfig.topologyToSdkEntityConverter(),
            repositoryConfig.repository(),
            actionsConfig.actionsServiceBlockingStub(),
            startExportThreadPool(),
            new PlanExportDumper());
    }

    /**
     * Returns a thread pool on which to run plan export jobs.
     *
     * @return a thread pool on which to run plan export jobs
     */
    @Bean(destroyMethod = "shutdownNow")
    public ExecutorService startExportThreadPool() {
        final ThreadFactory threadFactory = new ThreadFactoryBuilder()
            .setNameFormat("plan-export-runner-%d")
            .build();
        return Executors.newSingleThreadExecutor(threadFactory);
    }
}
