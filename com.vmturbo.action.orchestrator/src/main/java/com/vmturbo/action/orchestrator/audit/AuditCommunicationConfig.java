package com.vmturbo.action.orchestrator.audit;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.action.orchestrator.ActionOrchestratorDBConfig;
import com.vmturbo.action.orchestrator.ActionOrchestratorGlobalConfig;
import com.vmturbo.action.orchestrator.action.AuditActionsPersistenceManager;
import com.vmturbo.action.orchestrator.action.AuditActionsStore;
import com.vmturbo.action.orchestrator.action.AuditedActionsManager;
import com.vmturbo.action.orchestrator.api.impl.ActionOrchestratorClientConfig;
import com.vmturbo.action.orchestrator.dto.ActionMessages.ActionEvent;
import com.vmturbo.action.orchestrator.topology.TopologyProcessorConfig;
import com.vmturbo.action.orchestrator.translation.ActionTranslationConfig;
import com.vmturbo.action.orchestrator.workflow.config.WorkflowConfig;
import com.vmturbo.components.api.server.BaseKafkaProducerConfig;
import com.vmturbo.components.api.server.IMessageSender;

/**
 * Spring configuration to perform external audit functionality.
 */
@Configuration
@Import({
        BaseKafkaProducerConfig.class,
        WorkflowConfig.class,
        TopologyProcessorConfig.class,
        ActionTranslationConfig.class,
        ActionOrchestratorGlobalConfig.class
})
public class AuditCommunicationConfig {
    @Autowired
    private BaseKafkaProducerConfig kafkaProducerConfig;
    @Autowired
    private WorkflowConfig workflowConfig;
    @Autowired
    private ActionTranslationConfig actionTranslationConfig;
    @Autowired
    private TopologyProcessorConfig tpConfig;
    @Autowired
    private ActionOrchestratorDBConfig databaseConfig;
    @Autowired
    private ActionOrchestratorGlobalConfig actionOrchestratorGlobalConfig;

    /**
     * Criteria for CLEARED audit event for action. If action is not recommended more then
     * minsClearedActionsCriteria, then we send CLEARED event. By default it's 24 hours,
     * just like in the ServiceNOW App.
     */
    @Value("${minsClearedActionsCriteria:1440}")
    private long minsClearedActionsCriteria;

    /**
     * In {@link AuditedActionsManager} we sync up in_memory and DB bookkeeping cache in
     * background thread. Do it as soon as new actions appear for audit.
     * This property configures interval after which we try to sync up again if we
     * had db's issues.
     */
    @Value("${minsRecoveryInterval:1}")
    private long minsRecoveryInterval;


    /**
     * Notifications sender to send action audit events.
     *
     * @return the bean created
     */
    @Bean
    public IMessageSender<ActionEvent> auditMessageSender() {
        return kafkaProducerConfig.kafkaMessageSender().messageSender(
                ActionOrchestratorClientConfig.ACTION_AUDIT_TOPIC);
    }

    /**
     * Action audit sender.
     *
     * @return the bean created
     */
    @Bean
    public ActionAuditSender actionAuditSender() {
        return new ActionAuditSender(workflowConfig.workflowStore(), auditMessageSender(),
                tpConfig.thinTargetCache(), actionTranslationConfig.actionTranslator(),
                auditedActionsManager(), minsClearedActionsCriteria,
                actionOrchestratorGlobalConfig.actionOrchestratorClock());
    }

    /**
     * Creates manager for working with audited actions.
     *
     * @return instance of {@link AuditedActionsManager}
     */
    @Bean
    public AuditedActionsManager auditedActionsManager() {
        return new AuditedActionsManager(auditedActionsStore(), auditedActionsSyncUpPool(),
                minsRecoveryInterval);
    }

    /**
     * Thread pool shared by auditing services.
     *
     * @return the thread pool shared by auditing services.
     */
    @Bean(destroyMethod = "shutdownNow")
    public ScheduledExecutorService auditedActionsSyncUpPool() {
        final ThreadFactory threadFactory =
                new ThreadFactoryBuilder().setNameFormat("audited-actions-thread-%d").build();
        return Executors.newScheduledThreadPool(1, threadFactory);
    }

    /**
     * Creates DAO implementation for working with audited actions.
     *
     * @return instance of {@link AuditActionsPersistenceManager}
     */
    @Bean
    public AuditActionsPersistenceManager auditedActionsStore() {
        return new AuditActionsStore(databaseConfig.dsl());
    }

    /**
     * Bean to handle diagnostics import/export for worfklows.
     *
     * @return the bean created
     */
    @Bean
    public AuditActionsPersistenceDiagnostics auditActionsPersistenceDiagnostics() {
        return new AuditActionsPersistenceDiagnostics(auditedActionsStore());
    }
}
