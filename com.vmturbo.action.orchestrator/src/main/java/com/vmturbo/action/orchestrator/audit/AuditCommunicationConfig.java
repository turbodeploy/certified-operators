package com.vmturbo.action.orchestrator.audit;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;

import com.vmturbo.action.orchestrator.api.impl.ActionOrchestratorClientConfig;
import com.vmturbo.action.orchestrator.dto.ActionMessages.ActionEvent;
import com.vmturbo.action.orchestrator.topology.TopologyProcessorConfig;
import com.vmturbo.action.orchestrator.workflow.config.WorkflowConfig;
import com.vmturbo.components.api.server.BaseKafkaProducerConfig;
import com.vmturbo.components.api.server.IMessageSender;
import com.vmturbo.topology.processor.api.util.ThinTargetCache;

/**
 * Spring configuration to perform external audit functionality.
 */
@Import({
        BaseKafkaProducerConfig.class,
        WorkflowConfig.class,
        TopologyProcessorConfig.class
})
public class AuditCommunicationConfig {
    @Autowired
    private BaseKafkaProducerConfig kafkaProducerConfig;
    @Autowired
    private WorkflowConfig workflowConfig;

    @Autowired
    private TopologyProcessorConfig tpConfig;

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
        return new ActionAuditSender(workflowConfig.workflowStore(), auditMessageSender(), thinTargetCache());
    }

    /**
     * Thin target cache.
     *
     * @return the bean created
     */
    @Bean
    public ThinTargetCache thinTargetCache() {
        return new ThinTargetCache(tpConfig.topologyProcessor());
    }
}
