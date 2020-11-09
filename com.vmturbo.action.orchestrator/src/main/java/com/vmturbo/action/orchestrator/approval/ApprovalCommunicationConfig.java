package com.vmturbo.action.orchestrator.approval;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.action.orchestrator.api.impl.ActionOrchestratorClientConfig;
import com.vmturbo.action.orchestrator.dto.ActionMessages.ActionApprovalRequests;
import com.vmturbo.action.orchestrator.execution.ActionExecutionConfig;
import com.vmturbo.action.orchestrator.store.ActionStoreConfig;
import com.vmturbo.action.orchestrator.translation.ActionTranslationConfig;
import com.vmturbo.action.orchestrator.workflow.config.WorkflowConfig;
import com.vmturbo.components.api.server.BaseKafkaProducerConfig;
import com.vmturbo.components.api.server.IMessageSender;
import com.vmturbo.platform.sdk.common.MediationMessage.ActionResponse;

/**
 * Spring configuration for external action approval requestint.
 */
@Configuration
@Import({BaseKafkaProducerConfig.class, WorkflowConfig.class, ActionExecutionConfig.class})
public class ApprovalCommunicationConfig {

    @Autowired
    private BaseKafkaProducerConfig kafkaProducerConfig;

    @Autowired
    private WorkflowConfig workflowConfig;

    @Autowired
    private ActionTranslationConfig actionTranslationConfig;

    @Autowired
    private ActionExecutionConfig actionExecutionConfig;

    @Autowired
    private ActionStoreConfig actionStoreConfig;

    /**
     * A message sender to notify about new actions ready for approval.
     *
     * @return message sender
     */
    @Bean
    public IMessageSender<ActionApprovalRequests> approvalRequestsSender() {
        return kafkaProducerConfig.kafkaMessageSender()
                .messageSender(ActionOrchestratorClientConfig.ACTION_APPROVAL_REQUEST_TOPIC);
    }

    /**
     * A message sender to notify about action state updates - to notify external approval backend
     * on internal action state changes.
     *
     * @return message sender
     */
    @Bean
    public IMessageSender<ActionResponse> actionStateUpdatesSender() {
        return kafkaProducerConfig.kafkaMessageSender()
                .messageSender(ActionOrchestratorClientConfig.ACTION_UPDATE_STATE_REQUESTS_TOPIC);
    }

    /**
     * Action approval requester.
     *
     * @return requester
     */
    @Bean
    public ActionApprovalSender approvalRequester() {
        return new ActionApprovalSender(workflowConfig.workflowStore(), approvalRequestsSender(),
                actionExecutionConfig.actionTargetSelector(),
                actionStoreConfig.entitySettingsCache());
    }
}
