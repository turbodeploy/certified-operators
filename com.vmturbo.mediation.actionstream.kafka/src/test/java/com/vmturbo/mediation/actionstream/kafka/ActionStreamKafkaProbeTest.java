package com.vmturbo.mediation.actionstream.kafka;

import java.time.Instant;
import java.util.Collections;

import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;

import com.vmturbo.platform.common.dto.ActionExecution.ActionEventDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionExecutionDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO.ActionType;
import com.vmturbo.platform.common.dto.ActionExecution.ActionResponseState;
import com.vmturbo.platform.common.dto.ActionExecution.Workflow.ActionScriptPhase;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse;
import com.vmturbo.platform.common.dto.Discovery.ErrorDTO;
import com.vmturbo.platform.common.dto.Discovery.ErrorDTO.ErrorSeverity;
import com.vmturbo.platform.common.dto.Discovery.ValidationResponse;

/**
 * Tests the ActionStreamKafkaProbe.
 */
public class ActionStreamKafkaProbeTest {

    private static final String KAFKA_ADDRESS = "kafkaTarget";
    private static final String KAFKA_PORT = "9092";
    private static final String AUDIT_TOPIC = "onGenAudit";

    private ActionStreamKafkaProbeAccount account;

    @Mock
    private ActionStreamKafkaProducer kafkaProducer;

    @Mock
    private ActionStreamKafkaTopicChecker kafkaTopicChecker;

    @Spy
    private ActionStreamKafkaProbe actionStreamKafkaProbe;

    /**
     * Initialize test configuration.
     */
    @Before
    public void init() {
        MockitoAnnotations.initMocks(this);
        account = new ActionStreamKafkaProbeAccount(KAFKA_ADDRESS, KAFKA_PORT, AUDIT_TOPIC);
        Mockito.doReturn(kafkaProducer).when(actionStreamKafkaProbe).getKafkaProducer(account);
        Mockito.doReturn(kafkaTopicChecker)
                .when(actionStreamKafkaProbe)
                .getKafkaTopicChecker(account);
    }

    /**
     * Discovery should return the ON_GENERATION workflow.
     */
    @Test
    public void testDiscovery() {
        // succeeded validation
        Mockito.doReturn(ValidationResponse.newBuilder().build())
                .when(actionStreamKafkaProbe)
                .validateTarget(account);

        final DiscoveryResponse response = actionStreamKafkaProbe.discoverTarget(account);

        Assert.assertEquals(1, response.getWorkflowList().size());
        Assert.assertEquals(ActionScriptPhase.ON_GENERATION,
                response.getWorkflowList().get(0).getPhase());
    }

    /**
     * If validation fails, then discovery should also fail.
     */
    @Test
    public void testFailingValidationFailsDiscovery() {
        // failed validation
        Mockito.doReturn(ValidationResponse.newBuilder()
                .addErrorDTO(ErrorDTO.newBuilder()
                        .setDescription("something bad happened")
                        .setSeverity(ErrorSeverity.CRITICAL)
                        .build())
                .build()).when(actionStreamKafkaProbe).validateTarget(account);

        final DiscoveryResponse response = actionStreamKafkaProbe.discoverTarget(account);

        Assert.assertEquals(0, response.getWorkflowList().size());
        Assert.assertEquals(1, response.getErrorDTOList().size());
    }

    /**
     * Test sending audit events to external kafka.
     */
    @Test
    public void testAudit() {
        Mockito.when(kafkaTopicChecker.isTopicAvailable(AUDIT_TOPIC)).thenReturn(true);

        final ActionEventDTO actionEvent = ActionEventDTO.newBuilder()
                .setNewState(ActionResponseState.PENDING_ACCEPT)
                .setOldState(ActionResponseState.PENDING_ACCEPT)
                .setTimestamp(Instant.now().getEpochSecond())
                .setAction(ActionExecutionDTO.newBuilder()
                        .setActionOid(1L)
                        .setActionType(ActionType.MOVE)
                        .build())
                .build();
        actionStreamKafkaProbe.auditActions(account, Collections.singletonList(actionEvent));
        Mockito.verify(kafkaProducer, Mockito.times(1)).sendMessage(actionEvent, AUDIT_TOPIC);
    }

    /**
     * Tests failed validation if audit topic from account values doesn't exist on kafka cluster.
     */
    @Test
    public void testFailedValidation() {
        Mockito.when(kafkaTopicChecker.isTopicAvailable(AUDIT_TOPIC)).thenReturn(false);

        final ValidationResponse validationResponse =
                actionStreamKafkaProbe.validateTarget(account);
        Assert.assertEquals(1, validationResponse.getErrorDTOList().size());
        final ErrorDTO errorDTO = validationResponse.getErrorDTOList().get(0);
        Assert.assertThat(errorDTO.getDescription(),
                CoreMatchers.containsString("Topic " + AUDIT_TOPIC + " is not available on"));
    }

    /**
     * Tests success validation when audit topic from account values exist on kafka cluster.
     */
    @Test
    public void testSuccessValidation() {
        Mockito.when(kafkaTopicChecker.isTopicAvailable(AUDIT_TOPIC)).thenReturn(true);

        final ValidationResponse validationResponse =
                actionStreamKafkaProbe.validateTarget(account);
        Assert.assertTrue(validationResponse.getErrorDTOList().isEmpty());
    }
}
