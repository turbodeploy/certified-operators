package com.vmturbo.mediation.actionstream.kafka;

import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;

import com.vmturbo.platform.common.dto.ActionExecution.ActionErrorDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionEventDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionExecutionDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO.ActionType;
import com.vmturbo.platform.common.dto.ActionExecution.ActionResponseState;
import com.vmturbo.platform.common.dto.ActionExecution.Workflow.ActionScriptPhase;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse;
import com.vmturbo.platform.common.dto.Discovery.ErrorDTO;
import com.vmturbo.platform.common.dto.Discovery.ErrorDTO.ErrorSeverity;
import com.vmturbo.platform.common.dto.Discovery.ValidationResponse;
import com.vmturbo.platform.sdk.probe.TargetOperationException;

/**
 * Tests the ActionStreamKafkaProbe.
 */
public class ActionStreamKafkaProbeTest {

    private static final String KAFKA_ADDRESS = "kafkaTarget";
    private static final String KAFKA_PORT = "9092";
    private static final String AUDIT_TOPIC = "onGenAudit";
    private static final ActionExecutionDTO MOVE_ACTION = ActionExecutionDTO.newBuilder()
        .setActionOid(1L)
        .setActionType(ActionType.MOVE)
        .addActionItem(ActionItemDTO.newBuilder()
            .setActionType(ActionType.MOVE)
            .setUuid("1")
            .setTargetSE(EntityDTO.newBuilder()
                .setId("2")
                .setEntityType(EntityType.VIRTUAL_MACHINE)
                .build())
            .build())
        .build();
    private static final ActionEventDTO MOVE_ACTION_EVENT = ActionEventDTO.newBuilder()
        .setNewState(ActionResponseState.PENDING_ACCEPT)
        .setOldState(ActionResponseState.PENDING_ACCEPT)
        .setTimestamp(Instant.now().getEpochSecond())
        .setAction(MOVE_ACTION)
        .build();

    private static final ActionEventDTO CLEARED_ACTION = ActionEventDTO.newBuilder()
        .setAction(ActionExecutionDTO.newBuilder()
            .setActionType(ActionType.NONE)
            .setActionOid(636763995018096L)
            .build())
        .setOldState(ActionResponseState.PENDING_ACCEPT)
        .setNewState(ActionResponseState.CLEARED)
        .setTimestamp(1611698951378L)
        .build();


    private ActionStreamKafkaProbeAccount account;

    @Mock
    private ActionStreamKafkaProducer kafkaProducer;

    @Mock
    private ActionStreamKafkaTopicChecker kafkaTopicChecker;

    @Captor
    private ArgumentCaptor<String> stringCaptor;

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
        Mockito.when(kafkaTopicChecker.isTopicAvailable(AUDIT_TOPIC)).thenReturn(true);
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

        Assert.assertEquals(2, response.getWorkflowList().size());
        Assert.assertEquals(ActionScriptPhase.ON_GENERATION,
            response.getWorkflowList().get(0).getPhase());
        Assert.assertEquals(ActionScriptPhase.AFTER_EXECUTION,
            response.getWorkflowList().get(1).getPhase());
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
     *
     * @throws InterruptedException if thread is interrupted
     * @throws ExecutionException it shouldn't be thrown
     * @throws TargetOperationException it shouldn't be thrown
     */
    @Test
    public void testAudit() throws InterruptedException, ExecutionException, TargetOperationException {
        final Future<RecordMetadata> auditActionFuture = Mockito.mock(Future.class);
        final RecordMetadata fakeRecordMetadata =
                new RecordMetadata(new TopicPartition(AUDIT_TOPIC, 1), 1, 1, 1, 1L, 1, 1);
        Mockito.when(auditActionFuture.get()).thenReturn(fakeRecordMetadata);
        Mockito.when(kafkaProducer.sendMessage(ArgumentMatchers.anyString(), ArgumentMatchers.eq(AUDIT_TOPIC))).thenReturn(auditActionFuture);
        final Collection<ActionErrorDTO> actionErrorDTOS = actionStreamKafkaProbe.auditActions(account,
                Collections.singletonList(MOVE_ACTION_EVENT));
        Assert.assertEquals(0, actionErrorDTOS.size());
    }

    /**
     * Test failed audit when audited action wasn't receive by external kafka.
     *
     * @throws InterruptedException if thread is interrupted
     * @throws ExecutionException it shouldn't be thrown
     * @throws TargetOperationException it shouldn't be thrown
     */
    @Test
    public void testFailedAudit() throws InterruptedException, ExecutionException, TargetOperationException {
        final Future<RecordMetadata> auditActionFuture = Mockito.mock(Future.class);
        Mockito.when(auditActionFuture.get())
                .thenThrow(new ExecutionException(new RuntimeException()));
        Mockito.when(kafkaProducer.sendMessage(ArgumentMatchers.anyString(), ArgumentMatchers.eq(AUDIT_TOPIC))).thenReturn(auditActionFuture);
        final Collection<ActionErrorDTO> actionErrorDTOS = actionStreamKafkaProbe.auditActions(account,
                Collections.singletonList(MOVE_ACTION_EVENT));
        Assert.assertEquals(1, actionErrorDTOS.size());
        final ActionErrorDTO actionErrorDTO = actionErrorDTOS.iterator().next();
        Assert.assertEquals("Failed to send action to external kafka.", actionErrorDTO.getMessage());
    }

    /**
     * Cleared message missing all the details should fill in an ActionApiDTO with less information
     * instead of failing.
     *
     * @throws InterruptedException should not be thrown.
     * @throws ExecutionException should not be thrown.
     * @throws TargetOperationException should not be thrown.
     */
    @Test
    public void testClearedMessage() throws InterruptedException, ExecutionException, TargetOperationException {
        final Future<RecordMetadata> auditActionFuture = Mockito.mock(Future.class);
        final RecordMetadata fakeRecordMetadata =
            new RecordMetadata(new TopicPartition(AUDIT_TOPIC, 1), 1, 1, 1, 1L, 1, 1);
        Mockito.when(auditActionFuture.get()).thenReturn(fakeRecordMetadata);
        Mockito.when(kafkaProducer.sendMessage(stringCaptor.capture(), ArgumentMatchers.eq(AUDIT_TOPIC))).thenReturn(auditActionFuture);
        final Collection<ActionErrorDTO> actionErrorDTOS = actionStreamKafkaProbe.auditActions(account,
            Collections.singletonList(CLEARED_ACTION));
        Assert.assertEquals(0, actionErrorDTOS.size());
        String actual = stringCaptor.getValue();
        Assert.assertTrue(actual.contains("\"uuid\":\"636763995018096\""));
        Assert.assertTrue(actual.contains("\"actionImpactID\":636763995018096"));
        Assert.assertTrue(actual.contains("\"actionState\":\"CLEARED\""));
        Assert.assertTrue(actual.contains("\"actionID\":636763995018096"));
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
        final ValidationResponse validationResponse =
                actionStreamKafkaProbe.validateTarget(account);
        Assert.assertTrue(validationResponse.getErrorDTOList().isEmpty());
    }
}
