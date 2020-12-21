package com.vmturbo.topology.processor.api;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;

import io.opentracing.SpanContext;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.action.ActionNotificationDTO.ActionFailure;
import com.vmturbo.common.protobuf.action.ActionNotificationDTO.ActionProgress;
import com.vmturbo.common.protobuf.action.ActionNotificationDTO.ActionSuccess;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.communication.ITransport;
import com.vmturbo.communication.LoggingUncaughtExceptionHandler;
import com.vmturbo.communication.chunking.RemoteIterator;
import com.vmturbo.components.api.chunking.OversizedElementException;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO.ActionType;
import com.vmturbo.platform.common.dto.ActionExecution.ActionResponseState;
import com.vmturbo.platform.common.dto.Discovery.ErrorDTO;
import com.vmturbo.platform.common.dto.Discovery.ErrorDTO.ErrorSeverity;
import com.vmturbo.platform.sdk.common.MediationMessage;
import com.vmturbo.platform.sdk.common.MediationMessage.ActionResponse;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationClientMessage;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationServerMessage;
import com.vmturbo.topology.processor.api.server.TopologyBroadcast;
import com.vmturbo.topology.processor.api.server.TopologyProcessorNotificationSender;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.operation.Operation;
import com.vmturbo.topology.processor.operation.action.Action;
import com.vmturbo.topology.processor.operation.discovery.Discovery;
import com.vmturbo.topology.processor.operation.validation.Validation;
import com.vmturbo.topology.processor.probes.ProbeException;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.scheduling.Scheduler;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetStore;
import com.vmturbo.topology.processor.topology.TopologyHandler;
import com.vmturbo.topology.processor.util.Probes;

/**
 * Tests for TopologyProcessor API server-side calls (notifications).
 */
public class NotificationsApiTest extends AbstractApiCallsTest {

    private TargetStore targetStore;
    private ProbeStore probeStore;

    private final long actionId = 7;

    @Rule
    public TemporaryFolder testFolder = new TemporaryFolder();

    @BeforeClass
    public static void initStatic() {
        Thread.setDefaultUncaughtExceptionHandler(new LoggingUncaughtExceptionHandler());
    }

    @Before
    public void initStores() throws Exception {
        System.setProperty("com.vmturbo.keydir", testFolder.newFolder().getAbsolutePath());
        targetStore = integrationTestServer.getBean(TargetStore.class);
        probeStore = integrationTestServer.getBean(ProbeStore.class);
    }

    /**
     * Tests reporting entities.
     *
     * @throws Exception in exceptions occur
     */
    @Test
    @Ignore
    public void testTopologyNotification() throws Exception {
        final TopologyEntityDTO topology1 =
                        TopologyEntityDTO.newBuilder().setOid(1L).setEntityType(1).build();
        final long topologyContextId = 7000;
        final TopologyEntityDTO topology2 =
                        TopologyEntityDTO.newBuilder().setOid(2L).setEntityType(2).build();

        final TopologyAccumulator listener1 = Mockito.spy(new TopologyAccumulator(2));
        final TopologyAccumulator listener2 = Mockito.spy(new TopologyAccumulator(1));
        getTopologyProcessor().addLiveTopologyListener(listener1);
        final List<TopologyEntityDTO> entities = ImmutableList.of(topology1, topology2);

        final ArgumentCaptor<TopologyInfo> topologyInfoCaptor1 = ArgumentCaptor.forClass(TopologyInfo.class);

        final long topologyOneId = 7;
        final long topologyTwoId = 8;

        sendEntities(topologyContextId, topologyOneId, entities);

        getTopologyProcessor().addLiveTopologyListener(listener2);
        @SuppressWarnings({ "unchecked", "rawtypes" })
        final ArgumentCaptor<TopologyInfo> topologyInfoCaptor2 = ArgumentCaptor.forClass(TopologyInfo.class);

        sendEntities(topologyContextId, topologyTwoId, entities);
        Mockito.verify(listener1, Mockito.timeout(TIMEOUT_MS).times(2))
                        .onTopologyNotification(topologyInfoCaptor1.capture(), Mockito.any(), Mockito.any());
        Assert.assertEquals(topologyTwoId, topologyInfoCaptor1.getValue().getTopologyId());
        Assert.assertEquals(topologyContextId, topologyInfoCaptor1.getValue().getTopologyContextId());
        Mockito.verify(listener2, Mockito.timeout(TIMEOUT_MS).times(1))
                        .onTopologyNotification(topologyInfoCaptor2.capture(), Mockito.any(), Mockito.any());
        listener1.await();
        listener2.await();
        assertThat(listener1.result, containsInAnyOrder(entities.toArray()));
        assertThat(listener2.result, containsInAnyOrder(entities.toArray()));

        Assert.assertEquals(topologyTwoId, topologyInfoCaptor2.getValue().getTopologyId());
        Assert.assertEquals(topologyContextId, topologyInfoCaptor2.getValue().getTopologyContextId());
    }

    /**
     * Tests reporting entities, when one of the listeners failed on accepting entities.
     *
     * @throws Exception in exceptions occur
     */
    @Test
    public void testFailedListeners() throws Exception {
        final TopologyEntityDTO topology =
                        TopologyEntityDTO.newBuilder().setOid(1L).setEntityType(2).build();

        final TopologyAccumulator goodListener = Mockito.spy(new TopologyAccumulator(2));
        final EntitiesListener failingListener = Mockito.mock(EntitiesListener.class);
        Mockito.doThrow(new RuntimeException("Exception for tests")).when(failingListener)
                        .onTopologyNotification(Mockito.any(TopologyInfo.class), Mockito.any(), Mockito.any());

        getTopologyProcessor().addLiveTopologyListener(failingListener);
        getTopologyProcessor().addLiveTopologyListener(goodListener);
        final List<TopologyEntityDTO> entities = Collections.singletonList(topology);

        sendEntities(0L, 0L, entities);
        sendEntities(0L, 1L, entities);

        Mockito.verify(failingListener, Mockito.timeout(TIMEOUT_MS).times(2))
                .onTopologyNotification(Mockito.any(TopologyInfo.class), Mockito.any(), Mockito.any());
        Mockito.verify(goodListener, Mockito.timeout(TIMEOUT_MS).times(2))
                .onTopologyNotification(Mockito.any(TopologyInfo.class), Mockito.any(), Mockito.any());
        goodListener.await();
        assertThat(goodListener.result, containsInAnyOrder(entities.toArray()));
    }

    /**
     * Tests reporting target addition.
     *
     * @throws Exception in exceptions occur
     */
    @Test
    public void testAddTargetNotification() throws Exception {
        final TargetListener listener = Mockito.mock(TargetListener.class);
        getTopologyProcessor().addTargetListener(listener);
        final long probeId = createProbe();
        final Target target = createTarget(probeId, "1");

        final ArgumentCaptor<TargetInfo> targetCaptor = ArgumentCaptor.forClass(TargetInfo.class);
        Mockito.verify(listener, Mockito.timeout(TIMEOUT_MS).times(1))
                        .onTargetAdded(targetCaptor.capture());
        final TargetInfo result = targetCaptor.getValue();
        Assert.assertEquals(target.getId(), result.getId());
    }

    /**
     * Tests reporting target change.
     *
     * @throws Exception in exceptions occur
     */
    @Test
    public void testUpdateTargetNotification() throws Exception {
        final long probeId = createProbe();
        final Target target = createTarget(probeId, "1");

        final TargetListener listener = Mockito.mock(TargetListener.class);
        getTopologyProcessor().addTargetListener(listener);

        targetStore.updateTarget(target.getId(),
                        createTargetSpec(probeId, "2").getAccountValueList(), Optional.empty());

        final ArgumentCaptor<TargetInfo> targetCaptor = ArgumentCaptor.forClass(TargetInfo.class);
        Mockito.verify(listener, Mockito.timeout(TIMEOUT_MS).times(1))
                        .onTargetChanged(targetCaptor.capture());
        final TargetInfo result = targetCaptor.getValue();
        Assert.assertEquals("2", result.getAccountData().iterator().next().getStringValue());
        Assert.assertEquals(target.getId(), result.getId());
    }

    /**
     * Tests reporting target change.
     *
     * @throws Exception in exceptions occur
     */
    @Test
    public void testRemovedTargetNotification() throws Exception {
        final long probeId = createProbe();
        final Target target = createTarget(probeId, "1");
        final TopologyHandler topologyHandler = Mockito.mock(TopologyHandler.class);
        final Scheduler scheduler = Mockito.mock(Scheduler.class);
        final TargetListener listener = Mockito.mock(TargetListener.class);
        getTopologyProcessor().addTargetListener(listener);
        targetStore.removeTargetAndBroadcastTopology(target.getId(), topologyHandler, scheduler);
        final ArgumentCaptor<Long> targetCaptor = ArgumentCaptor.forClass(Long.class);
        Mockito.verify(listener, Mockito.timeout(TIMEOUT_MS).times(1))
                        .onTargetRemoved(targetCaptor.capture());
        final long result = targetCaptor.getValue();
        Assert.assertEquals(target.getId(), result);
    }

    /**
     * Tests reporting target validation status before completion.
     * The difference in setting up this test and {@link #testTargetValidatedNotification()} is
     * that here we don't set the operation status to success before sending it.
     *
     * @throws Exception if exceptions occur
     */
    @Test
    public void testTargetValidatedStartNotification() throws Exception {
        final long probeId = createProbe();
        final Target target = createTarget(probeId, "1");
        final TargetListener listener = Mockito.mock(TargetListener.class);
        getTopologyProcessor().addTargetListener(listener);
        final Validation vldResult = new Validation(probeId, target.getId(),
                        integrationTestServer.getBean(IdentityProvider.class));

        integrationTestServer.getBean(TopologyProcessorNotificationSender.class).notifyOperationState(vldResult);

        final ArgumentCaptor<ValidationStatus> targetCaptor =
                        ArgumentCaptor.forClass(ValidationStatus.class);
        Mockito.verify(listener, Mockito.timeout(TIMEOUT_MS).times(1))
                        .onTargetValidated(targetCaptor.capture());

        final ValidationStatus result = targetCaptor.getValue();
        Assert.assertEquals(target.getId(), result.getTargetId());
        Assert.assertEquals(toEpochMillis(vldResult.getStartTime()), result.getStartTime().getTime());
        Assert.assertNull(result.getCompletionTime());
        Assert.assertFalse(result.isCompleted());
        Assert.assertFalse(result.isSuccessful());
    }

    /**
     * Tests reporting target validation succeeded result.
     *
     * @throws Exception in exceptions occur
     */
    @Test
    public void testTargetValidatedNotification() throws Exception {
        final long probeId = createProbe();
        final Target target = createTarget(probeId, "1");
        final TargetListener listener = Mockito.mock(TargetListener.class);
        getTopologyProcessor().addTargetListener(listener);
        final Validation vldResult = new Validation(probeId, target.getId(),
                        integrationTestServer.getBean(IdentityProvider.class));
        vldResult.success();

        integrationTestServer.getBean(TopologyProcessorNotificationSender.class).notifyOperationState(vldResult);

        final ArgumentCaptor<ValidationStatus> targetCaptor =
                        ArgumentCaptor.forClass(ValidationStatus.class);
        Mockito.verify(listener, Mockito.timeout(TIMEOUT_MS).times(1))
                        .onTargetValidated(targetCaptor.capture());
        final ValidationStatus result = targetCaptor.getValue();
        assertOperationResult(target.getId(), vldResult, result);
        Assert.assertTrue(result.isSuccessful());
    }

    /**
     * Tests reporting target validation failed result.
     *
     * @throws Exception in exceptions occur
     */
    @Test
    public void testTargetValidationErrorNotification() throws Exception {
        final long probeId = createProbe();
        final Target target = createTarget(probeId, "1");
        final TargetListener listener = Mockito.mock(TargetListener.class);
        getTopologyProcessor().addTargetListener(listener);
        final Validation vldResult = new Validation(probeId, target.getId(),
                        integrationTestServer.getBean(IdentityProvider.class));
        vldResult.addError(ErrorDTO.newBuilder().setDescription("error1")
                        .setSeverity(ErrorSeverity.CRITICAL).build());
        vldResult.addError(ErrorDTO.newBuilder().setDescription("error2")
                        .setSeverity(ErrorSeverity.WARNING).build());
        vldResult.fail();

        integrationTestServer.getBean(TopologyProcessorNotificationSender.class).notifyOperationState(vldResult);

        final ArgumentCaptor<ValidationStatus> targetCaptor =
                        ArgumentCaptor.forClass(ValidationStatus.class);
        Mockito.verify(listener, Mockito.timeout(TIMEOUT_MS).times(1))
                        .onTargetValidated(targetCaptor.capture());
        final ValidationStatus result = targetCaptor.getValue();
        assertOperationResult(target.getId(), vldResult, result);
        Assert.assertFalse(result.isSuccessful());
    }

    /**
     * Tests reporting target discovery succeeded result.
     *
     * @throws Exception in exceptions occur
     */
    @Test
    public void testTargetDiscoveryNotification() throws Exception {
        final long probeId = createProbe();
        final Target target = createTarget(probeId, "1");
        final TargetListener listener = Mockito.mock(TargetListener.class);
        getTopologyProcessor().addTargetListener(listener);
        final Discovery discResult = new Discovery(probeId, target.getId(),
                        integrationTestServer.getBean(IdentityProvider.class));
        discResult.success();

        integrationTestServer.getBean(TopologyProcessorNotificationSender.class).notifyOperationState(discResult);

        final ArgumentCaptor<DiscoveryStatus> targetCaptor =
                        ArgumentCaptor.forClass(DiscoveryStatus.class);
        Mockito.verify(listener, Mockito.timeout(TIMEOUT_MS).times(1))
                        .onTargetDiscovered(targetCaptor.capture());
        final DiscoveryStatus result = targetCaptor.getValue();
        assertOperationResult(target.getId(), discResult, result);
        Assert.assertTrue(result.isSuccessful());
    }

    /**
     * Tests reporting target discovery failed result.
     *
     * @throws Exception in exceptions occur
     */
    @Test
    public void testTargetDiscoveryErrorNotification() throws Exception {
        final long probeId = createProbe();
        final Target target = createTarget(probeId, "1");
        final TargetListener listener = Mockito.mock(TargetListener.class);
        getTopologyProcessor().addTargetListener(listener);
        final Discovery discResult = new Discovery(probeId, target.getId(),
                        integrationTestServer.getBean(IdentityProvider.class));
        discResult.addError(ErrorDTO.newBuilder().setDescription("error1")
                        .setSeverity(ErrorSeverity.CRITICAL).build());
        discResult.addError(ErrorDTO.newBuilder().setDescription("error2")
                        .setSeverity(ErrorSeverity.WARNING).build());
        discResult.fail();

        integrationTestServer.getBean(TopologyProcessorNotificationSender.class).notifyOperationState(discResult);

        final ArgumentCaptor<DiscoveryStatus> targetCaptor =
                        ArgumentCaptor.forClass(DiscoveryStatus.class);
        Mockito.verify(listener, Mockito.timeout(TIMEOUT_MS).times(1))
                        .onTargetDiscovered(targetCaptor.capture());
        final DiscoveryStatus result = targetCaptor.getValue();
        assertOperationResult(target.getId(), discResult, result);
        Assert.assertFalse(result.isSuccessful());
    }

    @Test
    public void testTargetProgressNotification() throws Exception {
        final ActionExecutionListener listener = Mockito.mock(ActionExecutionListener.class);
        getTopologyProcessor().addActionListener(listener);

        final long probeId = createProbe();
        final Target target = createTarget(probeId, "1");
        final Action action = new Action(actionId, probeId, target.getId(),
                integrationTestServer.getBean(IdentityProvider.class), ActionType.MOVE);
        final MediationMessage.ActionResponse progressResponse = ActionResponse.newBuilder()
                .setProgress(33)
                .setActionResponseState(ActionResponseState.IN_PROGRESS)
                .setResponseDescription("Bulbasaur has evolved!")
                .build();

        action.updateProgress(progressResponse);

        integrationTestServer.getBean(TopologyProcessorNotificationSender.class).notifyOperationState(action);

        final ArgumentCaptor<ActionProgress> progressCaptor = ArgumentCaptor.forClass(ActionProgress.class);
        Mockito.verify(listener, Mockito.timeout(TIMEOUT_MS).times(1))
            .onActionProgress(progressCaptor.capture());

        final ActionProgress gotProgress = progressCaptor.getValue();
        Assert.assertEquals(progressResponse.getResponseDescription(), gotProgress.getDescription());
        Assert.assertEquals(progressResponse.getProgress(), gotProgress.getProgressPercentage());
        Assert.assertEquals(action.getActionId(), gotProgress.getActionId());
    }

    @Test
    public void testTargetSuccessNotification() throws Exception {
        final ActionExecutionListener listener = Mockito.mock(ActionExecutionListener.class);
        getTopologyProcessor().addActionListener(listener);

        final long probeId = createProbe();
        final Target target = createTarget(probeId, "1");
        final Action action = new Action(actionId, probeId, target.getId(),
                integrationTestServer.getBean(IdentityProvider.class), ActionType.MOVE);
        final MediationMessage.ActionResponse successResponse = ActionResponse.newBuilder()
                .setProgress(100)
                .setActionResponseState(ActionResponseState.SUCCEEDED)
                .setResponseDescription("Charmander has completed evolution!")
                .build();

        action.updateProgress(successResponse);
        action.success();

        final ArgumentCaptor<ActionSuccess> successCaptor = ArgumentCaptor.forClass(ActionSuccess.class);
        integrationTestServer.getBean(TopologyProcessorNotificationSender.class).notifyOperationState(action);
        Mockito.verify(listener, Mockito.timeout(TIMEOUT_MS).times(1))
               .onActionSuccess(successCaptor.capture());

        final ActionSuccess gotSuccess = successCaptor.getValue();
        Assert.assertEquals(successResponse.getResponseDescription(), gotSuccess.getSuccessDescription());
        Assert.assertEquals(action.getActionId(), gotSuccess.getActionId());
    }

    @Test
    public void testTargetFailureNotification() throws Exception {
        final ActionExecutionListener listener = Mockito.mock(ActionExecutionListener.class);
        getTopologyProcessor().addActionListener(listener);

        final long probeId = createProbe();
        final Target target = createTarget(probeId, "1");
        final Action action = new Action(actionId, probeId, target.getId(),
                integrationTestServer.getBean(IdentityProvider.class), ActionType.MOVE);
        final MediationMessage.ActionResponse failResponse = ActionResponse.newBuilder()
                .setProgress(100)
                .setActionResponseState(ActionResponseState.FAILED)
                .setResponseDescription("Pikachu died!")
                .build();

        action.updateProgress(failResponse);
        action.fail();

        final ArgumentCaptor<ActionFailure> failureCaptor = ArgumentCaptor.forClass(ActionFailure.class);
        integrationTestServer.getBean(TopologyProcessorNotificationSender.class).notifyOperationState(action);
        Mockito.verify(listener, Mockito.timeout(TIMEOUT_MS).times(1))
            .onActionFailure(failureCaptor.capture());

        final ActionFailure gotFailure = failureCaptor.getValue();
        Assert.assertEquals(failResponse.getResponseDescription(), gotFailure.getErrorDescription());
        Assert.assertEquals(action.getActionId(), gotFailure.getActionId());
    }

    @Test
    public void testProbeRegistrationNotification() throws Exception {
        final ProbeListener listener = Mockito.mock(ProbeListener.class);
        getTopologyProcessor().addProbeListener(listener);

        final long probeId = createProbe();
        final ArgumentCaptor<TopologyProcessorDTO.ProbeInfo> probeCaptor =
            ArgumentCaptor.forClass(TopologyProcessorDTO.ProbeInfo.class);
        Mockito.verify(listener, Mockito.timeout(TIMEOUT_MS).times(1))
            .onProbeRegistered(probeCaptor.capture());
        Assert.assertEquals(probeId, probeCaptor.getValue().getId());
    }

    private void assertOperationResult(long targetId, Operation expected, OperationStatus actual) {
        Assert.assertEquals(targetId, actual.getTargetId());
        Assert.assertEquals(toEpochMillis(expected.getStartTime()), actual.getStartTime().getTime());
        Assert.assertEquals(toEpochMillis(expected.getCompletionTime()),
                        actual.getCompletionTime().getTime());
        Assert.assertEquals(expected.getErrors(), actual.getErrorMessages());
        Assert.assertTrue(actual.isCompleted());
    }

    /**
     * Creates target spec for the specified probe with a specified id, put into account values.
     *
     * @param probeId probe id
     * @param id id to put into account values.
     * @return target spec
     */
    private TopologyProcessorDTO.TargetSpec createTargetSpec(long probeId, String id) {
        final TopologyProcessorDTO.AccountValue account =
                        TopologyProcessorDTO.AccountValue
                                        .newBuilder().setKey(Probes.mandatoryField
                                                        .getCustomDefinition().getName())
                                        .setStringValue(id).build();
        final TopologyProcessorDTO.TargetSpec spec = TopologyProcessorDTO.TargetSpec.newBuilder().setProbeId(probeId)
                        .addAccountValue(account).build();
        return spec;
    }

    /**
     * Creates a target with one mandatory field, filled with the specified {@code id} in account
     * values.
     *
     * @param probeId probe id to register target to
     * @param id string id to be used later for verifications
     * @return target object
     * @throws Exception on exceptions occurred
     */
    private Target createTarget(long probeId, String id) throws Exception {
        return targetStore.createTarget(createTargetSpec(probeId, id));
    }

    /**
     * Creates one probe with one mandatory field, registers it in probe store and returns its id.
     *
     * @return id of the probe
     * @throws Exception on exceptions occurred.
     */
    private long createProbe() throws Exception {
        @SuppressWarnings("unchecked")
        final ITransport<MediationServerMessage, MediationClientMessage> transport =
                        Mockito.mock(ITransport.class);
        probeStore.registerNewProbe(Probes.defaultProbe, transport);
        final long probeId = probeStore.getProbes().keySet().iterator().next();
        return probeId;
    }

    private String getProbeType(long probeId) throws Exception {
        return probeStore.getProbe(probeId)
                .map(MediationMessage.ProbeInfo::getProbeType)
                .orElseThrow(() -> new ProbeException("Probe " + probeId
                        + " is not registered"));
    }

    private static long toEpochMillis(LocalDateTime date) {
        return date.toInstant(ZoneOffset.ofTotalSeconds(0)).toEpochMilli();
    }

    private void sendEntities(final long topologyContextId, final long topologyId,
            @Nonnull final Collection<TopologyEntityDTO> entities)
        throws CommunicationException, InterruptedException, OversizedElementException {
        final TopologyInfo topologyInfo = TopologyInfo.newBuilder()
                .setTopologyType(TopologyType.PLAN)
                .setTopologyId(topologyId)
                .setTopologyContextId(topologyContextId)
                .setCreationTime(0L)
                .build();
        final TopologyBroadcast broadcast = getEntitiesListener()
                        .broadcastLiveTopology(topologyInfo);
        for (TopologyEntityDTO entity: entities) {
            broadcast.append(entity);
        }
        broadcast.finish();
        Assert.assertEquals(topologyId, broadcast.getTopologyId());
    }

    /**
     * Topology accumulator to store all the topology DTOs, sent by the server.
     */
    private class TopologyAccumulator implements EntitiesListener {

        private final CountDownLatch latch;

        /**
         * In this test, having a set is important because we sometimes send the same entities
         * multiple times (for convenience).
         */
        private final Set<TopologyEntityDTO> result = new HashSet<>();

        public TopologyAccumulator(int expectedInvocations) {
            this.latch = new CountDownLatch(expectedInvocations);
        }

        @Override
        public void onTopologyNotification(TopologyInfo topologyInfo,
                @Nonnull RemoteIterator<TopologyDTO.Topology.DataSegment> topologyDTOs,
                @Nonnull final SpanContext tracingContext) {
            try {
                while (topologyDTOs.hasNext()) {
                    result.addAll(topologyDTOs.nextChunk().stream().filter(
                        TopologyDTO.Topology.DataSegment::hasEntity).map(
                        TopologyDTO.Topology.DataSegment::getEntity).collect(Collectors.toList()));
                }
            } catch (Exception e) {
                logger.error("Error retrieving topologies", e);
            } finally {
                latch.countDown();
            }
        }

        public void await() throws InterruptedException {
            Assert.assertTrue(latch.await(10, TimeUnit.SECONDS));
        }
    }
}
