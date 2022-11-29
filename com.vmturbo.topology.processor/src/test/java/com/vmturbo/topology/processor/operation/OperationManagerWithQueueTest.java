package com.vmturbo.topology.processor.operation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyListOf;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;

import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import com.vmturbo.auth.api.licensing.LicenseCheckClient;
import com.vmturbo.commons.Pair;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.communication.ITransport;
import com.vmturbo.components.api.SetOnce;
import com.vmturbo.components.common.featureflags.FeatureFlags;
import com.vmturbo.matrix.component.TheMatrix;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.NotificationDTO;
import com.vmturbo.platform.common.dto.CommonDTO.NotificationDTO.Severity;
import com.vmturbo.platform.common.dto.Discovery.AccountValue;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryContextDTO;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryType;
import com.vmturbo.platform.common.dto.Discovery.ErrorDTO;
import com.vmturbo.platform.common.dto.Discovery.ErrorDTO.ErrorSeverity;
import com.vmturbo.platform.common.dto.Discovery.NoChange;
import com.vmturbo.platform.sdk.common.MediationMessage.ContainerInfo;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationClientMessage;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationServerMessage;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.platform.sdk.common.util.NotificationCategoryDTO;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.test.utils.FeatureFlagTestRule;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.OperationStatus.Status;
import com.vmturbo.topology.processor.communication.ProbeContainerChooser;
import com.vmturbo.topology.processor.communication.RemoteMediationServerWithDiscoveryWorkers;
import com.vmturbo.topology.processor.communication.queues.IDiscoveryQueueElement;
import com.vmturbo.topology.processor.controllable.EntityActionDao;
import com.vmturbo.topology.processor.cost.AliasedOidsUploader;
import com.vmturbo.topology.processor.cost.BilledCloudCostUploader;
import com.vmturbo.topology.processor.cost.DiscoveredCloudCostUploader;
import com.vmturbo.topology.processor.discoverydumper.BinaryDiscoveryDumper;
import com.vmturbo.topology.processor.discoverydumper.TargetDumpingSettings;
import com.vmturbo.topology.processor.entity.EntityStore;
import com.vmturbo.topology.processor.group.discovery.DiscoveredGroupUploader;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.notification.SystemNotificationProducer;
import com.vmturbo.topology.processor.operation.OperationTestUtilities.TrackingOperationListener;
import com.vmturbo.topology.processor.operation.discovery.Discovery;
import com.vmturbo.topology.processor.operation.discovery.DiscoveryBundle;
import com.vmturbo.topology.processor.operation.discovery.DiscoveryMessageHandler;
import com.vmturbo.topology.processor.planexport.DiscoveredPlanDestinationUploader;
import com.vmturbo.topology.processor.probeproperties.ProbePropertyStore;
import com.vmturbo.topology.processor.probes.ProbeException;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.targets.DerivedTargetParser;
import com.vmturbo.topology.processor.targets.GroupScopeResolver;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetNotFoundException;
import com.vmturbo.topology.processor.targets.TargetStore;
import com.vmturbo.topology.processor.template.DiscoveredTemplateDeploymentProfileUploader;
import com.vmturbo.topology.processor.workflow.DiscoveredWorkflowUploader;

/**
 * Testing the {@link OperationManagerWithQueue} functionality.
 */
@RunWith(JUnitParamsRunner.class)
public class OperationManagerWithQueueTest {

    /**
     * Feature flag rule.
     */
    @Rule
    public FeatureFlagTestRule featureFlagTestRule = new FeatureFlagTestRule(
            FeatureFlags.ENABLE_PROBE_AUTH);
    /**
     * Feature flag rule mandatory.
     */
    @Rule
    public FeatureFlagTestRule featureFlagTestRuleMandatory = new FeatureFlagTestRule(
            FeatureFlags.ENABLE_MANDATORY_PROBE_AUTH);

    private final IdentityProvider identityProvider = mock(IdentityProvider.class);

    private final EntityActionDao entityActionDao = mock(EntityActionDao.class);

    private final ProbeStore probeStore = mock(ProbeStore.class);

    private final GroupScopeResolver groupScopeResolver = mock(GroupScopeResolver.class);

    private final TargetDumpingSettings targetDumpingSettings = mock(TargetDumpingSettings.class);

    private final SystemNotificationProducer systemNotificationProducer = mock(SystemNotificationProducer.class);

    private final TargetStore targetStore = mock(TargetStore.class);

    private final ProbeContainerChooser containerChooser = mock(ProbeContainerChooser.class);

    private final ProbePropertyStore probePropertyStore = mock(ProbePropertyStore.class);

    private final EntityStore entityStore = mock(EntityStore.class);

    private final DiscoveredGroupUploader discoveredGroupUploader = mock(DiscoveredGroupUploader.class);
    private final DiscoveredWorkflowUploader discoveredWorkflowUploader = mock(DiscoveredWorkflowUploader.class);
    private final DiscoveredCloudCostUploader discoveredCloudCostUploader = mock(DiscoveredCloudCostUploader.class);
    private final BilledCloudCostUploader billedCloudCostUploader = mock(BilledCloudCostUploader.class);
    private final AliasedOidsUploader aliasedOidsUploader = mock(AliasedOidsUploader.class);
    private final DiscoveredPlanDestinationUploader discoveredPlanDestinationUploader = mock(DiscoveredPlanDestinationUploader.class);

    private TrackingOperationListener operationListener = spy(new TrackingOperationListener());

    private DiscoveredTemplateDeploymentProfileUploader discoveredTemplatesUploader = mock(DiscoveredTemplateDeploymentProfileUploader.class);

    private DerivedTargetParser derivedTargetParser = mock(DerivedTargetParser.class);

    private BinaryDiscoveryDumper binaryDiscoveryDumper =
        mock(BinaryDiscoveryDumper.class);

    private final LicenseCheckClient licenseCheckClient = mock(LicenseCheckClient.class);

    @Mock
    private ITransport<MediationServerMessage, MediationClientMessage> transport1;

    @Mock
    private ITransport<MediationServerMessage, MediationClientMessage> transport2;

    ArgumentCaptor<ITransport.EventHandler> transportListener =
            ArgumentCaptor.forClass(ITransport.EventHandler.class);

    private final TestAggregatingDiscoveryQueue discoveryQueue = spy(
            new TestAggregatingDiscoveryQueue(null));

    private final RemoteMediationServerWithDiscoveryWorkers remoteMediationServer =
            spy(new RemoteMediationServerWithDiscoveryWorkers(probeStore, probePropertyStore,
                    containerChooser, discoveryQueue, 1, 1, 10,
                    Mockito.mock(TargetStore.class)));

    private OperationManager operationManager;

    private static final String VC_PROBE_TYPE = "VC";

    private static final long probeIdVc = 1111L;

    private static final long target1Id = 1010L;

    private static final long target2Id = 2020L;

    private Target target1 = mock(Target.class);

    private Target target2 = mock(Target.class);

    private final ProbeInfo probeInfoVc1 = ProbeInfo.newBuilder()
            .setProbeCategory(ProbeCategory.HYPERVISOR.getCategory())
            .setProbeType(VC_PROBE_TYPE)
            .setIncrementalRediscoveryIntervalSeconds(15)
            .build();

    private final ContainerInfo containerInfo = ContainerInfo.newBuilder()
            .addProbes(probeInfoVc1)
            .build();

    private final EntityDTO entity = EntityDTO.newBuilder()
            .setEntityType(EntityType.VIRTUAL_MACHINE)
            .setId("vm-1")
            .build();

    private String target1IdFields = "target1";

    private String target2IdFields = "target2";

    /**
     * Argument captor for simulating returning a permit to the TransportWorker.
     */
    @Captor
    private ArgumentCaptor<MediationServerMessage> mediationMsgArgumentCaptor;

    private AccountValue target1AccountValue = AccountValue.newBuilder()
            .setKey("address").setStringValue("foo").build();

    private AccountValue target2AccountValue = AccountValue.newBuilder()
            .setKey("address").setStringValue("bar").build();

    /**
     * Initializes the tests.
     *
     * @throws Exception on exceptions occurred
     */
    @Before
    public void setup() throws Exception {
        MockitoAnnotations.initMocks(this);
        operationManager = new OperationManagerWithQueue(identityProvider, targetStore,
                probeStore, remoteMediationServer, operationListener, entityStore, discoveredGroupUploader,
                discoveredWorkflowUploader, discoveredCloudCostUploader, billedCloudCostUploader,
                aliasedOidsUploader, discoveredPlanDestinationUploader, discoveredTemplatesUploader,
                entityActionDao, derivedTargetParser, groupScopeResolver, targetDumpingSettings,
                systemNotificationProducer,
                discoveryQueue, 10, 10, 10, 10, TheMatrix.instance(), binaryDiscoveryDumper, false,
                licenseCheckClient, 60000);
        IdentityGenerator.initPrefix(0);
        when(identityProvider.generateOperationId()).thenAnswer((invocation) -> IdentityGenerator.next());

        when(identityProvider.getProbeId(probeInfoVc1)).thenReturn(probeIdVc);

        when(targetDumpingSettings.getDumpsToHold(any())).thenReturn(0);
        doNothing().when(targetDumpingSettings).refreshSettings();
        when(probeStore.getProbeIdForType(VC_PROBE_TYPE)).thenReturn(Optional.of(probeIdVc));
        when(probeStore.getProbe(probeIdVc)).thenReturn(Optional.of(probeInfoVc1));
        when(target1.getProbeId()).thenReturn(probeIdVc);
        when(target1.getId()).thenReturn(target1Id);
        when(target1.getSerializedIdentifyingFields()).thenReturn(target1IdFields);
        when(target1.getProbeInfo()).thenReturn(probeInfoVc1);
        when(target2.getProbeId()).thenReturn(probeIdVc);
        when(target2.getId()).thenReturn(target2Id);
        when(target2.getSerializedIdentifyingFields()).thenReturn(target2IdFields);
        when(target2.getProbeInfo()).thenReturn(probeInfoVc1);
        when(target1.getMediationAccountVals(any())).thenReturn(Collections
                .singletonList(target1AccountValue));
        when(target2.getMediationAccountVals(any())).thenReturn(Collections
                .singletonList(target2AccountValue));
        when(targetStore.getTarget(target1Id)).thenReturn(Optional.of(target1));
        when(targetStore.getProbeTargets(probeIdVc)).thenReturn(Collections.singletonList(target1));
        doReturn(Optional.ofNullable(
                SDKProbeType.create(target1.getProbeInfo().getProbeType()))).when(targetStore)
                .getProbeTypeForTarget(target1Id);
    }

    private Optional<IDiscoveryQueueElement> simulateRemoteMediation(
            @Nonnull DiscoveryType discoveryType,
            @Nonnull Runnable runAfterDiscovery) {
        return simulateRemoteMediation(discoveryType, runAfterDiscovery, false, null);
    }

    private Optional<IDiscoveryQueueElement> simulateRemoteMediation(
            @Nonnull DiscoveryType discoveryType, @Nonnull Runnable runAfterDiscovery,
            boolean expire) {
        return simulateRemoteMediation(discoveryType, runAfterDiscovery, expire, null);
    }

    private Optional<IDiscoveryQueueElement> simulateRemoteMediation(
            @Nonnull DiscoveryType discoveryType, @Nonnull Runnable runAfterDiscovery,
            boolean expire, @Nullable DiscoveryResponse response) {
        Optional<IDiscoveryQueueElement> element = discoveryQueue.pollNextQueuedDiscovery(
                transport1, Collections.singletonList(probeIdVc), discoveryType);
        final SetOnce<DiscoveryMessageHandler> handler = new SetOnce<>();
        element.ifPresent(elmnt -> elmnt.performDiscovery((bundle) -> {
            handler.trySetValue(bundle.getDiscoveryMessageHandler());
            return bundle.getDiscovery();
        }, runAfterDiscovery));
        if (expire) {
            handler.getValue().get().onExpiration();
        } else if (response != null) {
            handler.getValue().get()
                    .onMessage(MediationClientMessage.newBuilder()
                            .setDiscoveryResponse(response)
                            .build());
        }
        return element;
    }

    private Pair<IDiscoveryQueueElement, DiscoveryBundle> simulateRemoteMediationAndReturnBundle(
            @Nonnull DiscoveryType discoveryType, @Nonnull Runnable runAfterDiscovery,
            boolean expire, @Nullable DiscoveryResponse response) {
        Optional<IDiscoveryQueueElement> element = discoveryQueue.pollNextQueuedDiscovery(
                transport1, Collections.singletonList(probeIdVc), discoveryType);
        final SetOnce<DiscoveryBundle> retBundle = new SetOnce<>();
        element.ifPresent(elmnt -> elmnt.performDiscovery((bundle) -> {
            retBundle.trySetValue(bundle);
            return bundle.getDiscovery();
        }, runAfterDiscovery));
        if (expire) {
            retBundle.getValue().get().getDiscoveryMessageHandler().onExpiration();
        } else if (response != null) {
            retBundle.getValue().get().getDiscoveryMessageHandler()
                    .onMessage(MediationClientMessage.newBuilder()
                            .setDiscoveryResponse(response)
                            .build());
        }
        return new Pair<>(element.get(), retBundle.getValue().get());
    }

    private Discovery startAndRunDiscovery(long targetId,
            @Nonnull DiscoveryType discoveryType,
            @Nonnull Runnable runAfter)
            throws InterruptedException, ProbeException, TargetNotFoundException,
            CommunicationException {
        operationManager.startDiscovery(targetId, discoveryType, false);
        return simulateRemoteMediation(discoveryType, runAfter).get().getDiscovery(0);
    }

    private Pair<IDiscoveryQueueElement, DiscoveryBundle> runDiscoveryWithResult(long targetId, @Nonnull DiscoveryType discoveryType,
            @Nonnull Runnable runAfter, @Nonnull DiscoveryResponse response)
            throws InterruptedException {
        return simulateRemoteMediationAndReturnBundle(discoveryType, runAfter, false, response);
    }

    private Discovery addPendingAndStartDiscovery(long targetId,
            @Nonnull DiscoveryType discoveryType)
            throws InterruptedException, TargetNotFoundException, CommunicationException {
        operationManager.addPendingDiscovery(targetId, discoveryType);
        return simulateRemoteMediation(discoveryType, () -> { }).get().getDiscovery(0);
    }

    /**
     * Test starting a discovery operation.
     *
     * @param discoveryType type of the discovery to test
     * @throws Exception If anything goes wrong.
     */
    @Test
    @Parameters({"FULL", "INCREMENTAL"})
    public void testDiscoverTarget(DiscoveryType discoveryType) throws Exception {
        // This will add an element to the discoveryQueue, but no Discovery will be created yet
        final Optional<Discovery> discovery = operationManager.startDiscovery(target1Id,
                discoveryType, false);
        assertFalse(discovery.isPresent());

        // Simulate the processing of the discoveryQueue. At this point Discovery is created and
        // DiscoveryRequest sent to mediation container
        final Optional<IDiscoveryQueueElement> optElement = simulateRemoteMediation(discoveryType,
                () -> { });

        // Make sure discovery exists and is consistent with what was sent to mediation
        final List<Discovery> discoveryList = operationManager.getInProgressDiscoveries();
        assertTrue(optElement.isPresent());
        assertEquals(1, discoveryList.size());
        assertEquals(optElement.get().getDiscovery(0), discoveryList.get(0));

        // test that if we call startDiscovery again, we get back the existing discovery
        final Optional<Discovery> secondDiscovery = operationManager.startDiscovery(target1Id,
                discoveryType, false);
        assertTrue(secondDiscovery.isPresent());
        assertEquals(optElement.get().getDiscovery(0), secondDiscovery.get());
    }

    /**
     * Test getting ongoing discovery by target.
     *
     * @param discoveryType type of the discovery to test
     * @throws Exception If anything goes wrong.
     */
    @Test
    @Parameters({"FULL", "INCREMENTAL"})
    public void testGetInProgressDiscoveryForTarget(DiscoveryType discoveryType) throws Exception {
        // This will add an element to the discoveryQueue, but no Discovery will be created yet
        final Optional<Discovery> discovery = operationManager.startDiscovery(target1Id,
                discoveryType, false);
        assertFalse(discovery.isPresent());
        final AtomicBoolean cleanupRun = new AtomicBoolean(false);

        // This will simulate the RemoteMediationServer running the discovery and returning the
        // DiscoveryResponse.
        final DiscoveryBundle bundle = runDiscoveryWithResult(target1Id, discoveryType,
                () -> cleanupRun.set(true), DiscoveryResponse.getDefaultInstance()).second;
        OperationTestUtilities.waitForEvent(() ->
                bundle.getDiscovery().getStatus() == Status.SUCCESS);
        assertFalse(operationManager.getInProgressDiscoveryForTarget(target1Id,
                discoveryType).isPresent());

        // Make sure our runnable was run after discovery completed.
        assertTrue(cleanupRun.get());
    }

    /**
     * Test getting last discovery by target.
     *
     * @param discoveryType type of the discovery to test
     * @throws Exception If anything goes wrong.
     */
    @Test
    @Parameters({"FULL", "INCREMENTAL"})
    public void testGetLastDiscoveryForTarget(DiscoveryType discoveryType) throws Exception {
        // This will add an element to the discoveryQueue, but no Discovery will be created yet
        final Optional<Discovery> discovery = operationManager.startDiscovery(target1Id,
                discoveryType, false);

        assertEquals(Optional.empty(),
            operationManager.getLastDiscoveryForTarget(target1Id, discoveryType));

        // Simulate the processing of the discoveryQueue. At this point Discovery is created and
        // DiscoveryRequest sent to mediation container
        final Optional<IDiscoveryQueueElement> optElement = simulateRemoteMediation(discoveryType,
                () -> { });

        // Make sure that we can still get the discovery after the
        // operation is complete.
        OperationTestUtilities.notifyAndWaitForDiscovery(operationManager,
                optElement.get().getDiscovery(0), DiscoveryResponse.getDefaultInstance());

        final Discovery lastDiscovery = operationManager.getLastDiscoveryForTarget(target1Id,
                discoveryType).get();
        assertEquals(optElement.get().getDiscovery(0), lastDiscovery);
        assertEquals(Status.SUCCESS, lastDiscovery.getStatus());
    }

    /**
     * Test that a completed discovery gets processed properly.
     *
     * @param discoveryType type of the discovery to test
     * @throws Exception If anything goes wrong.
     */
    @Test
    @Parameters({"FULL", "INCREMENTAL"})
    public void testProcessDiscoverySuccess(DiscoveryType discoveryType) throws Exception {
        // This will add an element to the discoveryQueue, but no Discovery will be created yet
        final Optional<Discovery> discovery = operationManager.startDiscovery(target1Id,
                discoveryType, false);
        final DiscoveryResponse result = DiscoveryResponse.newBuilder()
                .addEntityDTO(entity)
                .build();

        // Simulate the processing of the discoveryQueue. At this point Discovery is created and
        // DiscoveryRequest sent to mediation container
        final Optional<IDiscoveryQueueElement> optElement = simulateRemoteMediation(discoveryType,
                () -> { });

        OperationTestUtilities.notifyAndWaitForDiscovery(operationManager,
                optElement.get().getDiscovery(0), result);

        verify(entityStore).entitiesDiscovered(eq(probeIdVc), eq(target1Id),
                eq(optElement.get().getDiscovery(0).getMediationMessageId()),
            eq(discoveryType), eq(Collections.singletonList(entity)));
    }

    /**
     * Test that a failed discovery gets processed properly.
     *
     * @param discoveryType type of the discovery to test
     * @throws Exception If anything goes wrong.
     */
    @Test
    @Parameters({"FULL", "INCREMENTAL"})
    public void testProcessDiscoveryFailure(DiscoveryType discoveryType) throws Exception {
        // This will add an element to the discoveryQueue, but no Discovery will be created yet
        operationManager.startDiscovery(target1Id,
                discoveryType, false);
        // Critical errors applying to the target rather than a specific entity
        // should prevent any EntityDTOs in the discovery from being added to
        // the topology snapshot for the target.
        final DiscoveryResponse result = DiscoveryResponse.newBuilder()
                .addEntityDTO(entity)
                .addErrorDTO(ErrorDTO.newBuilder()
                        .setSeverity(ErrorSeverity.CRITICAL)
                        .setDescription("error"))
                .build();

        // Simulate the processing of the discoveryQueue. At this point Discovery is created and
        // DiscoveryRequest sent to mediation container
        final Optional<IDiscoveryQueueElement> optElement = simulateRemoteMediation(discoveryType,
                () -> { });
        final Discovery discovery = optElement.get().getDiscovery(0);

        OperationTestUtilities.notifyAndWaitForDiscovery(operationManager, discovery, result);
        Mockito.verify(entityStore, never()).entitiesDiscovered(anyLong(), anyLong(), anyInt(),
            eq(discoveryType), any());
    }

    /**
     * Test that failed and successful discovery notifications get processed properly.
     *
     * @param discoveryType type of the discovery to test
     * @throws Exception If anything goes wrong.
     */
    @Test
    @Parameters({"FULL", "INCREMENTAL"})
    public void testProcessDiscoveryNotification(DiscoveryType discoveryType) throws Exception {
        // Failure
        // This will add an element to the discoveryQueue, but no Discovery will be created yet
        operationManager.startDiscovery(target1Id, discoveryType, false);
        // Critical errors applying to the target rather than a specific entity
        // should prevent any EntityDTOs in the discovery from being added to
        // the topology snapshot for the target.
        NotificationDTO.Builder notification = NotificationDTO.newBuilder()
                        .setEvent("Target Discovery")
                        .setCategory(NotificationCategoryDTO.DISCOVERY.toString())
                        .setSeverity(Severity.CRITICAL).setDescription("Discovery Failed");
        final DiscoveryResponse resultFailure = DiscoveryResponse.newBuilder()
                .addEntityDTO(entity)
                .addErrorDTO(ErrorDTO.newBuilder()
                        .setSeverity(ErrorSeverity.CRITICAL)
                        .setDescription("error"))
                .addNotification(notification)
                .build();

        // Simulate the processing of the discoveryQueue. At this point Discovery is created and
        // DiscoveryRequest sent to mediation container
        final Optional<IDiscoveryQueueElement> optElement = simulateRemoteMediation(discoveryType,
                () -> { });
        final Discovery discovery = optElement.get().getDiscovery(0);

        // Wait until we receive notification of the failure
        operationManager.notifyDiscoveryResult(discovery, resultFailure).get(
                OperationTestUtilities.DISCOVERY_PROCESSING_TIMEOUT_SECONDS, TimeUnit.SECONDS);

        // Discovery should have finished
        assertFalse(operationManager.getInProgressDiscovery(discovery.getId()).isPresent());

        //Success
        operationManager.startDiscovery(target1Id, discoveryType, false);
        final DiscoveryResponse resultSuccess = DiscoveryResponse.newBuilder()
                        .addEntityDTO(entity)
                        .addNotification(notification)
                        .build();

        // Simulate the processing of the discoveryQueue. At this point Discovery is created and
        // DiscoveryRequest sent to mediation container
        final Optional<IDiscoveryQueueElement> optElement2 = simulateRemoteMediation(discoveryType,
                () -> { });
        final Discovery discovery2 = optElement.get().getDiscovery(0);

        // Wait until we receive notification of the failure
        operationManager.notifyDiscoveryResult(
                discovery2, resultSuccess)
                .get(OperationTestUtilities.DISCOVERY_PROCESSING_TIMEOUT_SECONDS, TimeUnit.SECONDS);

        // Check that discovery2 started
        assertFalse(operationManager.getInProgressDiscovery(discovery2.getId()).isPresent());
    }

    /**
     * Test that a discovery with no change gets processed properly.
     *
     * @param discoveryType type of the discovery to test
     * @throws Exception If anything goes wrong.
     */
    @Test
    @Parameters({"FULL", "INCREMENTAL"})
    public void testProcessDiscoveryNoChange(DiscoveryType discoveryType) throws Exception {
        // This will add an element to the discoveryQueue, but no Discovery will be created yet
        operationManager.startDiscovery(target1Id, discoveryType, false);

        // When the probe responds that nothing has changed (the NoChange message)
        // the code that interacts with the entity store is skipped.
        final DiscoveryResponse result = DiscoveryResponse.newBuilder()
                .setNoChange(NoChange.getDefaultInstance())
                .build();

        // Simulate the processing of the discoveryQueue. At this point Discovery is created and
        // DiscoveryRequest sent to mediation container
        final Optional<IDiscoveryQueueElement> optElement = simulateRemoteMediation(discoveryType,
                () -> { });
        final Discovery discovery = optElement.get().getDiscovery(0);

        OperationTestUtilities.notifyAndWaitForDiscovery(operationManager, discovery, result);
        Mockito.verify(entityStore, never()).entitiesDiscovered(anyLong(), anyLong(), anyInt(),
            eq(discoveryType), any());
    }

    /**
     * Test that a discovery context received in the last discovery response is
     * placed in the subsequent discovery request.
     *
     * @param discoveryType type of the discovery to test
     * @throws Exception If anything goes wrong.
     */
    @Test
    @Parameters({"FULL", "INCREMENTAL"})
    public void testSendDiscoveryContext(DiscoveryType discoveryType) throws Exception {
        // This will add an element to the discoveryQueue, but no Discovery will be created yet
        operationManager.startDiscovery(target1Id,
                discoveryType, false);
        DiscoveryContextDTO contextResponse = DiscoveryContextDTO.newBuilder()
                .putContextEntry("A", "B")
                .build();
        final DiscoveryResponse result = DiscoveryResponse.newBuilder()
                .setDiscoveryContext(contextResponse)
                .build();

        // Simulate the discovery being run and the result being returned.
       final Pair<IDiscoveryQueueElement, DiscoveryBundle> pair1 = runDiscoveryWithResult(target1Id,
               discoveryType, () -> { }, result);

        // wait for operationManager to process discovery on a different thread
        OperationTestUtilities.waitForEvent(() ->
                pair1.second.getDiscovery().getStatus() == Status.SUCCESS);

        // Queue the next discovery
        operationManager.startDiscovery(target1Id, discoveryType, false);

        final Pair<IDiscoveryQueueElement, DiscoveryBundle> pair2 =
                simulateRemoteMediationAndReturnBundle(discoveryType, () -> { }, false, null);

        // Verify that the first discovery request contained an empty discovery context
        assertEquals(DiscoveryContextDTO.getDefaultInstance(),
            pair1.second.getDiscoveryRequest().getDiscoveryContext());
        // Verify that the second discovery request contained the discovery context
        // received in the first discovery response
        assertEquals(contextResponse, pair2.second.getDiscoveryRequest().getDiscoveryContext());
    }

    /**
     * Test that a failed discovery does not overwrite the topology
     * from the previous successful discovery.
     *
     * @param discoveryType type of the discovery to test
     * @throws Exception If anything goes wrong.
     */
    @Test
    @Parameters({"FULL", "INCREMENTAL"})
    public void testProcessDiscoveryFailureDoesNotClearPreviousResult(DiscoveryType discoveryType)
            throws Exception {
        // This will add an element to the discoveryQueue, but no Discovery will be created yet
        operationManager.startDiscovery(target1Id,
                discoveryType, false);
        final DiscoveryResponse.Builder responseBuilder = DiscoveryResponse.newBuilder()
                .addEntityDTO(entity);

        // Simulate the processing of the discoveryQueue. At this point Discovery is created and
        // DiscoveryRequest sent to mediation container
        final Optional<IDiscoveryQueueElement> optElement = simulateRemoteMediation(discoveryType,
                () -> { });
        final Discovery discovery = optElement.get().getDiscovery(0);

        OperationTestUtilities.notifyAndWaitForDiscovery(operationManager, discovery,
                responseBuilder.build());
        verify(entityStore).entitiesDiscovered(eq(probeIdVc), eq(target1Id),
                eq(discovery.getMediationMessageId()), eq(discoveryType),
                eq(Collections.singletonList(entity)));

        final DiscoveryResponse errorResponse = responseBuilder
                .addErrorDTO(ErrorDTO.newBuilder().setSeverity(ErrorSeverity.CRITICAL).setDescription("error"))
                .build();

        // This will add an element to the discoveryQueue, but no Discovery will be created yet
        operationManager.startDiscovery(target1Id, discoveryType, false);

        // Simulate the processing of the discoveryQueue. At this point Discovery is created and
        // DiscoveryRequest sent to mediation container
        final Optional<IDiscoveryQueueElement> optElement2 = simulateRemoteMediation(discoveryType,
                () -> { });
        final Discovery discovery2 = optElement2.get().getDiscovery(0);

        OperationTestUtilities.notifyAndWaitForDiscovery(operationManager, discovery2, errorResponse);

        // The failed discovery shouldn't have triggered another call to the entity store.
        verify(entityStore).entitiesDiscovered(eq(probeIdVc), eq(target1Id), anyInt(),
                eq(discoveryType), eq(Collections.singletonList(entity)));
    }

    /**
     * Test that a timed out discovery does not overwrite the topology
     * from the previous successful discovery.
     *
     * @param discoveryType type of the discovery to test
     * @throws Exception If anything goes wrong.
     */
    @Test
    @Parameters({"FULL", "INCREMENTAL"})
    public void testProcessDiscoveryTimeoutDoesNotClearPreviousResult(DiscoveryType discoveryType) throws Exception {
        // This will add an element to the discoveryQueue, but no Discovery will be created yet
        operationManager.startDiscovery(target1Id,
                discoveryType, false);
        final DiscoveryResponse response = DiscoveryResponse.newBuilder()
                .addEntityDTO(entity)
                .build();

        // Simulate the processing of the discoveryQueue. At this point Discovery is created and
        // DiscoveryRequest sent to mediation container
        final Optional<IDiscoveryQueueElement> optElement = simulateRemoteMediation(discoveryType,
                () -> { });
        final Discovery discovery = optElement.get().getDiscovery(0);

        OperationTestUtilities.notifyAndWaitForDiscovery(operationManager, discovery, response);

        verify(entityStore, times(1)).entitiesDiscovered(eq(probeIdVc),
                eq(target1Id), anyInt(), eq(discoveryType), eq(Collections.singletonList(entity)));

        // This will add an element to the discoveryQueue, but no Discovery will be created yet
        operationManager.startDiscovery(target1Id, discoveryType, false);

        // Simulate the processing of the discoveryQueue. At this point Discovery is created and
        // DiscoveryRequest sent to mediation container and onExpiration is called on the
        // DiscoveryResponseHandler
        final Optional<IDiscoveryQueueElement> optElement2 = simulateRemoteMediation(discoveryType,
                () -> { }, true);
        final Discovery discovery2 = optElement.get().getDiscovery(0);

        operationListener.awaitOperation(
                operation -> operation.equals(discovery2) && operation.getCompletionTime() != null);

        // The timeout shouldn't have resulted in another call to entitiesDiscovered.
        verify(entityStore, times(1)).entitiesDiscovered(eq(probeIdVc),
                eq(target1Id), anyInt(), eq(discoveryType), eq(Collections.singletonList(entity)));
    }

    /**
     * Transport closed when performing discovery. Critical error with the message is expected.
     *
     * @param discoveryType type of the discovery to test
     * @throws Exception If anything goes wrong.
     */
    @Test
    @Parameters({"FULL", "INCREMENTAL"})
    public void testProcessDiscoveryCancelOperation(DiscoveryType discoveryType) throws Exception {
        // This will add an element to the discoveryQueue, but no Discovery will be created yet
        operationManager.startDiscovery(target1Id,
                discoveryType, false);
        // Simulate the processing of the discoveryQueue. At this point Discovery is created and
        // DiscoveryRequest sent to mediation container
        final Optional<IDiscoveryQueueElement> optElement = simulateRemoteMediation(discoveryType,
                () -> { });
        final Discovery discovery = optElement.get().getDiscovery(0);

        assertTrue(operationManager.getInProgressDiscovery(discovery.getId()).isPresent());
        operationManager.onTargetRemoved(target1);
        operationListener.awaitOperation(
                operation -> operation.equals(discovery) && operation.getCompletionTime() != null);
        final List<String> errors = discovery.getErrors();
        assertEquals(1, errors.size());
        final String errorMessage = errors.iterator().next();
        Assert.assertThat(errorMessage,
                CoreMatchers.containsString("Target " + target1Id + " removed."));
    }

    /**
     * Test getting ongoing discoveries when there are none.
     */
    @Test
    public void testGetOngoingDiscoveriesEmpty() {
        assertTrue(operationManager.getInProgressDiscoveries().isEmpty());
    }


    /**
     * Test getting ongoing discoveries.
     *
     * @param discoveryType type of the discovery to test
     * @throws Exception If anything goes wrong.
     */
    @Test
    @Parameters({"FULL", "INCREMENTAL"})
    public void testGetOngoingDiscoveries(DiscoveryType discoveryType) throws Exception {
        // This will add an element to the discoveryQueue, but no Discovery will be created yet
        operationManager.startDiscovery(target1Id,
                discoveryType, false);
        // Simulate the processing of the discoveryQueue. At this point Discovery is created and
        // DiscoveryRequest sent to mediation container
        final Optional<IDiscoveryQueueElement> optElement = simulateRemoteMediation(discoveryType,
                () -> { });
        final Discovery discovery = optElement.get().getDiscovery(0);

        assertEquals(1, operationManager.getInProgressDiscoveries().size());
        final Discovery discoveryInprogress = operationManager.getInProgressDiscoveries().get(0);

        assertEquals(discovery.getId(), discoveryInprogress.getId());
        assertEquals(target1Id, discoveryInprogress.getTargetId());
    }

    /**
     * Test addPendingDiscovery when no discovery is in progress.
     *
     * @param discoveryType type of the discovery to test
     * @throws Exception If anything goes wrong.
     */
    @Test
    @Parameters({"FULL", "INCREMENTAL"})
    public void testPendingDiscoverNoOngoing(DiscoveryType discoveryType) throws Exception {
        long discoveryId = addPendingAndStartDiscovery(target1Id, discoveryType).getId();
        assertEquals(1, operationManager.getInProgressDiscoveries().size());
        Discovery discovery = operationManager.getInProgressDiscoveries().get(0);

        assertEquals(discoveryId, discovery.getId());
        assertEquals(target1Id, discovery.getTargetId());
        assertFalse(operationManager.hasPendingDiscovery(target1Id, discoveryType));
    }

    /**
     * Test addPendingDiscovery when there is discovery is in progress.
     *
     * @param discoveryType type of the discovery to test
     * @throws Exception If anything goes wrong.
     */
    @Test
    @Parameters({"FULL", "INCREMENTAL"})
    public void testPendingDiscoverWithOngoing(DiscoveryType discoveryType) throws Exception {
        // This will add an element to the discoveryQueue, but no Discovery will be created yet
        final Discovery originalDiscovery = startAndRunDiscovery(target1Id,
                discoveryType, () -> { });
        final Optional<Discovery> newDiscoveryQueued =
                operationManager.addPendingDiscovery(target1Id, discoveryType);

        assertFalse(newDiscoveryQueued.isPresent());
        assertTrue(operationManager.hasPendingDiscovery(target1Id, discoveryType));
        assertEquals(1, operationManager.getInProgressDiscoveries().size());

        DiscoveryResponse.Builder responseBuilder = DiscoveryResponse.newBuilder()
                .addEntityDTO(entity);

        when(probeStore.isProbeConnected(anyLong())).thenReturn(true);
        when(probeStore.isAnyTransportConnectedForTarget(any())).thenReturn(true);
        OperationTestUtilities.notifyAndWaitForDiscovery(operationManager, originalDiscovery,
                responseBuilder.build());
        verify(entityStore).entitiesDiscovered(eq(probeIdVc), eq(target1Id), anyInt(),
                eq(discoveryType), eq(Collections.singletonList(entity)));

        // After the current discovery completes, the pending discovery should be removed
        // and an actual discovery should be kicked off.
        assertFalse(operationManager.hasPendingDiscovery(target1Id, discoveryType));
        assertTrue(operationManager.getLastDiscoveryForTarget(target1Id, discoveryType).isPresent());
    }

    /**
     * Test that discovery is marked as pending if probe is unregistered.
     *
     * @param discoveryType type of the discovery to test
     * @throws Exception If anything goes wrong.
     */
    @Test
    @Parameters({"FULL"})
    public void testPendingDiscoverWithUnregisteredProbe(DiscoveryType discoveryType) throws Exception {
        when(probeStore.getProbe(probeIdVc)).thenReturn(Optional.empty());

        operationManager.addPendingDiscovery(target1Id, discoveryType);
        assertTrue(operationManager.hasPendingDiscovery(target1Id, discoveryType));
    }

    /**
     * Test that pending discovery is activated once probe is registered.
     *
     * @param discoveryType type of the discovery to test
     * @throws Exception If anything goes wrong.
     */
    @Test
    @Parameters({"FULL"})
    public void testOnProbeRegisteredActivatesPendingDiscoveries(DiscoveryType discoveryType) throws Exception {
        when(probeStore.getProbe(probeIdVc)).thenReturn(Optional.empty());
        when(probeStore.isAnyTransportConnectedForTarget(any())).thenReturn(true);

        operationManager.addPendingDiscovery(target1Id, discoveryType);
        assertTrue(operationManager.hasPendingDiscovery(target1Id, discoveryType));

        when(probeStore.getProbe(probeIdVc)).thenReturn(Optional.of(probeInfoVc1));
        operationManager.onProbeRegistered(probeIdVc, probeInfoVc1);
        // wait for discovery to be queued
        OperationTestUtilities.waitForEvent(
                () -> discoveryQueue.size(discoveryType) > 0
        );
        // simulate operation of transport workers
        final Optional<IDiscoveryQueueElement> optElement = simulateRemoteMediation(discoveryType,
                () -> { });
        assertTrue(optElement.isPresent());

        OperationTestUtilities.waitForEvent(
            () -> operationManager.getInProgressDiscoveryForTarget(target1Id, discoveryType).isPresent()
        );
       assertEquals(optElement.get().getDiscovery(0),
               operationManager.getInProgressDiscoveryForTarget(target1Id, discoveryType).get());
    }

    /**
     * Test that when transport encounters a CommunicationException during discovery,
     * OperationManager fails the discovery. Also, tests that synchronous discovery works.
     *
     * @param discoveryType what type of discovery to run.
     * @throws Exception when OperationManager or transport or RemoteMediationServer throw.
     */
    @Test
    @Parameters({"FULL"})
    public void testTransportThrowsException(DiscoveryType discoveryType) throws Exception {
        doThrow(new CommunicationException("Test Exception")).when(transport2).send(any());
        remoteMediationServer.registerTransport(containerInfo, transport2);
        // set synchronous flag true
        final Optional<Discovery> discovery = operationManager.startDiscovery(target1Id,
                discoveryType, true);
        // Since we made synchronous call, we should've waited for discovery to be launched and
        // returned it.
        assertTrue(discovery.isPresent());
        verify(transport2).addEventHandler(transportListener.capture());
        assertEquals(Status.FAILED, discovery.get().getStatus());
        transportListener.getValue().onClose();
    }

    /**
     * Test that a runtime exception during discovery response processing does not cause
     * us to leave the operation in a state that continues to say it is in progress.
     *
     * @param discoveryType discovery type to test
     * @throws Exception If something goes wrong.
     */
    @Test
    @Parameters({"FULL", "INCREMENTAL"})
    public void testRuntimeExceptionDuringDiscoveryResponse(DiscoveryType discoveryType) throws Exception {
        // This will add an element to the discoveryQueue, but no Discovery will be created yet
        final Discovery discovery = startAndRunDiscovery(target1Id, discoveryType, () -> { });
        final DiscoveryResponse result = DiscoveryResponse.newBuilder()
            .addEntityDTO(entity)
            .build();
        doThrow(RuntimeException.class).when(entityStore)
            .entitiesDiscovered(anyLong(), anyLong(), anyInt(), eq(discoveryType), anyListOf(EntityDTO.class));
        operationManager.notifyDiscoveryResult(discovery, result).get(
                OperationTestUtilities.DISCOVERY_PROCESSING_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        assertFalse(operationManager.getInProgressDiscovery(discovery.getId()).isPresent());
    }

}
