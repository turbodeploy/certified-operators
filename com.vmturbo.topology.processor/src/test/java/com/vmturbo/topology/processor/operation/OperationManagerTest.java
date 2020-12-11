package com.vmturbo.topology.processor.operation;

import static com.vmturbo.topology.processor.db.Tables.ENTITY_ACTION;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyListOf;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import com.google.common.collect.Lists;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;

import org.hamcrest.CoreMatchers;
import org.jooq.DSLContext;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.communication.ITransport;
import com.vmturbo.identity.exceptions.IdentityServiceException;
import com.vmturbo.identity.store.IdentityStore;
import com.vmturbo.kvstore.MapKeyValueStore;
import com.vmturbo.matrix.component.TheMatrix;
import com.vmturbo.platform.common.dto.ActionExecution.ActionExecutionDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO.ActionType;
import com.vmturbo.platform.common.dto.ActionExecution.ActionResponseState;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.NotificationDTO;
import com.vmturbo.platform.common.dto.CommonDTO.NotificationDTO.Severity;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryContextDTO;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryType;
import com.vmturbo.platform.common.dto.Discovery.ErrorDTO;
import com.vmturbo.platform.common.dto.Discovery.ErrorDTO.ErrorSeverity;
import com.vmturbo.platform.common.dto.Discovery.NoChange;
import com.vmturbo.platform.common.dto.Discovery.ValidationResponse;
import com.vmturbo.platform.sdk.common.MediationMessage.ActionRequest;
import com.vmturbo.platform.sdk.common.MediationMessage.ActionResponse;
import com.vmturbo.platform.sdk.common.MediationMessage.ActionResult;
import com.vmturbo.platform.sdk.common.MediationMessage.DiscoveryRequest;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationClientMessage;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationServerMessage;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.platform.sdk.common.MediationMessage.ValidationRequest;
import com.vmturbo.platform.sdk.common.util.NotificationCategoryDTO;
import com.vmturbo.sql.utils.DbCleanupRule;
import com.vmturbo.sql.utils.DbConfigurationRule;
import com.vmturbo.topology.processor.TestIdentityStore;
import com.vmturbo.topology.processor.TestProbeStore;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.OperationStatus.Status;
import com.vmturbo.topology.processor.api.dto.InputField;
import com.vmturbo.topology.processor.api.impl.TargetRESTApi.TargetSpec;
import com.vmturbo.topology.processor.communication.RemoteMediationServer;
import com.vmturbo.topology.processor.controllable.EntityActionDao;
import com.vmturbo.topology.processor.controllable.EntityActionDaoImp;
import com.vmturbo.topology.processor.cost.DiscoveredCloudCostUploader;
import com.vmturbo.topology.processor.db.TopologyProcessor;
import com.vmturbo.topology.processor.db.enums.EntityActionActionType;
import com.vmturbo.topology.processor.db.tables.records.EntityActionRecord;
import com.vmturbo.topology.processor.discoverydumper.BinaryDiscoveryDumper;
import com.vmturbo.topology.processor.discoverydumper.TargetDumpingSettings;
import com.vmturbo.topology.processor.entity.EntityStore;
import com.vmturbo.topology.processor.group.discovery.DiscoveredGroupUploader;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.notification.SystemNotificationProducer;
import com.vmturbo.topology.processor.operation.OperationTestUtilities.TrackingOperationListener;
import com.vmturbo.topology.processor.operation.action.Action;
import com.vmturbo.topology.processor.operation.discovery.Discovery;
import com.vmturbo.topology.processor.operation.discovery.DiscoveryMessageHandler;
import com.vmturbo.topology.processor.operation.validation.Validation;
import com.vmturbo.topology.processor.operation.validation.ValidationMessageHandler;
import com.vmturbo.topology.processor.operation.validation.ValidationResult;
import com.vmturbo.topology.processor.targets.CachingTargetStore;
import com.vmturbo.topology.processor.targets.DerivedTargetParser;
import com.vmturbo.topology.processor.targets.GroupScopeResolver;
import com.vmturbo.topology.processor.targets.KvTargetDao;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetDao;
import com.vmturbo.topology.processor.targets.TargetSpecAttributeExtractor;
import com.vmturbo.topology.processor.targets.TargetStore;
import com.vmturbo.topology.processor.template.DiscoveredTemplateDeploymentProfileUploader;
import com.vmturbo.topology.processor.util.Probes;
import com.vmturbo.topology.processor.workflow.DiscoveredWorkflowUploader;

/**
 * Testing the {@link OperationManager} functionality.
 */
@RunWith(JUnitParamsRunner.class)
public class OperationManagerTest {
    /**
     * Rule to create the DB schema and migrate it.
     */
    @ClassRule
    public static DbConfigurationRule dbConfig = new DbConfigurationRule(TopologyProcessor.TOPOLOGY_PROCESSOR);

    /**
     * Rule to automatically cleanup DB data before each test.
     */
    @Rule
    public DbCleanupRule dbCleanup = dbConfig.cleanupRule();

    private DSLContext dsl = dbConfig.getDslContext();

    private EntityActionDao entityActionDao;

    private final IdentityProvider identityProvider = Mockito.mock(IdentityProvider.class);

    private final TestProbeStore probeStore = new TestProbeStore(identityProvider);

    private final IdentityStore<TopologyProcessorDTO.TargetSpec> targetIdentityStore = new TestIdentityStore<>(
            new TargetSpecAttributeExtractor(probeStore));

    private final TargetDao kvStore = new KvTargetDao(new MapKeyValueStore(), probeStore);

    private final GroupScopeResolver groupScopeResolver = Mockito.mock(GroupScopeResolver.class);

    private final TargetDumpingSettings targetDumpingSettings = Mockito.mock(TargetDumpingSettings.class);

    private final SystemNotificationProducer systemNotificationProducer = Mockito.mock(SystemNotificationProducer.class);

    private final TargetStore targetStore = new CachingTargetStore(kvStore, probeStore,
            targetIdentityStore);

    private final RemoteMediationServer mockRemoteMediationServer = Mockito.mock(RemoteMediationServer.class);

    private final EntityStore entityStore = Mockito.mock(EntityStore.class);

    private final DiscoveredGroupUploader discoveredGroupUploader = Mockito.mock(DiscoveredGroupUploader.class);
    private final DiscoveredWorkflowUploader discoveredWorkflowUploader = Mockito.mock(DiscoveredWorkflowUploader.class);
    private final DiscoveredCloudCostUploader discoveredCloudCostUploader = Mockito.mock(DiscoveredCloudCostUploader.class);

    private TrackingOperationListener operationListener = Mockito.spy(new TrackingOperationListener());

    private DiscoveredTemplateDeploymentProfileUploader discoveredTemplatesUploader = Mockito.mock(DiscoveredTemplateDeploymentProfileUploader.class);

    private DerivedTargetParser derivedTargetParser = Mockito.mock(DerivedTargetParser.class);

    private BinaryDiscoveryDumper binaryDiscoveryDumper =
        Mockito.mock(BinaryDiscoveryDumper.class);

    private OperationManager operationManager;

    private long probeId;
    private long targetId;
    private Target target;

    private static final long ACTIVATE_VM_ID = 100L;
    private static final long DEACTIVATE_VM_ID = 200L;
    private static final long MOVE_SOURCE_ID = 20L;
    private static final long MOVE_DESTINATION_ID = 30L;

    @SuppressWarnings("unchecked")
    private final ITransport<MediationServerMessage, MediationClientMessage> transport =
            Mockito.mock(ITransport.class);

    /**
     * Temporary folder rule.
     */
    @Rule
    public TemporaryFolder testFolder = new TemporaryFolder();

    private final EntityDTO entity = EntityDTO.newBuilder()
            .setEntityType(EntityType.VIRTUAL_MACHINE)
            .setId("vm-1")
            .build();

    /**
     * Initializes the tests.
     *
     * @throws Exception on exceptions occurred
     */
    @Before
    public void setup() throws Exception {
        entityActionDao = new EntityActionDaoImp(dsl, 100, 300,
                360, 360, 360);
        operationManager = new OperationManager(identityProvider, targetStore, probeStore,
            mockRemoteMediationServer, operationListener, entityStore, discoveredGroupUploader,
            discoveredWorkflowUploader, discoveredCloudCostUploader, discoveredTemplatesUploader,
            entityActionDao, derivedTargetParser, groupScopeResolver, targetDumpingSettings, systemNotificationProducer, 10, 10, 10,
            5, 10, 1, 1, TheMatrix.instance(), binaryDiscoveryDumper, false);
        IdentityGenerator.initPrefix(0);
        when(identityProvider.generateOperationId()).thenAnswer((invocation) -> IdentityGenerator.next());

        probeId = IdentityGenerator.next();
        when(identityProvider.getProbeId(any())).thenReturn(probeId);

        System.setProperty("com.vmturbo.keydir", testFolder.newFolder().getAbsolutePath());
        final ProbeInfo probeInfo = Probes.emptyProbe;
        probeStore.registerNewProbe(probeInfo, transport);
        final TargetSpec targetSpec = new TargetSpec(probeId, Collections.singletonList(new InputField("targetId",
            "123",
            Optional.empty())));
        target = targetStore.createTarget(targetSpec.toDto());
        targetId = target.getId();

        when(mockRemoteMediationServer.getMessageHandlerExpirationClock())
                .thenReturn(Clock.systemUTC());

        when(targetDumpingSettings.getDumpsToHold(any())).thenReturn(0);
        doNothing().when(targetDumpingSettings).refreshSettings();
    }

    /**
     * Test Discovery constructor.
     */
    @Test
    public void testDiscoveryObject() {
        LocalDateTime before = LocalDateTime.now();
        Discovery discovery = new Discovery(50, 100, identityProvider);
        LocalDateTime after = LocalDateTime.now();
        testNow(discovery.getStartTime(), before, after);
        Assert.assertNull(discovery.getCompletionTime());
        Assert.assertEquals(50, discovery.getProbeId());
        Assert.assertEquals(100, discovery.getTargetId());
        Assert.assertEquals(Status.IN_PROGRESS, discovery.getStatus());
        Assert.assertTrue(discovery.isInProgress());
        Assert.assertTrue(discovery.getErrors().isEmpty());

        before = LocalDateTime.now();
        discovery.success();
        after = LocalDateTime.now();
        testNow(discovery.getCompletionTime(), before, after);
        Assert.assertEquals(Status.SUCCESS, discovery.getStatus());

        discovery.fail();
        Assert.assertEquals(Status.FAILED, discovery.getStatus());
    }

    private void testNow(LocalDateTime time, LocalDateTime before, LocalDateTime after) {
        Assert.assertTrue(!before.isAfter(time));
        Assert.assertTrue(!time.isAfter(after));
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
        final Discovery discovery = operationManager.startDiscovery(targetId, discoveryType);
        Mockito.verify(mockRemoteMediationServer).sendDiscoveryRequest(eq(target),
                any(DiscoveryRequest.class), any(OperationMessageHandler.class));
        Assert.assertEquals(discovery, operationManager.getInProgressDiscovery(discovery.getId()).get());
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
        final Discovery discovery = operationManager.startDiscovery(targetId, discoveryType);
        Assert.assertEquals(discovery, operationManager.getInProgressDiscoveryForTarget(targetId, discoveryType).get());

        // Make sure that we can still get the discovery after the
        // operation is complete.
        OperationTestUtilities.notifyAndWaitForDiscovery(operationManager, discovery, DiscoveryResponse.getDefaultInstance());

        Assert.assertFalse(operationManager.getInProgressDiscoveryForTarget(targetId, discoveryType).isPresent());
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
        final Discovery discovery = operationManager.startDiscovery(targetId, discoveryType);
        Assert.assertEquals(Optional.empty(),
            operationManager.getLastDiscoveryForTarget(targetId, discoveryType));

        // Make sure that we can still get the discovery after the
        // operation is complete.
        OperationTestUtilities.notifyAndWaitForDiscovery(operationManager, discovery, DiscoveryResponse.getDefaultInstance());

        final Discovery lastDiscovery = operationManager.getLastDiscoveryForTarget(targetId, discoveryType).get();
        Assert.assertEquals(discovery, lastDiscovery);
        Assert.assertEquals(Status.SUCCESS, lastDiscovery.getStatus());
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
        final Discovery discovery = operationManager.startDiscovery(targetId, discoveryType);
        final DiscoveryResponse result = DiscoveryResponse.newBuilder()
                .addEntityDTO(entity)
                .build();

        OperationTestUtilities.notifyAndWaitForDiscovery(operationManager, discovery, result);

        verify(entityStore).entitiesDiscovered(eq(probeId), eq(targetId), eq(discovery.getMediationMessageId()),
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
        final Discovery discovery = operationManager.startDiscovery(targetId, discoveryType);
        // Critical errors applying to the target rather than a specific entity
        // should prevent any EntityDTOs in the discovery from being added to
        // the topology snapshot for the target.
        final DiscoveryResponse result = DiscoveryResponse.newBuilder()
                .addEntityDTO(entity)
                .addErrorDTO(ErrorDTO.newBuilder()
                        .setSeverity(ErrorSeverity.CRITICAL)
                        .setDescription("error"))
                .build();

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
        final Discovery discovery = operationManager.startDiscovery(targetId, discoveryType);
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

        // Wait until we receive notification of the failure
        operationManager.notifyDiscoveryResult(discovery, resultFailure).get(
                OperationTestUtilities.DISCOVERY_PROCESSING_TIMEOUT_SECONDS, TimeUnit.SECONDS);

        // We should have received two notifications - once for start, once for complete
        Assert.assertFalse(operationManager.getInProgressDiscovery(discovery.getId()).isPresent());



        //Success
        final Discovery discovery2 = operationManager.startDiscovery(targetId, discoveryType);
        final DiscoveryResponse resultSuccess = DiscoveryResponse.newBuilder()
                        .addEntityDTO(entity)
                        .addNotification(notification)
                        .build();

        // Wait until we receive notification of the failure
        operationManager.notifyDiscoveryResult(
                discovery2, resultSuccess).get(OperationTestUtilities.DISCOVERY_PROCESSING_TIMEOUT_SECONDS, TimeUnit.SECONDS);

        // We should have received two notifications - once for start, once for complete
        Assert.assertFalse(operationManager.getInProgressDiscovery(discovery2.getId()).isPresent());

    }

    /**
     * Test that a discovery with no chgange gets processed properly.
     *
     * @param discoveryType type of the discovery to test
     * @throws Exception If anything goes wrong.
     */
    @Test
    @Parameters({"FULL", "INCREMENTAL"})
    public void testProcessDiscoveryNoChange(DiscoveryType discoveryType) throws Exception {
        final Discovery discovery = operationManager.startDiscovery(targetId, discoveryType);
        // When the probe responds that nothing has changed (the NoChange message)
        // the code that interacts with the entity store is skipped.
        final DiscoveryResponse result = DiscoveryResponse.newBuilder()
                .setNoChange(NoChange.getDefaultInstance())
                .build();
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
        final Discovery discovery1 = operationManager.startDiscovery(targetId, discoveryType);
        DiscoveryContextDTO contextResponse = DiscoveryContextDTO.newBuilder()
                .putContextEntry("A", "B")
                .build();
        final DiscoveryResponse result = DiscoveryResponse.newBuilder()
                .setDiscoveryContext(contextResponse)
                .build();
        OperationTestUtilities.notifyAndWaitForDiscovery(operationManager, discovery1, result);
        final Discovery discovery2 = operationManager.startDiscovery(targetId, discoveryType);
        ArgumentCaptor<DiscoveryRequest> requestCaptor
            = ArgumentCaptor.forClass(DiscoveryRequest.class);
        Mockito.verify(mockRemoteMediationServer, times(2))
            .sendDiscoveryRequest(eq(target),
                requestCaptor.capture(), any(OperationMessageHandler.class));
        List<DiscoveryRequest> requests = requestCaptor.getAllValues();
        // Verify that the first discovery request contained an empty discovery context
        Assert.assertEquals(DiscoveryContextDTO.getDefaultInstance(),
            requests.get(0).getDiscoveryContext());
        // Verify that the second discovery request contained the discovery context
        // received in the first discovery response
        Assert.assertEquals(contextResponse, requests.get(1).getDiscoveryContext());
    }

    /**
     * Test that discovery fails when entities fail to identify.
     *
     * @param discoveryType type of the discovery to test
     * @throws Exception If anything goes wrong.
     */
    @Test
    @Parameters({"FULL", "INCREMENTAL"})
    public void testProcessDiscoveryFailureIdentification(DiscoveryType discoveryType) throws Exception {
        final Discovery discovery = operationManager.startDiscovery(targetId, discoveryType);
        final DiscoveryResponse result = DiscoveryResponse.newBuilder()
                .addEntityDTO(entity)
                .build();

        // Force an exception on the entitiesDiscovered call.
        final IdentityServiceException exception = Mockito.mock(IdentityServiceException.class);
        Mockito.doThrow(exception)
               .when(entityStore).entitiesDiscovered(anyLong(), anyLong(), anyInt(), eq(discoveryType), any());

        OperationTestUtilities.notifyAndWaitForDiscovery(operationManager, discovery, result);
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
    public void testProcessDiscoveryFailureDoesNotClearPreviousResult(DiscoveryType discoveryType) throws Exception {
        final Discovery discovery = operationManager.startDiscovery(targetId, discoveryType);
        final DiscoveryResponse.Builder responseBuilder = DiscoveryResponse.newBuilder()
                .addEntityDTO(entity);

        OperationTestUtilities.notifyAndWaitForDiscovery(operationManager, discovery, responseBuilder.build());
        verify(entityStore).entitiesDiscovered(eq(probeId), eq(targetId), eq(discovery.getMediationMessageId()),
            eq(discoveryType), eq(Collections.singletonList(entity)));

        final DiscoveryResponse errorResponse = responseBuilder
                .addErrorDTO(ErrorDTO.newBuilder().setSeverity(ErrorSeverity.CRITICAL).setDescription("error"))
                .build();

        final Discovery discovery2 = operationManager.startDiscovery(targetId, discoveryType);
        OperationTestUtilities.notifyAndWaitForDiscovery(operationManager, discovery2, errorResponse);

        // The failed discovery shouldn't have triggered another call to the entity store.
        verify(entityStore).entitiesDiscovered(eq(probeId), eq(targetId), anyInt(), eq(discoveryType),
            eq(Collections.singletonList(entity)));
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
        final Discovery discovery = operationManager.startDiscovery(targetId, discoveryType);
        final DiscoveryResponse response = DiscoveryResponse.newBuilder()
                .addEntityDTO(entity)
                .build();

        OperationTestUtilities.notifyAndWaitForDiscovery(operationManager, discovery, response);

        verify(entityStore, times(1)).entitiesDiscovered(eq(probeId),
                eq(targetId), anyInt(), eq(discoveryType), eq(Collections.singletonList(entity)));

        final Discovery discovery2 = operationManager.startDiscovery(targetId, discoveryType);
        final ArgumentCaptor<DiscoveryMessageHandler> captor = ArgumentCaptor.forClass(
                DiscoveryMessageHandler.class);
        Mockito.verify(mockRemoteMediationServer, Mockito.times(2)).sendDiscoveryRequest(
                Mockito.eq(target), Mockito.any(), captor.capture());
        captor.getAllValues().get(1).onExpiration();
        operationListener.awaitOperation(
                operation -> operation.equals(discovery2) && operation.getCompletionTime() != null);

        // The timeout shouldn't have resulted in another call to entitiesDiscovered.
        verify(entityStore, times(1)).entitiesDiscovered(eq(probeId),
                eq(targetId), anyInt(), eq(discoveryType), eq(Collections.singletonList(entity)));
    }

    /**
     * Test that a timed out validation DOES overwrite the result
     * from a previous successful validation.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testProcessValidationTimeoutClearPreviousResult() throws Exception {
        final Validation validation = operationManager.startValidation(targetId);
        final ValidationResponse response = ValidationResponse.newBuilder().build();
        OperationTestUtilities.notifyAndWaitForValidation(operationManager, validation, response);
        final ValidationResult result = operationManager.getValidationResult(targetId).get();
        Assert.assertEquals(0, result.getErrors().get(ErrorSeverity.CRITICAL).size());

        final Validation validation2 = operationManager.startValidation(targetId);
        final ArgumentCaptor<ValidationMessageHandler> captor = ArgumentCaptor.forClass(
                ValidationMessageHandler.class);
        Mockito.verify(mockRemoteMediationServer, Mockito.times(2)).sendValidationRequest(Mockito.any(),
                Mockito.any(), captor.capture());
        captor.getAllValues().get(1).onExpiration();
        operationListener.awaitOperation(operation -> operation.equals(validation2)
                && operation.getCompletionTime() != null);

        final ValidationResult result2 = operationManager.getValidationResult(targetId).get();
        Assert.assertEquals(1, result2.getErrors().get(ErrorSeverity.CRITICAL).size());
    }

    /**
     * Transport closed when performing validation. Critical error with the message is expected.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testProcessValidationCancelOperation() throws Exception {
        final Validation validation = operationManager.startValidation(targetId);
        Assert.assertTrue(operationManager.getInProgressValidation(validation.getId()).isPresent());
        final ArgumentCaptor<ValidationMessageHandler> captor = ArgumentCaptor.forClass(
                ValidationMessageHandler.class);
        Mockito.verify(mockRemoteMediationServer).sendValidationRequest(Mockito.eq(target),
                Mockito.any(), captor.capture());
        captor.getValue().onTransportClose();
        operationListener.awaitOperation(
                operation -> operation.equals(validation) && operation.getCompletionTime() != null);
        final Map<ErrorSeverity, List<ErrorDTO>> errors =
                        operationManager.getValidationResult(targetId).get().getErrors();
        Assert.assertEquals(1, errors.get(ErrorSeverity.CRITICAL).size());
        final String errorMessage =
                        errors.get(ErrorSeverity.CRITICAL).iterator().next().getDescription();
        Assert.assertThat(errorMessage,
                CoreMatchers.containsString("Communication transport to remote probe closed."));
    }

    /**
     * Transport closed when performin discovery. Critical error with the message is expected.
     *
     * @param discoveryType type of the discovery to test
     * @throws Exception If anything goes wrong.
     */
    @Test
    @Parameters({"FULL", "INCREMENTAL"})
    public void testProcessDiscoveryCancelOperation(DiscoveryType discoveryType) throws Exception {
        final Discovery discovery = operationManager.startDiscovery(targetId, discoveryType);
        Assert.assertTrue(operationManager.getInProgressDiscovery(discovery.getId()).isPresent());
        operationManager.onTargetRemoved(target);
        operationListener.awaitOperation(
                operation -> operation.equals(discovery) && operation.getCompletionTime() != null);
        final List<String> errors = discovery.getErrors();
        Assert.assertEquals(1, errors.size());
        final String errorMessage = errors.iterator().next();
        Assert.assertThat(errorMessage,
                CoreMatchers.containsString("Target " + targetId + " removed."));
    }

    /**
     * Test getting ongoing discoveries when there are none.
     */
    @Test
    public void testGetOngoingDiscoveriesEmpty() {
        Assert.assertTrue(operationManager.getInProgressDiscoveries().isEmpty());
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
        final long discoveryId = operationManager.startDiscovery(targetId, discoveryType).getId();
        Assert.assertEquals(1, operationManager.getInProgressDiscoveries().size());
        final Discovery discovery = operationManager.getInProgressDiscoveries().get(0);

        Assert.assertEquals(discoveryId, discovery.getId());
        Assert.assertEquals(targetId, discovery.getTargetId());
    }

    /**
     * Test starting a validation operation.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testValidateTarget() throws Exception {
        final Validation validation = operationManager.startValidation(targetId);
        Mockito.verify(mockRemoteMediationServer).sendValidationRequest(eq(target),
            any(ValidationRequest.class), any(OperationMessageHandler.class));
        Assert.assertEquals(validation, operationManager.getInProgressValidation(validation.getId()).get());

    }

    /**
     * Test getting ongoing validation by target.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testGetInProgressValidationForTarget() throws Exception {
        final Validation validation = operationManager.startValidation(targetId);
        Assert.assertEquals(validation, operationManager.getInProgressValidationForTarget(targetId).get());

        // Make sure that we can still get the validation after the
        // operation is complete.
        OperationTestUtilities.notifyAndWaitForValidation(operationManager, validation,
            ValidationResponse.getDefaultInstance());

        Assert.assertFalse(operationManager.getInProgressValidationForTarget(targetId).isPresent());
    }

    /**
     * Test getting the validation by target ID.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testGetLastValidationForTarget() throws Exception {
        final Validation validation = operationManager.startValidation(targetId);
        Assert.assertEquals(Optional.empty(),
            operationManager.getLastValidationForTarget(targetId));

        // Make sure that we can still get the validation after the
        // operation is complete.
        OperationTestUtilities.notifyAndWaitForValidation(operationManager, validation,
            ValidationResponse.getDefaultInstance());

        final Validation lastValidation = operationManager.getLastValidationForTarget(targetId).get();
        Assert.assertEquals(validation, lastValidation);
        Assert.assertEquals(Status.SUCCESS, lastValidation.getStatus());
    }

    /**
     * Test that a successful validation is processed appropriately.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testProcessValidationSuccess() throws Exception {
        final Validation validation = operationManager.startValidation(targetId);
        final ValidationResponse result = ValidationResponse.newBuilder()
                .addErrorDTO(ErrorDTO.newBuilder()
                        .setDescription("test")
                        .setSeverity(ErrorSeverity.WARNING))
                .build();
        OperationTestUtilities.notifyAndWaitForValidation(operationManager, validation, result);

        final Optional<ValidationResult> validationResult = operationManager.getValidationResult(targetId);
        Assert.assertTrue(validationResult.isPresent());

        Assert.assertTrue(validationResult.get().isSuccess());

        Assert.assertTrue(validationResult.get().getErrors().get(ErrorSeverity.CRITICAL).isEmpty());

        final List<ErrorDTO> errors = validationResult.get().getErrors().get(ErrorSeverity.WARNING);
        Assert.assertEquals(1, errors.size());
        Assert.assertEquals("test", errors.get(0).getDescription());
    }

    /**
     * Test that a failed validation is processed properly.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testProcessValidationFailure() throws Exception {
        final Validation validation = operationManager.startValidation(targetId);
        final ValidationResponse result = ValidationResponse.newBuilder()
                .addErrorDTO(ErrorDTO.newBuilder()
                        .setSeverity(ErrorSeverity.CRITICAL)
                        .setDescription("error"))
                .build();
        OperationTestUtilities.notifyAndWaitForValidation(operationManager, validation, result);
        final ValidationResult validationResult = operationManager.getValidationResult(targetId).get();
        Assert.assertFalse(validationResult.isSuccess());
        Assert.assertTrue(validationResult.getErrors().get(ErrorSeverity.WARNING).isEmpty());

        final List<ErrorDTO> errors = validationResult.getErrors().get(ErrorSeverity.CRITICAL);
        Assert.assertEquals(1, errors.size());
        Assert.assertEquals("error", errors.get(0).getDescription());
    }

    /**
     * Test getting ongoing validations when there are none.
     */
    @Test
    public void testGetOngoingValidationsEmpty() {
        Assert.assertTrue(operationManager.getAllInProgressValidations().isEmpty());
    }

    /**
     * Test getting an ongoing validation.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testGetOngoingValidation() throws Exception {
        final long validationId = operationManager.startValidation(targetId).getId();
        Assert.assertEquals(1, operationManager.getAllInProgressValidations().size());
        final Operation validation = operationManager.getAllInProgressValidations().get(0);

        Assert.assertEquals(validationId, validation.getId());
        Assert.assertEquals(targetId, validation.getTargetId());
    }

    /**
     * Test that triggering a validation when another is in progress
     * returns the ongoing operation's ID.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testDoubleValidation() throws Exception {
        final long validationId = operationManager.startValidation(targetId).getId();
        Assert.assertEquals(1, operationManager.getAllInProgressValidations().size());
        final long doubleValidationId = operationManager.startValidation(targetId).getId();
        Assert.assertEquals(validationId, doubleValidationId);
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
        long discoveryId = operationManager.addPendingDiscovery(targetId, discoveryType).get().getId();
        Assert.assertEquals(1, operationManager.getInProgressDiscoveries().size());
        Discovery discovery = operationManager.getInProgressDiscoveries().get(0);

        Assert.assertEquals(discoveryId, discovery.getId());
        Assert.assertEquals(targetId, discovery.getTargetId());
        Assert.assertFalse(operationManager.hasPendingDiscovery(targetId, discoveryType));
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
        final Discovery originalDiscovery = operationManager.startDiscovery(targetId, discoveryType);
        final Optional<Discovery> pendingDiscovery = operationManager.addPendingDiscovery(targetId, discoveryType);

        Assert.assertFalse(pendingDiscovery.isPresent());
        Assert.assertTrue(operationManager.hasPendingDiscovery(targetId, discoveryType));
        Assert.assertEquals(1, operationManager.getInProgressDiscoveries().size());

        DiscoveryResponse.Builder responseBuilder = DiscoveryResponse.newBuilder()
                .addEntityDTO(entity);

        OperationTestUtilities.notifyAndWaitForDiscovery(operationManager, originalDiscovery, responseBuilder.build());
        verify(entityStore).entitiesDiscovered(eq(probeId), eq(targetId), anyInt(), eq(discoveryType),
            eq(Collections.singletonList(entity)));

        // After the current discovery completes, the pending discovery should be removed
        // and an actual discovery should be kicked off.
        Assert.assertFalse(operationManager.hasPendingDiscovery(targetId, discoveryType));
        Assert.assertTrue(operationManager.getLastDiscoveryForTarget(targetId, discoveryType).isPresent());
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
        probeStore.removeProbe(probeStore.getProbe(probeId).get());

        Optional<Discovery> discovery = operationManager.addPendingDiscovery(targetId, discoveryType);
        Assert.assertFalse(discovery.isPresent());
        Assert.assertTrue(operationManager.hasPendingDiscovery(targetId, discoveryType));
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
        ProbeInfo probeInfo = probeStore.getProbe(probeId).get();
        probeStore.removeProbe(probeInfo);

        operationManager.addPendingDiscovery(targetId, discoveryType);
        Assert.assertTrue(operationManager.hasPendingDiscovery(targetId, discoveryType));
        Mockito.verify(mockRemoteMediationServer, never()).sendDiscoveryRequest(eq(target),
            any(DiscoveryRequest.class), any(OperationMessageHandler.class));

        probeInfo = Probes.defaultProbe.toBuilder()
            .setIncrementalRediscoveryIntervalSeconds(30)
            .build();
        probeStore.registerNewProbe(probeInfo, transport);
        operationManager.onProbeRegistered(probeId, probeInfo);

        OperationTestUtilities.waitForEvent(
            () -> operationManager.getInProgressDiscoveryForTarget(targetId, discoveryType).isPresent()
        );
        Mockito.verify(mockRemoteMediationServer).sendDiscoveryRequest(any(Target.class),
            any(DiscoveryRequest.class), any(OperationMessageHandler.class));
    }

    /**
     * Test starting an action operation.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testStartAction() throws Exception {
        final Action moveAction = operationManager.requestActions(actionDto(),
                targetId,
                null,
                new HashSet<>(Arrays.asList(MOVE_SOURCE_ID, MOVE_DESTINATION_ID)));
        Mockito.verify(mockRemoteMediationServer).sendActionRequest(any(Target.class),
            any(ActionRequest.class), any(OperationMessageHandler.class));
        Assert.assertTrue(operationManager.getInProgressAction(moveAction.getId()).isPresent());
        Set<Long> moveEntityIds = dsl.selectFrom(ENTITY_ACTION)
                        .where(ENTITY_ACTION.ACTION_TYPE.eq(EntityActionActionType.move))
                        .fetchSet(ENTITY_ACTION.ENTITY_ID);
        Assert.assertTrue(moveEntityIds.size() == 2);
        Assert.assertTrue(moveEntityIds.contains(MOVE_SOURCE_ID) && moveEntityIds.contains(MOVE_DESTINATION_ID));

        final Action activateAction = operationManager.requestActions(
                ActionExecutionDTO.newBuilder(actionDto())
                        .setActionType(ActionType.START)
                        .build(),
               targetId,
               null,
               Collections.singleton(ACTIVATE_VM_ID));
        Assert.assertTrue(operationManager.getInProgressAction(activateAction.getId()).isPresent());
        Set<Long> activateEntityIds = dsl.selectFrom(ENTITY_ACTION)
               .where(ENTITY_ACTION.ACTION_TYPE.eq(EntityActionActionType.activate))
               .fetchSet(ENTITY_ACTION.ENTITY_ID);
        Assert.assertTrue(activateEntityIds.size() == 1);
        Assert.assertTrue(activateEntityIds.contains(ACTIVATE_VM_ID));

        final Action deactivateAction = operationManager.requestActions(
                ActionExecutionDTO.newBuilder(actionDto())
                        .setActionType(ActionType.SUSPEND)
                        .build(),
               targetId,
               null,
               Collections.singleton(DEACTIVATE_VM_ID));
               Assert.assertTrue(operationManager.getInProgressAction(activateAction.getId()).isPresent());
       List<EntityActionRecord> deactivateEntityIds = dsl.selectFrom(ENTITY_ACTION)
               .where(ENTITY_ACTION.ENTITY_ID.eq(DEACTIVATE_VM_ID))
               .fetch();
       Assert.assertTrue(deactivateEntityIds.isEmpty());
    }

    /**
     * Tests process action success.
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testProcessActionSuccess() throws Exception {
        final Action action = operationManager.requestActions(actionDto(),
                targetId,
                null,
                new HashSet<>(Arrays.asList(MOVE_SOURCE_ID, MOVE_DESTINATION_ID)));

        final ActionResult result = ActionResult.newBuilder()
                .setResponse(ActionResponse.newBuilder()
                        .setActionResponseState(ActionResponseState.SUCCEEDED)
                        .setProgress(100)
                        .setResponseDescription("Huzzah!"))
                .build();
        operationManager.notifyActionResult(action, result);

        OperationTestUtilities.waitForAction(operationManager, action);
        Assert.assertEquals(Status.SUCCESS, action.getStatus());
        Set<Long> moveEntityIds = dsl.selectFrom(ENTITY_ACTION)
                .where(ENTITY_ACTION.ACTION_TYPE.eq(EntityActionActionType.move))
                .fetchSet(ENTITY_ACTION.ENTITY_ID);
        Assert.assertTrue(moveEntityIds.size() == 2);
        Assert.assertTrue(moveEntityIds.contains(MOVE_SOURCE_ID) && moveEntityIds.contains(MOVE_DESTINATION_ID));

        final Action activateAction = operationManager.requestActions(
                ActionExecutionDTO.newBuilder(actionDto())
                        .setActionType(ActionType.START)
                        .build(),
                targetId,
                null,
                Collections.singleton(ACTIVATE_VM_ID));
        Assert.assertTrue(operationManager.getInProgressAction(activateAction.getId()).isPresent());
        Set<Long> activateEntityIds = dsl.selectFrom(ENTITY_ACTION)
                .where(ENTITY_ACTION.ACTION_TYPE.eq(EntityActionActionType.activate))
                .fetchSet(ENTITY_ACTION.ENTITY_ID);
        Assert.assertTrue(activateEntityIds.size() == 1);
        Assert.assertTrue(activateEntityIds.contains(ACTIVATE_VM_ID));

        final Action deactivateAction = operationManager.requestActions(
                ActionExecutionDTO.newBuilder(actionDto())
                        .setActionType(ActionType.SUSPEND)
                        .build(),
                targetId,
                null,
                Collections.singleton(DEACTIVATE_VM_ID));
       Assert.assertTrue(operationManager.getInProgressAction(activateAction.getId()).isPresent());
       List<EntityActionRecord> deactivateEntityIds = dsl.selectFrom(ENTITY_ACTION)
                       .where(ENTITY_ACTION.ENTITY_ID.eq(DEACTIVATE_VM_ID))
                       .fetch();
       Assert.assertTrue(deactivateEntityIds.isEmpty());
    }

    /**
     * Tests action discovery failure.
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testActionDiscoveryFailure() throws Exception {
        final Action action = operationManager.requestActions(actionDto(),
                targetId,
                null,
                Collections.singleton(targetId));
        // Critical errors applying to the target rather than a specific entity
        // should prevent any EntityDTOs in the discovery from being added to
        // the topology snapshot for the target.
        final ActionResult result = ActionResult.newBuilder()
                .setResponse(ActionResponse.newBuilder()
                        .setActionResponseState(ActionResponseState.FAILED)
                        .setProgress(0)
                        .setResponseDescription("Boo!"))
                .build();

        operationManager.notifyActionResult(action, result);
        OperationTestUtilities.waitForAction(operationManager, action);

        // Wait until we receive notification of the failure
        OperationTestUtilities.waitForEvent(() ->
            operationListener.getLastNotifiedStatus()
                .map(status -> status == Status.FAILED)
                .orElse(false));

        // We should have received two notifications - once for start, once for complete
        Mockito.verify(operationListener, times(2)).notifyOperationState(action);
    }

    /**
     * Tests process action cancel operation.
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testProcessActionCancelOperation() throws Exception {
        final Action action = operationManager.requestActions(actionDto(),
                targetId,
                null,
                Collections.singleton(targetId));
        Assert.assertTrue(operationManager.getInProgressAction(action.getId()).isPresent());
        operationManager.onTargetRemoved(target);
        OperationTestUtilities.waitForEvent(
                () -> !operationListener.lastStatusMatches(Status.IN_PROGRESS));
        OperationTestUtilities.waitForEvent(
                () -> !operationManager.getInProgressAction(action.getId()).isPresent());

        final List<String> errors = action.getErrors();
        Assert.assertEquals(1, errors.size());
        final String errorMessage = errors.iterator().next();
        Assert.assertThat(errorMessage,
                CoreMatchers.containsString("Target " + targetId + " removed"));

    }

    /**
     * Tests processing action when target is removed.
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testProcessActionTargetRemoval() throws Exception {
        final Target target = targetStore.getTarget(targetId).get();

        final Action action = operationManager.requestActions(actionDto(),
                targetId,
                null,
                Collections.singleton(targetId));
        Assert.assertTrue(operationManager.getInProgressAction(action.getId()).isPresent());
        operationManager.onTargetRemoved(target);
        OperationTestUtilities.waitForAction(operationManager, action);

        final List<String> errors = action.getErrors();
        Assert.assertEquals(1, errors.size());
        final String errorMessage = errors.iterator().next();
        Assert.assertThat(errorMessage,
                CoreMatchers.containsString("Target " + targetId + " removed"));
        Assert.assertFalse(operationManager.getInProgressAction(action.getId()).isPresent());
    }

    /**
     * Tests that expiration is checked.
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void checkForExpiredOperations() throws Exception {
        operationManager.checkForExpiredOperations();

        Mockito.verify(mockRemoteMediationServer).checkForExpiredHandlers();
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
        final Discovery discovery = operationManager.startDiscovery(targetId, discoveryType);
        final DiscoveryResponse result = DiscoveryResponse.newBuilder()
            .addEntityDTO(entity)
            .build();
        doThrow(RuntimeException.class).when(entityStore)
            .entitiesDiscovered(anyLong(), anyLong(), anyInt(), eq(discoveryType), anyListOf(EntityDTO.class));
        operationManager.notifyDiscoveryResult(discovery, result).get(
                OperationTestUtilities.DISCOVERY_PROCESSING_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        Assert.assertFalse(operationManager.getInProgressDiscovery(discovery.getId()).isPresent());
    }

    @Nonnull
    private ActionExecutionDTO actionDto() {
        return ActionExecutionDTO.newBuilder()
                .setActionOid(111L)
                .addAllActionItem(actionItemDtos())
                .setActionType(ActionType.MOVE)
                .setActionState(ActionResponseState.IN_PROGRESS)
                .build();
    }

    private List<ActionItemDTO> actionItemDtos() {
        final EntityDTO target = EntityDTO.newBuilder()
            .setEntityType(EntityType.VIRTUAL_MACHINE)
            .setId("vm")
            .build();

        return Lists.newArrayList(ActionItemDTO.newBuilder()
            .setActionType(ActionType.MOVE)
            .setUuid("test")
            .setTargetSE(target)
            .build());
    }
}
