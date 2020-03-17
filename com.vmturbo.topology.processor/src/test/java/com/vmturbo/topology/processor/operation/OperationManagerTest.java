package com.vmturbo.topology.processor.operation;

import static com.vmturbo.topology.processor.db.Tables.ENTITY_ACTION;
import static org.mockito.Matchers.any;
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

import com.google.common.collect.Lists;

import org.hamcrest.CoreMatchers;
import org.jooq.DSLContext;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.communication.ITransport;
import com.vmturbo.identity.store.IdentityStore;
import com.vmturbo.kvstore.MapKeyValueStore;
import com.vmturbo.matrix.component.TheMatrix;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO.ActionType;
import com.vmturbo.platform.common.dto.ActionExecution.ActionResponseState;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryContextDTO;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse;
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
import com.vmturbo.sql.utils.DbCleanupRule;
import com.vmturbo.sql.utils.DbConfigurationRule;
import com.vmturbo.topology.processor.TestIdentityStore;
import com.vmturbo.topology.processor.TestProbeStore;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.OperationStatus.Status;
import com.vmturbo.topology.processor.api.impl.TargetRESTApi.TargetSpec;
import com.vmturbo.topology.processor.communication.RemoteMediationServer;
import com.vmturbo.topology.processor.controllable.EntityActionDao;
import com.vmturbo.topology.processor.controllable.EntityActionDaoImp;
import com.vmturbo.topology.processor.cost.DiscoveredCloudCostUploader;
import com.vmturbo.topology.processor.db.TopologyProcessor;
import com.vmturbo.topology.processor.db.enums.EntityActionActionType;
import com.vmturbo.topology.processor.db.tables.records.EntityActionRecord;
import com.vmturbo.topology.processor.discoverydumper.TargetDumpingSettings;
import com.vmturbo.topology.processor.entity.EntityStore;
import com.vmturbo.topology.processor.group.discovery.DiscoveredGroupUploader;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.identity.IdentityUninitializedException;
import com.vmturbo.topology.processor.notification.SystemNotificationProducer;
import com.vmturbo.topology.processor.operation.OperationTestUtilities.TrackingOperationListener;
import com.vmturbo.topology.processor.operation.action.Action;
import com.vmturbo.topology.processor.operation.discovery.Discovery;
import com.vmturbo.topology.processor.operation.validation.Validation;
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

    private OperationManager operationManager;

    private long probeId;
    private long targetId;

    private static final long ACTIVATE_VM_ID = 100L;
    private static final long DEACTIVATE_VM_ID = 200L;
    private static final long MOVE_SOURCE_ID = 20L;
    private static final long MOVE_DESTINATION_ID = 30L;

    @SuppressWarnings("unchecked")
    private final ITransport<MediationServerMessage, MediationClientMessage> transport =
            Mockito.mock(ITransport.class);

    @Rule
    public TemporaryFolder testFolder = new TemporaryFolder();

    private final EntityDTO entity = EntityDTO.newBuilder()
            .setEntityType(EntityType.VIRTUAL_MACHINE)
            .setId("vm-1")
            .build();

    @Before
    public void setup() throws Exception {
        entityActionDao = new EntityActionDaoImp(dsl, 100, 300,
                360, 360, 360);
        operationManager = new OperationManager(identityProvider, targetStore, probeStore,
            mockRemoteMediationServer, operationListener, entityStore, discoveredGroupUploader,
            discoveredWorkflowUploader, discoveredCloudCostUploader, discoveredTemplatesUploader,
            entityActionDao, derivedTargetParser, groupScopeResolver, targetDumpingSettings, systemNotificationProducer, 10, 10, 10,
            5, 1, 1, TheMatrix.instance());
        IdentityGenerator.initPrefix(0);
        when(identityProvider.generateOperationId()).thenAnswer((invocation) -> IdentityGenerator.next());

        probeId = IdentityGenerator.next();
        when(identityProvider.getProbeId(any())).thenReturn(probeId);

        System.setProperty("com.vmturbo.keydir", testFolder.newFolder().getAbsolutePath());
        final ProbeInfo probeInfo = Probes.emptyProbe;
        probeStore.registerNewProbe(probeInfo, transport);
        final TargetSpec target = new TargetSpec(probeId, Collections.emptyList());
        targetId = targetStore.createTarget(target.toDto()).getId();

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
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testDiscoverTarget() throws Exception {
        final Discovery discovery = operationManager.startDiscovery(targetId);
        Mockito.verify(mockRemoteMediationServer).sendDiscoveryRequest(eq(probeId),
                any(DiscoveryRequest.class), any(OperationMessageHandler.class));
        Assert.assertEquals(discovery, operationManager.getInProgressDiscovery(discovery.getId()).get());
    }

    /**
     * Test getting ongoing discovery by target.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testGetInProgressDiscoveryForTarget() throws Exception {
        final Discovery discovery = operationManager.startDiscovery(targetId);
        Assert.assertEquals(discovery, operationManager.getInProgressDiscoveryForTarget(targetId).get());

        // Make sure that we can still get the discovery after the
        // operation is complete.
        operationManager.notifyDiscoveryResult(discovery, DiscoveryResponse.getDefaultInstance());
        OperationTestUtilities.waitForDiscovery(operationManager, discovery);

        Assert.assertFalse(operationManager.getInProgressDiscoveryForTarget(targetId).isPresent());
    }

    /**
     * Test getting last discovery by target.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testGetLastDiscoveryForTarget() throws Exception {
        final Discovery discovery = operationManager.startDiscovery(targetId);
        Assert.assertEquals(Optional.empty(),
            operationManager.getLastDiscoveryForTarget(targetId));

        // Make sure that we can still get the discovery after the
        // operation is complete.
        operationManager.notifyDiscoveryResult(discovery, DiscoveryResponse.getDefaultInstance());
        OperationTestUtilities.waitForDiscovery(operationManager, discovery);

        final Discovery lastDiscovery = operationManager.getLastDiscoveryForTarget(targetId).get();
        Assert.assertEquals(discovery, lastDiscovery);
        Assert.assertEquals(Status.SUCCESS, lastDiscovery.getStatus());
    }

    /**
     * Test that a completed discovery gets processed properly.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testProcessDiscoverySuccess() throws Exception {
        final Discovery discovery = operationManager.startDiscovery(targetId);
        final DiscoveryResponse result = DiscoveryResponse.newBuilder()
                .addEntityDTO(entity)
                .build();

        operationManager.notifyDiscoveryResult(discovery, result);

        OperationTestUtilities.waitForDiscovery(operationManager, discovery);
        verify(entityStore).entitiesDiscovered(eq(probeId), eq(targetId),
                eq(Collections.singletonList(entity)));
    }

    /**
     * Test that a failed discovery gets processed properly.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testProcessDiscoveryFailure() throws Exception {
        final Discovery discovery = operationManager.startDiscovery(targetId);
        // Critical errors applying to the target rather than a specific entity
        // should prevent any EntityDTOs in the discovery from being added to
        // the topology snapshot for the target.
        final DiscoveryResponse result = DiscoveryResponse.newBuilder()
                .addEntityDTO(entity)
                .addErrorDTO(ErrorDTO.newBuilder()
                        .setSeverity(ErrorSeverity.CRITICAL)
                        .setDescription("error"))
                .build();

        operationManager.notifyDiscoveryResult(discovery, result);
        OperationTestUtilities.waitForDiscovery(operationManager, discovery);
        Mockito.verify(entityStore, never()).entitiesDiscovered(anyLong(), anyLong(), any());
    }

    /**
     * Test that a discovery with no chgange gets processed properly.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testProcessDiscoveryNoChange() throws Exception {
        final Discovery discovery = operationManager.startDiscovery(targetId);
        // When the probe responds that nothing has changed (the NoChange message)
        // the code that interacts with the entity store is skipped.
        final DiscoveryResponse result = DiscoveryResponse.newBuilder()
                .setNoChange(NoChange.getDefaultInstance())
                .build();
        operationManager.notifyDiscoveryResult(discovery, result);
        OperationTestUtilities.waitForDiscovery(operationManager, discovery);
        Mockito.verify(entityStore, never()).entitiesDiscovered(anyLong(), anyLong(), any());
    }

    /**
     * Test that a discovery context received in the last discovery response is
     * placed in the subsequent discovery request.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testSendDiscoveryContext() throws Exception {
        final Discovery discovery1 = operationManager.startDiscovery(targetId);
        DiscoveryContextDTO contextResponse = DiscoveryContextDTO.newBuilder()
                .putContextEntry("A", "B")
                .build();
        final DiscoveryResponse result = DiscoveryResponse.newBuilder()
                .setDiscoveryContext(contextResponse)
                .build();
        operationManager.notifyDiscoveryResult(discovery1, result);
        OperationTestUtilities.waitForDiscovery(operationManager, discovery1);
        final Discovery discovery2 = operationManager.startDiscovery(targetId);
        ArgumentCaptor<DiscoveryRequest> requestCaptor
            = ArgumentCaptor.forClass(DiscoveryRequest.class);
        Mockito.verify(mockRemoteMediationServer, times(2)).sendDiscoveryRequest(eq(probeId),
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
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testProcessDiscoveryFailureIdentification() throws Exception {
        final Discovery discovery = operationManager.startDiscovery(targetId);
        final DiscoveryResponse result = DiscoveryResponse.newBuilder()
                .addEntityDTO(entity)
                .build();

        // Force an exception on the entitiesDiscovered call.
        final IdentityUninitializedException exception = Mockito.mock(IdentityUninitializedException.class);
        Mockito.doThrow(exception)
               .when(entityStore).entitiesDiscovered(anyLong(), anyLong(), any());

        operationManager.notifyDiscoveryResult(discovery, result);
        OperationTestUtilities.waitForDiscovery(operationManager, discovery);
    }

    /**
     * Test that a failed discovery does not overwrite the topology
     * from the previous successful discovery.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testProcessDiscoveryFailureDoesNotClearPreviousResult() throws Exception {
        final Discovery discovery = operationManager.startDiscovery(targetId);
        final DiscoveryResponse.Builder responseBuilder = DiscoveryResponse.newBuilder()
                .addEntityDTO(entity);

        operationManager.notifyDiscoveryResult(discovery, responseBuilder.build());
        OperationTestUtilities.waitForDiscovery(operationManager, discovery);
        verify(entityStore).entitiesDiscovered(eq(probeId), eq(targetId),
                eq(Collections.singletonList(entity)));

        final DiscoveryResponse errorResponse = responseBuilder
                .addErrorDTO(ErrorDTO.newBuilder().setSeverity(ErrorSeverity.CRITICAL).setDescription("error"))
                .build();

        final Discovery discovery2 = operationManager.startDiscovery(targetId);
        operationManager.notifyDiscoveryResult(discovery2, (errorResponse));
        OperationTestUtilities.waitForDiscovery(operationManager, discovery);

        // The failed discovery shouldn't have triggered another call to the entity store.
        verify(entityStore).entitiesDiscovered(eq(probeId), eq(targetId),
                eq(Collections.singletonList(entity)));
    }

    /**
     * Test that a timed out discovery does not overwrite the topology
     * from the previous successful discovery.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testProcessDiscoveryTimeoutDoesNotClearPreviousResult() throws Exception {
        final Discovery discovery = operationManager.startDiscovery(targetId);
        final DiscoveryResponse response = DiscoveryResponse.newBuilder()
                .addEntityDTO(entity)
                .build();

        operationManager.notifyDiscoveryResult(discovery, response);
        OperationTestUtilities.waitForDiscovery(operationManager, discovery);

        verify(entityStore, times(1)).entitiesDiscovered(eq(probeId),
                eq(targetId), eq(Collections.singletonList(entity)));

        final Discovery discovery2 = operationManager.startDiscovery(targetId);
        operationManager.notifyTimeout(discovery2, 1);
        OperationTestUtilities.waitForDiscovery(operationManager, discovery2);

        // The timeout shouldn't have resulted in another call to entitiesDiscovered.
        verify(entityStore, times(1)).entitiesDiscovered(eq(probeId),
                eq(targetId), eq(Collections.singletonList(entity)));
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
        operationManager.notifyValidationResult(validation, response);
        OperationTestUtilities.waitForValidation(operationManager, validation);
        final ValidationResult result = operationManager.getValidationResult(targetId).get();
        Assert.assertEquals(0, result.getErrors().get(ErrorSeverity.CRITICAL).size());

        final Validation validation2 = operationManager.startValidation(targetId);
        operationManager.notifyTimeout(validation2, 1);
        OperationTestUtilities.waitForValidation(operationManager, validation2);
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
        operationManager.notifyOperationCancelled(validation, "transport closed");
        OperationTestUtilities.waitForEvent(operationManager, operationManager -> operationManager
                        .getValidationResult(targetId).isPresent());
        final Map<ErrorSeverity, List<ErrorDTO>> errors =
                        operationManager.getValidationResult(targetId).get().getErrors();
        Assert.assertEquals(1, errors.get(ErrorSeverity.CRITICAL).size());
        final String errorMessage =
                        errors.get(ErrorSeverity.CRITICAL).iterator().next().getDescription();
        Assert.assertThat(errorMessage, CoreMatchers.containsString("transport closed"));
    }

    /**
     * Transport closed when performin discovery. Critical error with the message is expected.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testProcessDiscoveryCancelOperation() throws Exception {
        final Discovery discovery = operationManager.startDiscovery(targetId);
        Assert.assertTrue(operationManager.getInProgressDiscovery(discovery.getId()).isPresent());
        operationManager.notifyOperationCancelled(discovery, "Transport closed");
        OperationTestUtilities.waitForEvent(operationListener,
                        listener -> !listener.lastStatusMatches(Status.IN_PROGRESS));
        final List<String> errors = discovery.getErrors();
        Assert.assertEquals(1, errors.size());
        final String errorMessage = errors.iterator().next();
        Assert.assertThat(errorMessage, CoreMatchers.containsString("Transport closed"));
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
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testGetOngoingDiscoveries() throws Exception {
        final long discoveryId = operationManager.startDiscovery(targetId).getId();
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
        Mockito.verify(mockRemoteMediationServer).sendValidationRequest(eq(probeId),
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
        operationManager.notifyValidationResult(validation,
                ValidationResponse.getDefaultInstance());
        OperationTestUtilities.waitForValidation(operationManager, validation);

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
        operationManager.notifyValidationResult(validation,
                ValidationResponse.getDefaultInstance());
        OperationTestUtilities.waitForValidation(operationManager, validation);

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
        operationManager.notifyValidationResult(validation, result);
        OperationTestUtilities.waitForValidation(operationManager, validation);
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
        operationManager.notifyValidationResult(validation, result);
        OperationTestUtilities.waitForValidation(operationManager, validation);
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

    @Test
    public void testPendingDiscoverNoOngoing() throws Exception {
        long discoveryId = operationManager.addPendingDiscovery(targetId).get().getId();
        Assert.assertEquals(1, operationManager.getInProgressDiscoveries().size());
        Discovery discovery = operationManager.getInProgressDiscoveries().get(0);

        Assert.assertEquals(discoveryId, discovery.getId());
        Assert.assertEquals(targetId, discovery.getTargetId());
        Assert.assertFalse(operationManager.hasPendingDiscovery(targetId));
    }

    @Test
    public void testPendingDiscoverWithOngoing() throws Exception {
        final Discovery originalDiscovery = operationManager.startDiscovery(targetId);
        final Optional<Discovery> pendingDiscovery = operationManager.addPendingDiscovery(targetId);

        Assert.assertFalse(pendingDiscovery.isPresent());
        Assert.assertTrue(operationManager.hasPendingDiscovery(targetId));
        Assert.assertEquals(1, operationManager.getInProgressDiscoveries().size());

        DiscoveryResponse.Builder responseBuilder = DiscoveryResponse.newBuilder()
                .addEntityDTO(entity);

        operationManager.notifyDiscoveryResult(originalDiscovery, responseBuilder.build());
        OperationTestUtilities.waitForDiscovery(operationManager, originalDiscovery);
        verify(entityStore).entitiesDiscovered(eq(probeId), eq(targetId),
                eq(Collections.singletonList(entity)));


        // After the current discovery completes, the pending discovery should be removed
        // and an actual discovery should be kicked off.
        Assert.assertFalse(operationManager.hasPendingDiscovery(targetId));
        Assert.assertTrue(operationManager.getLastDiscoveryForTarget(targetId).isPresent());
    }

    @Test
    public void testPendingDiscoverWithUnregisteredProbe() throws Exception {
        probeStore.removeProbe(probeStore.getProbe(probeId).get());

        Optional<Discovery> discovery = operationManager.addPendingDiscovery(targetId);
        Assert.assertFalse(discovery.isPresent());
        Assert.assertTrue(operationManager.hasPendingDiscovery(targetId));
    }

    @Test
    public void testOnProbeRegisteredActivatesPendingDiscoveries() throws Exception {
        ProbeInfo probeInfo = probeStore.getProbe(probeId).get();
        probeStore.removeProbe(probeInfo);

        operationManager.addPendingDiscovery(targetId);
        Assert.assertTrue(operationManager.hasPendingDiscovery(targetId));
        Mockito.verify(mockRemoteMediationServer, never()).sendDiscoveryRequest(eq(probeId),
            any(DiscoveryRequest.class), any(OperationMessageHandler.class));

        probeStore.registerNewProbe(probeInfo, transport);
        operationManager.onProbeRegistered(probeId, probeInfo);

        OperationTestUtilities.waitForEvent(
            operationManager,
            operationManager -> operationManager.getInProgressDiscoveryForTarget(targetId).isPresent()
        );
        Mockito.verify(mockRemoteMediationServer).sendDiscoveryRequest(eq(probeId),
            any(DiscoveryRequest.class), any(OperationMessageHandler.class));
    }

    /**
     * Test starting an action operation.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testStartAction() throws Exception {
        final List<ActionItemDTO> actionItemDtos = actionItemDtos();

        final Action moveAction = operationManager.requestActions(0,
                targetId,
                null,
                ActionType.MOVE,
                actionItemDtos,
                new HashSet<>(Arrays.asList(MOVE_SOURCE_ID, MOVE_DESTINATION_ID)),
                Optional.empty());
        Mockito.verify(mockRemoteMediationServer).sendActionRequest(eq(probeId),
            any(ActionRequest.class), any(OperationMessageHandler.class));
        Assert.assertTrue(operationManager.getInProgressAction(moveAction.getId()).isPresent());
        Set<Long> moveEntityIds = dsl.selectFrom(ENTITY_ACTION)
                        .where(ENTITY_ACTION.ACTION_TYPE.eq(EntityActionActionType.move))
                        .fetchSet(ENTITY_ACTION.ENTITY_ID);
        Assert.assertTrue(moveEntityIds.size() == 2);
        Assert.assertTrue(moveEntityIds.contains(MOVE_SOURCE_ID) && moveEntityIds.contains(MOVE_DESTINATION_ID));

        final Action activateAction = operationManager.requestActions(0,
               targetId,
               null,
               ActionType.START,
               actionItemDtos,
               Collections.singleton(ACTIVATE_VM_ID),
               Optional.empty());
        Assert.assertTrue(operationManager.getInProgressAction(activateAction.getId()).isPresent());
        Set<Long> activateEntityIds = dsl.selectFrom(ENTITY_ACTION)
               .where(ENTITY_ACTION.ACTION_TYPE.eq(EntityActionActionType.activate))
               .fetchSet(ENTITY_ACTION.ENTITY_ID);
        Assert.assertTrue(activateEntityIds.size() == 1);
        Assert.assertTrue(activateEntityIds.contains(ACTIVATE_VM_ID));

        final Action deactivateAction = operationManager.requestActions(0,
               targetId,
               null,
               ActionType.SUSPEND,
               actionItemDtos,
               Collections.singleton(DEACTIVATE_VM_ID),
               Optional.empty());
               Assert.assertTrue(operationManager.getInProgressAction(activateAction.getId()).isPresent());
       List<EntityActionRecord> deactivateEntityIds = dsl.selectFrom(ENTITY_ACTION)
               .where(ENTITY_ACTION.ENTITY_ID.eq(DEACTIVATE_VM_ID))
               .fetch();
       Assert.assertTrue(deactivateEntityIds.isEmpty());
    }

    @Test
    public void testProcessActionSuccess() throws Exception {
        final List<ActionItemDTO> actionItemDtos = actionItemDtos();

        final Action action = operationManager.requestActions(0,
                targetId,
                null,
                ActionType.MOVE,
                actionItemDtos,
                new HashSet<>(Arrays.asList(MOVE_SOURCE_ID, MOVE_DESTINATION_ID)),
                Optional.empty());

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

        final Action activateAction = operationManager.requestActions(0,
                targetId,
                null,
                ActionType.START,
                actionItemDtos,
                Collections.singleton(ACTIVATE_VM_ID),
                Optional.empty());
        Assert.assertTrue(operationManager.getInProgressAction(activateAction.getId()).isPresent());
        Set<Long> activateEntityIds = dsl.selectFrom(ENTITY_ACTION)
                .where(ENTITY_ACTION.ACTION_TYPE.eq(EntityActionActionType.activate))
                .fetchSet(ENTITY_ACTION.ENTITY_ID);
        Assert.assertTrue(activateEntityIds.size() == 1);
        Assert.assertTrue(activateEntityIds.contains(ACTIVATE_VM_ID));

        final Action deactivateAction = operationManager.requestActions(0,
                targetId,
                null,
                ActionType.SUSPEND,
                actionItemDtos,
                Collections.singleton(DEACTIVATE_VM_ID),
                Optional.empty());
       Assert.assertTrue(operationManager.getInProgressAction(activateAction.getId()).isPresent());
       List<EntityActionRecord> deactivateEntityIds = dsl.selectFrom(ENTITY_ACTION)
                       .where(ENTITY_ACTION.ENTITY_ID.eq(DEACTIVATE_VM_ID))
                       .fetch();
       Assert.assertTrue(deactivateEntityIds.isEmpty());
    }

    @Test
    public void testActionDiscoveryFailure() throws Exception {
        final List<ActionItemDTO> actionItemDtos = actionItemDtos();

        final Action action = operationManager.requestActions(0,
                targetId,
                null,
                ActionType.MOVE,
                actionItemDtos,
                Collections.singleton(targetId),
                Optional.empty());
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
        OperationTestUtilities.waitForEvent(operationListener, listener ->
            listener.getLastNotifiedStatus()
                .map(status -> status == Status.FAILED)
                .orElse(false));

        // We should have received two notifications - once for start, once for complete
        Mockito.verify(operationListener, times(2)).notifyOperationState(action);
    }

    @Test
    public void testProcessActionCancelOperation() throws Exception {
        final List<ActionItemDTO> actionItemDtos = actionItemDtos();

        final Action action = operationManager.requestActions(0,
                targetId,
                null,
                ActionType.MOVE,
                actionItemDtos,
                Collections.singleton(targetId),
                Optional.empty());
        Assert.assertTrue(operationManager.getInProgressAction(action.getId()).isPresent());
        operationManager.notifyOperationCancelled(action, "Transport closed");
        OperationTestUtilities.waitForEvent(operationListener, listener -> !listener.lastStatusMatches(Status.IN_PROGRESS));

        final List<String> errors = action.getErrors();
        Assert.assertEquals(1, errors.size());
        final String errorMessage = errors.iterator().next();
        Assert.assertThat(errorMessage, CoreMatchers.containsString("Transport closed"));
        Assert.assertFalse(operationManager.getInProgressAction(action.getId()).isPresent());

        // Timing out the handler after cancelling the operation should not override the previously set error.
        operationManager.notifyTimeout(action, 10);
        Assert.assertThat(action.getErrors().iterator().next(),
            CoreMatchers.containsString("Transport closed"));
    }

    @Test
    public void testProcessActionTargetRemoval() throws Exception {
        final List<ActionItemDTO> actionItemDtos = actionItemDtos();
        final Target target = targetStore.getTarget(targetId).get();

        final Action action = operationManager.requestActions(0,
                targetId,
                null,
                ActionType.MOVE,
                actionItemDtos,
                Collections.singleton(targetId),
                Optional.empty());
        Assert.assertTrue(operationManager.getInProgressAction(action.getId()).isPresent());
        operationManager.onTargetRemoved(target);
        OperationTestUtilities.waitForAction(operationManager, action);

        final List<String> errors = action.getErrors();
        Assert.assertEquals(1, errors.size());
        final String errorMessage = errors.iterator().next();
        Assert.assertThat(errorMessage, CoreMatchers.containsString("Target removed."));
        Assert.assertFalse(operationManager.getInProgressAction(action.getId()).isPresent());
    }

    @Test
    public void checkForExpiredOperations() throws Exception {
        operationManager.checkForExpiredOperations();

        Mockito.verify(mockRemoteMediationServer).checkForExpiredHandlers();
    }

    /**
     * Test that a runtime exception during discovery response processing does not cause
     * us to leave the operation in a state that continues to say it is in progress.
     *
     * @throws Exception If something goes wrong.
     */
    @Test
    public void testRuntimeExceptionDuringDiscoveryResponse() throws Exception {
        final Discovery discovery = operationManager.startDiscovery(targetId);
        final DiscoveryResponse result = DiscoveryResponse.newBuilder()
            .addEntityDTO(entity)
            .build();
        doThrow(RuntimeException.class).when(entityStore)
            .entitiesDiscovered(anyLong(), anyLong(), anyListOf(EntityDTO.class));
        operationManager.notifyDiscoveryResult(discovery, result);

        OperationTestUtilities.waitForDiscovery(operationManager, discovery);
        Assert.assertFalse(operationManager.getInProgressDiscovery(discovery.getId()).isPresent());
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
