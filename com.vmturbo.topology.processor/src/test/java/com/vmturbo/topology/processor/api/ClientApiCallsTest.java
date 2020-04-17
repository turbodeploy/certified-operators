package com.vmturbo.topology.processor.api;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

import org.hamcrest.CoreMatchers;
import org.hamcrest.Matcher;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.mockito.AdditionalAnswers;
import org.mockito.ArgumentCaptor;
import org.mockito.Matchers;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.action.ActionDTO.Move;
import com.vmturbo.common.protobuf.action.ActionNotificationDTO.ActionFailure;
import com.vmturbo.common.protobuf.action.ActionNotificationDTO.ActionProgress;
import com.vmturbo.common.protobuf.action.ActionNotificationDTO.ActionSuccess;
import com.vmturbo.common.protobuf.topology.ActionExecution.ExecuteActionRequest;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Builder;
import com.vmturbo.platform.common.dto.ActionExecution.ActionResponseState;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CommodityBought;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.Discovery.AccountDefEntry;
import com.vmturbo.platform.common.dto.Discovery.CustomAccountDefEntry;
import com.vmturbo.platform.common.dto.Discovery.CustomAccountDefEntry.PrimitiveValue;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryType;
import com.vmturbo.platform.common.dto.Discovery.ValidationResponse;
import com.vmturbo.platform.sdk.common.MediationMessage;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationClientMessage;
import com.vmturbo.platform.sdk.common.util.SDKUtil;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.TargetSpec;
import com.vmturbo.topology.processor.api.dto.InputField;
import com.vmturbo.topology.processor.conversions.SdkToTopologyEntityConverter;
import com.vmturbo.topology.processor.entity.EntityStore;
import com.vmturbo.topology.processor.identity.IdentityMetadataMissingException;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.identity.IdentityProviderException;
import com.vmturbo.topology.processor.identity.IdentityUninitializedException;
import com.vmturbo.topology.processor.operation.action.ActionMessageHandler;
import com.vmturbo.topology.processor.probes.AccountValueAdaptor;
import com.vmturbo.topology.processor.probes.ProbeException;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.targets.GroupScopeResolver;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetNotFoundException;
import com.vmturbo.topology.processor.targets.TargetStore;
import com.vmturbo.topology.processor.util.Probes;

/**
 * Tests for client side calls for {@link TopologyProcessor}.
 */
public class ClientApiCallsTest extends AbstractApiCallsTest {

    private ProbeStore probeStore;
    private TargetStore targetStore;
    private EntityStore entityStore;
    private IdentityProvider identityProviderSpy;
    private GroupScopeResolver groupScopeResolver;
    // For mocking the responses for remote calls to the Repository service
    private FakeRepositoryClient repositoryClientFake;

    private static final String FIELD_NAME = FakeRemoteMediation.TGT_ID;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Rule
    public TemporaryFolder testFolder = new TemporaryFolder();

    @Before
    public void initClient() throws Exception {
        System.setProperty("com.vmturbo.keydir", testFolder.newFolder().getAbsolutePath());
        probeStore = integrationTestServer.getBean(ProbeStore.class);
        targetStore = integrationTestServer.getBean(TargetStore.class);
        entityStore = integrationTestServer.getBean(EntityStore.class);
        identityProviderSpy = integrationTestServer.getBean(IdentityProvider.class);
        repositoryClientFake = integrationTestServer.getBean(FakeRepositoryClient.class);
        groupScopeResolver = Mockito.mock(GroupScopeResolver.class);
        Mockito.when(groupScopeResolver.processGroupScope(Matchers.any(), Matchers.any(), Matchers.any()))
                .then(AdditionalAnswers.returnsSecondArg());
    }

    /**
     * Tests retrieval of all the probes.
     *
     * @throws Exception if exception occured.
     */
    @Test
    public void testGetAllProbes() throws Exception {
        Assert.assertEquals(Collections.emptySet(), getTopologyProcessor().getAllProbes());
        assertEquals(Collections.emptyMap(), getTopologyProcessor().getAllProbes());
        final long probe1 = createProbe();
        Assert.assertEquals(Collections.singleton(probe1), getTopologyProcessor().getAllProbes()
                        .stream().map(pi -> pi.getId()).collect(Collectors.toSet()));
        assertEquals(probeStore.getProbes(), getTopologyProcessor().getAllProbes());
        final long probe2 = createProbe();
        Assert.assertEquals(new HashSet<>(Arrays.asList(probe1, probe2)), getTopologyProcessor()
                        .getAllProbes().stream().map(pi -> pi.getId()).collect(Collectors.toSet()));
        assertEquals(probeStore.getProbes(), getTopologyProcessor().getAllProbes());
    }

    /**
     * Tests retrieval of existing probe.
     *
     * @throws Exception if exception occured.
     */
    @Test
    public void testGetExistingProbe() throws Exception {
        final long probe = createProbe();
        final ProbeInfo actual = getTopologyProcessor().getProbe(probe);
        Assert.assertEquals(probe, actual.getId());
        assertEquals(probeStore.getProbe(probe).get(), actual);
    }

    /**
     * Tests retrieval of absent probe. TopologyProcessor is expected to throw
     * {@link TopologyProcessorException}.
     *
     * @throws Exception if exception occured.
     */
    @Test
    public void testGetAbsentProbe() throws Exception {
        final long probeId = -1;
        expectExceptionForId(probeId, "not found");
        getTopologyProcessor().getProbe(probeId);
    }

    /**
     * Tests retrieval of all the targets.
     *
     * @throws Exception if exception occured.
     */
    @Test
    public void testGetAllTargets() throws Exception {
        Assert.assertEquals(Collections.emptySet(), getTopologyProcessor().getAllTargets());

        final long probeId = createProbe();
        final TargetSpec targetSpec = TargetSpec.newBuilder().setProbeId(probeId).build();
        final Target target = targetStore.createTarget(targetSpec);

        final Set<TargetInfo> targetInfos = getTopologyProcessor().getAllTargets();
        Assert.assertEquals(1, targetInfos.size());
        assertEquals(wrapTarget(target), targetInfos.iterator().next());
    }

    /**
     * Tests getting target by id.
     *
     * @throws Exception if exception occured.
     */
    @Test
    public void testGetTarget() throws Exception {
        Assert.assertEquals(Collections.emptySet(), getTopologyProcessor().getAllTargets());
        final long probeId = createProbe();
        final TargetSpec targetSpec = TargetSpec.newBuilder().setProbeId(probeId).build();
        final Target target = targetStore.createTarget(targetSpec);

        final TargetInfo targetInfo = getTopologyProcessor().getTarget(target.getId());
        assertEquals(wrapTarget(target), targetInfo);

        expectedException.expect(TopologyProcessorException.class);
        expectedException.expectMessage("not found");
        getTopologyProcessor().getTarget(target.getId() + 1);
    }

    /**
     * Tests adding a target.
     *
     * @throws Exception if exception occured.
     */
    @Test
    public void testAddTarget() throws Exception {
        final long probeId = createProbe();
        final AccountValue original = new InputField(FIELD_NAME, "fieldValue", Optional.empty());
        final TargetData data = Mockito.mock(TargetData.class);
        Mockito.when(data.getAccountData()).thenReturn(Collections.singleton(original));
        final long targetId = getTopologyProcessor().addTarget(probeId, data);
        final Optional<Target> target = targetStore.getTarget(targetId);
        Assert.assertTrue(target.isPresent());
        final Collection<AccountValue> actual = target.get().createTargetInfo().getAccountData();
        Assert.assertEquals(Collections.singleton(original), new HashSet<>(actual));
    }

    /**
     * Tests adding an invalid target.
     *
     * @throws Exception if exception occured.
     */
    @Test
    public void testAddInvalidTarget() throws Exception {
        final long probeId = createProbe();
        final AccountValue field1 = new InputField(FIELD_NAME + 1, "fieldValue", Optional.empty());
        final AccountValue field2 = new InputField(FIELD_NAME + 2, "fieldValue", Optional.empty());
        final TargetData data = Mockito.mock(TargetData.class);
        Mockito.when(data.getAccountData())
                        .thenReturn(new HashSet<>(Arrays.asList(field1, field2)));
        expectedException.expect(TopologyProcessorException.class);
        expectedException.expectMessage(
                        CoreMatchers.allOf(CoreMatchers.containsString(field1.getName()),
                                        CoreMatchers.containsString(field2.getName())));
        getTopologyProcessor().addTarget(probeId, data);
    }

    /**
     * Creates a target with one mandatory field, filled with the specified {@code id} in account
     * values.
     *
     * @param probeId probe id to register target to
     * @param id string id to be used later for verifications
     * @return target id
     * @throws Exception on exceptions occurred
     */
    private long createTarget(long probeId, String id) throws Exception {
        return targetStore.createTarget(createTargetSpec(probeId, id)).getId();
    }

    /**
     * Creates target spec for the specified probe with a specified id, put into account values.
     *
     * @param probeId probe id
     * @param id id to put into account values.
     * @return target spec
     */
    private TopologyProcessorDTO.TargetSpec createTargetSpec(long probeId, String id) {
        final TopologyProcessorDTO.TargetSpec spec = TopologyProcessorDTO.TargetSpec.newBuilder().setProbeId(probeId)
                        .addAllAccountValue(createAccountValue(id)).build();
        return spec;
    }

    private Set<TopologyProcessorDTO.AccountValue> createAccountValue(String id) {
        final TopologyProcessorDTO.AccountValue account = TopologyProcessorDTO.AccountValue.newBuilder()
                        .setKey(FIELD_NAME).setStringValue(id).build();
        return Collections.singleton(account);
    }

    /**
     * Tests legal target removal.
     *
     * @throws Exception on exceptions occur
     */
    @Test
    public void testRemoveTarget() throws Exception {
        final long probeId = createProbe();
        final long targetId = createTarget(probeId, "1");
        Assert.assertTrue(targetStore.getTarget(targetId).isPresent());
        getTopologyProcessor().removeTarget(targetId);
        Assert.assertFalse(targetStore.getTarget(targetId).isPresent());
    }

    private void expectExceptionForId(long id, String substring) {
        final Matcher<String> matcher =
                        CoreMatchers.allOf(CoreMatchers.containsString(Long.toString(id)),
                                        CoreMatchers.containsString(substring));
        expectedException.expect(TopologyProcessorException.class);
        expectedException.expectMessage(matcher);
    }

    /**
     * Tests removal of not existing target.
     *
     * @throws Exception on exceptions occur
     */
    @Test
    public void testRemoveAbsentTarget() throws Exception {
        expectExceptionForId(-1, "does not exist");
        getTopologyProcessor().removeTarget(-1);
    }

    /**
     * Tests legal update of the target.
     *
     * @throws Exception on exceptions occur
     */
    @Test
    public void testUpdateTarget() throws Exception {
        final long probeId = createProbe();
        final long targetId = createTarget(probeId, "1");
        Assert.assertTrue(targetStore.getTarget(targetId).isPresent());
        final TargetData targetData = Mockito.mock(TargetData.class);
        final InputField field = new InputField(FIELD_NAME, "2", Optional.empty());
        Mockito.when(targetData.getAccountData()).thenReturn(Collections.singleton(field));

        getTopologyProcessor().modifyTarget(targetId, targetData);
        final Target resultTarge = targetStore.getTarget(targetId).get();
        Assert.assertEquals("2",
                        resultTarge.getMediationAccountVals(groupScopeResolver)
                                .iterator().next().getStringValue());
    }

    /**
     * Tests update of not existing target.
     *
     * @throws Exception on exceptions occur
     */
    @Test
    public void testUpdateNonExistingTarget() throws Exception {
        final TargetData targetData = Mockito.mock(TargetData.class);
        final InputField field = new InputField(FIELD_NAME, "2", Optional.empty());
        Mockito.when(targetData.getAccountData()).thenReturn(Collections.singleton(field));

        final long targetId = -1;
        expectExceptionForId(targetId, "does not exist");
        getTopologyProcessor().modifyTarget(targetId, targetData);
    }

    /**
     * Tests validation notifications after validation request.
     *
     * @throws Exception on exceptions occur
     */
    @Test
    public void testValidationRequest() throws Exception {
        final long probeId = createProbe();
        final long targetId = createTarget(probeId, "1");
        addValidationResult(targetId);
        final OperationWaiter listener = new OperationWaiter();
        getTopologyProcessor().addTargetListener(listener);
        final ValidationStatus vldStarted = getTopologyProcessor().validateTarget(targetId);
        Assert.assertEquals(targetId, vldStarted.getTargetId());

        final ValidationStatus vldResult = listener.awaitValidation();
        Assert.assertEquals(targetId, vldResult.getTargetId());
        Assert.assertTrue(vldResult.isCompleted());
        Assert.assertTrue(vldResult.isSuccessful());
    }

    /**
     * Tests failed validation notifications after validation request.
     *
     * @throws Exception on exceptions occur
     */
    @Test
    public void testValidationFailedRequest() throws Exception {
        final long probeId = createProbe();
        final long targetId = createTarget(probeId, "1");
        addValidationFailure(targetId);
        final OperationWaiter listener = new OperationWaiter();
        getTopologyProcessor().addTargetListener(listener);
        final ValidationStatus vldStarted = getTopologyProcessor().validateTarget(targetId);
        Assert.assertEquals(targetId, vldStarted.getTargetId());

        final ValidationStatus vldResult = listener.awaitValidation();
        Assert.assertEquals(targetId, vldResult.getTargetId());
        Assert.assertTrue(vldResult.isCompleted());
        Assert.assertFalse(vldResult.isSuccessful());
    }

    /**
     * Tests discovery notification after discovery request.
     *
     * @throws Exception on exceptions occur
     */
    @Test
    public void testDiscoveryRequest() throws Exception {
        final long probeId = createProbe();
        final long targetId = createTarget(probeId, "1");
        addDiscoveryResult(targetId);

        final OperationWaiter listener = new OperationWaiter();
        getTopologyProcessor().addTargetListener(listener);
        final DiscoveryStatus dscStarted = getTopologyProcessor().discoverTarget(targetId);
        Assert.assertEquals(targetId, dscStarted.getTargetId());

        final DiscoveryStatus dscResult = listener.awaitDiscovery();
        Assert.assertEquals(targetId, dscResult.getTargetId());
        Assert.assertTrue(dscResult.isCompleted());
        Assert.assertTrue(dscResult.isSuccessful());
    }

    /**
     * Tests failed discovery notification after discovery request.
     *
     * @throws Exception on exceptions occur
     */
    @Test
    public void testDiscoveryFailedRequest() throws Exception {
        final long probeId = createProbe();
        final long targetId = createTarget(probeId, "1");
        addDiscoveryFailure(targetId);

        final OperationWaiter listener = new OperationWaiter();
        getTopologyProcessor().addTargetListener(listener);
        final DiscoveryStatus dscStarted = getTopologyProcessor().discoverTarget(targetId);
        Assert.assertEquals(targetId, dscStarted.getTargetId());

        final DiscoveryStatus dscResult = listener.awaitDiscovery();
        Assert.assertEquals(targetId, dscResult.getTargetId());
        Assert.assertTrue(dscResult.isCompleted());
        Assert.assertFalse(dscResult.isSuccessful());
    }

    /**
     * Test multiple discovery and validation notifications after calls for discover and validate
     * all the targets.
     *
     * @throws Exception on exceptions occur
     */
    @Test
    public void testDiscAndValidateMix() throws Exception {
        final long probeId = createProbe();
        final long target1 = createTarget(probeId, "1");
        final long target2 = createTarget(probeId, "2");
        final long target3 = createTarget(probeId, "3");
        final Set<Long> targets = new HashSet<>(Arrays.asList(target1, target2, target3));
        addValidationResult(target1);
        addValidationFailure(target2);
        addValidationResult(target3);
        addDiscoveryResult(target1);
        addDiscoveryFailure(target2);
        addDiscoveryFailure(target3);

        final OperationWaiter listener = new OperationWaiter();
        getTopologyProcessor().addTargetListener(listener);

        final Set<ValidationStatus> vldResult = getTopologyProcessor().validateAllTargets();
        final Set<DiscoveryStatus> dscResult = getTopologyProcessor().discoverAllTargets();

        Assert.assertEquals(targets, vldResult.stream().map(vld -> vld.getTargetId())
                        .collect(Collectors.toSet()));
        Assert.assertEquals(targets, dscResult.stream().map(dsc -> dsc.getTargetId())
                        .collect(Collectors.toSet()));

        listener.awaitDiscoveries(3);
        listener.awaitValidations(3);
        final Collection<DiscoveryStatus> discoveries = listener.discoveredTargets.values();
        final Collection<ValidationStatus> validations = listener.validatedTargets.values();

        final Set<Long> allDiscoveries = listener.discoveredTargets.keySet();
        final Set<Long> allValidations = listener.validatedTargets.keySet();
        final Set<Long> successfullDiscoveries =
                        discoveries.stream().filter(val -> val.isSuccessful() && val.isCompleted())
                                        .map(val -> val.getTargetId()).collect(Collectors.toSet());
        final Set<Long> successfullValidations =
                        validations.stream().filter(val -> val.isSuccessful() && val.isCompleted())
                                        .map(val -> val.getTargetId()).collect(Collectors.toSet());

        Assert.assertEquals(targets, allDiscoveries);
        Assert.assertEquals(targets, allValidations);
        Assert.assertEquals(Sets.newHashSet(target1), successfullDiscoveries);
        Assert.assertEquals(Sets.newHashSet(target1, target3), successfullValidations);
    }

    private long startMove() throws Exception {
        final long probeId = createProbe();
        final long target = createTarget(probeId, "1");

        final EntityDTO pm1 = createEntity(EntityType.PHYSICAL_MACHINE, "1");
        final EntityDTO pm2 = createEntity(EntityType.PHYSICAL_MACHINE, "2");

        final EntityDTO vmOnPm1 = EntityDTO.newBuilder()
                .setEntityType(EntityType.VIRTUAL_MACHINE)
                .setId("3")
                .addCommoditiesBought(CommodityBought.newBuilder()
                        .setProviderId("1"))
                .build();

        final Map<Long, EntityDTO> entities = ImmutableMap.of(
                1L, pm1,
                2L, pm2,
                3L, vmOnPm1);

        addEntities(probeId, target, entities);

        final ExecuteActionRequest request = ExecuteActionRequest.newBuilder()
                .setActionId(0L)
                .setTargetId(target)
                .setActionInfo(ActionInfo.newBuilder()
                    .setMove(Move.newBuilder()
                        .addChanges(ChangeProvider.newBuilder()
                            .setSource(ActionEntity.newBuilder()
                                .setId(1)
                                .setType(1)
                                .build())
                            .setDestination(ActionEntity.newBuilder()
                                .setId(2)
                                .setType(1)
                                .build())
                            .build())
                        .setTarget(ActionEntity.newBuilder()
                            .setId(3)
                            .setType(1)
                            .build())
                        .build())
                    .build())
                .build();

        final ActionExecutionListener listener = Mockito.mock(ActionExecutionListener.class);
        getTopologyProcessor().addActionListener(listener);

        // No exceptions - successfully started!
        actionExecutionService.executeAction(request);

        // Block until receiving a progress notification so we know the action has actually started
        // Otherwise other tests that expect specific notifications may run into race conditions.
        final ArgumentCaptor<ActionProgress> progressCaptor = ArgumentCaptor.forClass(ActionProgress.class);
        Mockito.verify(listener, Mockito.timeout(TIMEOUT_MS).atLeastOnce())
            .onActionProgress(progressCaptor.capture());

        return request.getActionId();
    }

    @Test
    public void testFailMove() throws Exception {
        final long actionId = startMove();

        final FakeRemoteMediation remoteMediation =
                integrationTestServer.getBean(FakeRemoteMediation.class);
        final ActionMessageHandler messageHandler = remoteMediation.getActionMessageHandler();

        final ActionExecutionListener listener = Mockito.mock(ActionExecutionListener.class);
        getTopologyProcessor().addActionListener(listener);

        messageHandler.onReceive(MediationClientMessage.newBuilder()
                .setActionResponse(MediationMessage.ActionResult.newBuilder()
                        .setResponse(MediationMessage.ActionResponse.newBuilder()
                                .setActionResponseState(ActionResponseState.FAILED)
                                .setProgress(100)
                                .setResponseDescription("Failed!")))
                .build());
        final ArgumentCaptor<ActionFailure> failureCaptor = ArgumentCaptor.forClass(ActionFailure.class);
        Mockito.verify(listener, Mockito.timeout(TIMEOUT_MS).times(1))
                .onActionFailure(failureCaptor.capture());
        final ActionFailure failure = failureCaptor.getValue();
        Assert.assertEquals("Failed!", failure.getErrorDescription());
        Assert.assertEquals(actionId, failure.getActionId());
    }

    @Test
    public void testSuccessMove() throws Exception {
        final long actionId = startMove();

        final FakeRemoteMediation remoteMediation =
                integrationTestServer.getBean(FakeRemoteMediation.class);
        final ActionMessageHandler messageHandler = remoteMediation.getActionMessageHandler();

        final ActionExecutionListener listener = Mockito.mock(ActionExecutionListener.class);
        getTopologyProcessor().addActionListener(listener);

        messageHandler.onReceive(MediationClientMessage.newBuilder()
            .setActionProgress(MediationMessage.ActionProgress.newBuilder()
                .setResponse(MediationMessage.ActionResponse.newBuilder()
                    .setActionResponseState(ActionResponseState.IN_PROGRESS)
                    .setProgress(50)
                    .setResponseDescription("Making progress...")))
            .build());

        final ArgumentCaptor<ActionProgress> progressCaptor = ArgumentCaptor.forClass(ActionProgress.class);
        Mockito.verify(listener, Mockito.timeout(TIMEOUT_MS).times(1))
                .onActionProgress(progressCaptor.capture());
        final ActionProgress progress = progressCaptor.getValue();
        Assert.assertEquals("Making progress...", progress.getDescription());
        Assert.assertEquals(actionId, progress.getActionId());
        Assert.assertEquals(50, progress.getProgressPercentage());

        messageHandler.onReceive(MediationClientMessage.newBuilder()
            .setActionResponse(MediationMessage.ActionResult.newBuilder()
                .setResponse(MediationMessage.ActionResponse.newBuilder()
                    .setActionResponseState(ActionResponseState.SUCCEEDED)
                    .setProgress(100)
                    .setResponseDescription("Success!")))
            .build());
        final ArgumentCaptor<ActionSuccess> successCaptor = ArgumentCaptor.forClass(ActionSuccess.class);
        Mockito.verify(listener, Mockito.timeout(TIMEOUT_MS).times(1))
                .onActionSuccess(successCaptor.capture());
        final ActionSuccess success = successCaptor.getValue();
        Assert.assertEquals("Success!", success.getSuccessDescription());
        Assert.assertEquals(actionId, success.getActionId());
    }

    // Add entities to the appropriate data stores so that they will be found when tests access them.
    private void addEntities(final long probeId,
                             final long targetId,
                             Map<Long, EntityDTO> entities)
            throws IdentityUninitializedException, IdentityMetadataMissingException,
            IdentityProviderException, TargetNotFoundException {
        Mockito.doReturn(entities)
                .when(identityProviderSpy)
                .getIdsForEntities(Mockito.eq(probeId),
                        Mockito.eq(new ArrayList<>(entities.values())));
        // Add the entities to the entity store, which houses the raw discovered entity data
        entityStore.entitiesDiscovered(probeId, targetId, 0, DiscoveryType.FULL,
            new ArrayList<>(entities.values()));
        // Also update the repository client mock to respond appropriately when queried about these
        // entities. This simulates these entities also being found in the repository (where
        // stitched data is found).
        List<TopologyEntityDTO> convertedEntities =
                SdkToTopologyEntityConverter.convertToTopologyEntityDTOs(entities)
                        .stream()
                        .map(Builder::build)
                        .collect(Collectors.toList());
        repositoryClientFake.addEntities(convertedEntities);
    }

    private EntityDTO createEntity(final EntityType type, final String id) {
        return EntityDTO.newBuilder()
                .setEntityType(type)
                .setId(id)
                .build();
    }

    private void addValidationResult(long targetId) throws Exception {
        final FakeRemoteMediation remoteMediation =
                        integrationTestServer.getBean(FakeRemoteMediation.class);
        final ValidationResponse response = ValidationResponse.newBuilder().build();
        remoteMediation.addValidationResponse(targetId, response);
    }

    private void addValidationFailure(long targetId) throws Exception {
        final FakeRemoteMediation remoteMediation =
                        integrationTestServer.getBean(FakeRemoteMediation.class);
        final ValidationResponse response = ValidationResponse.newBuilder()
                        .addErrorDTO(SDKUtil.createCriticalError("test error")).build();
        remoteMediation.addValidationResponse(targetId, response);
    }

    private void addDiscoveryResult(long targetId) throws Exception {
        final FakeRemoteMediation remoteMediation =
                        integrationTestServer.getBean(FakeRemoteMediation.class);
        final DiscoveryResponse response = DiscoveryResponse.newBuilder().build();
        remoteMediation.addDiscoveryResponse(targetId, response);
    }

    private void addDiscoveryFailure(long targetId) throws Exception {
        final FakeRemoteMediation remoteMediation =
                        integrationTestServer.getBean(FakeRemoteMediation.class);
        final DiscoveryResponse response = DiscoveryResponse.newBuilder()
                        .addErrorDTO(SDKUtil.createCriticalError("test discovery error")).build();
        remoteMediation.addDiscoveryResponse(targetId, response);
    }

    private long createProbe() throws ProbeException {
        final AccountDefEntry accountDefEntry =
                        AccountDefEntry.newBuilder()
                                        .setCustomDefinition(CustomAccountDefEntry.newBuilder()
                                                        .setName(FIELD_NAME).setDisplayName("blah")
                                                        .setDescription("BLAH")
                                                        .setPrimitiveValue(PrimitiveValue.STRING))
                                        .setMandatory(false).build();
        final MediationMessage.ProbeInfo oneMandatory =
                        Probes.createEmptyProbe().addAccountDefinition(accountDefEntry).build();
        probeStore.registerNewProbe(oneMandatory, null);
        return probeStore.getProbes().entrySet().stream()
                        .filter(e -> e.getValue().getProbeType()
                                        .equals(oneMandatory.getProbeType()))
                        .map(e -> e.getKey()).collect(Collectors.toSet()).iterator().next();
    }

    private void assertEquals(Map<Long, MediationMessage.ProbeInfo> expected,
                    Collection<? extends ProbeInfo> actual) {
        for (ProbeInfo desc : actual) {
            final MediationMessage.ProbeInfo info = expected.get(desc.getId());
            Assert.assertNotNull("Extra probe with id " + desc.getId(), info);
            assertEquals(info, desc);
        }
        Assert.assertEquals(expected.size(), actual.size());
    }

    private void assertEquals(MediationMessage.ProbeInfo expected, ProbeInfo actual) {
        Assert.assertEquals(expected.getProbeType(), actual.getType());
        Assert.assertEquals(expected.getProbeCategory(), actual.getCategory());
        final List<String> actualFields = actual.getAccountDefinitions().stream()
                        .map(af -> af.getName()).collect(Collectors.toList());
        final List<String> expectedFields = expected.getAccountDefinitionList().stream()
                        .map(av -> AccountValueAdaptor.wrap(av).getName())
                        .collect(Collectors.toList());
        Assert.assertEquals(expectedFields, actualFields);
        Assert.assertEquals(expected.getTargetIdentifierFieldList(),
                        actual.getIdentifyingFields());
    }

    /**
     * Operations waiter is aimed to await finished operations: validations and discoveries and
     * store operation statuses.
     */
    private static class OperationWaiter implements TargetListener {

        private final Semaphore discoverySemaphore = new Semaphore(0);
        private final Semaphore validationSemaphore = new Semaphore(0);
        private final ConcurrentMap<Long, DiscoveryStatus> discoveredTargets =
                        new ConcurrentHashMap<>();
        private final ConcurrentMap<Long, ValidationStatus> validatedTargets =
                        new ConcurrentHashMap<>();

        @Override
        public void onTargetValidated(@Nonnull final ValidationStatus result) {
            if (result.isCompleted()) {
                if (validatedTargets.putIfAbsent(result.getTargetId(), result) == null) {
                    validationSemaphore.release();
                }
            }
        }

        @Override
        public void onTargetDiscovered(@Nonnull final DiscoveryStatus result) {
            if (result.isCompleted()) {
                if (discoveredTargets.putIfAbsent(result.getTargetId(), result) == null) {
                    discoverySemaphore.release();
                }
            }
        }

        public void awaitValidations(int number) throws InterruptedException {
            Assert.assertTrue("Failed to await " + number + " validations",
                            validationSemaphore.tryAcquire(number, TIMEOUT_MS, TimeUnit.MILLISECONDS));
        }

        public void awaitDiscoveries(int number) throws InterruptedException {
            Assert.assertTrue("Failed to await " + number + " discoveries",
                            discoverySemaphore.tryAcquire(number, TIMEOUT_MS, TimeUnit.MILLISECONDS));
        }

        public ValidationStatus awaitValidation() throws InterruptedException {
            awaitValidations(1);
            return validatedTargets.values().iterator().next();
        }

        public DiscoveryStatus awaitDiscovery() throws InterruptedException {
            awaitDiscoveries(1);
            return discoveredTargets.values().iterator().next();
        }
    }
}
