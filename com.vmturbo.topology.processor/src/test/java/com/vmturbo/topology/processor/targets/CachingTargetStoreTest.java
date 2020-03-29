package com.vmturbo.topology.processor.targets;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.AdditionalAnswers;
import org.mockito.Mockito;

import com.vmturbo.identity.exceptions.IdentityStoreException;
import com.vmturbo.identity.store.IdentityStore;
import com.vmturbo.identity.store.IdentityStoreUpdate;
import com.vmturbo.kvstore.KeyValueStore;
import com.vmturbo.platform.common.dto.Discovery.AccountDefEntry;
import com.vmturbo.platform.common.dto.Discovery.CustomAccountDefEntry;
import com.vmturbo.platform.common.dto.Discovery.CustomAccountDefEntry.PrimitiveValue;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.platform.sdk.common.PredefinedAccountDefinition;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.topology.processor.TestIdentityStore;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.TargetSpec;
import com.vmturbo.topology.processor.api.dto.InputField;
import com.vmturbo.topology.processor.api.impl.TargetRESTApi;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.scheduling.Scheduler;
import com.vmturbo.topology.processor.stitching.journal.StitchingJournalFactory;
import com.vmturbo.topology.processor.topology.TopologyHandler;
import com.vmturbo.topology.processor.util.Probes;

/**
 * Test the {@link CachingTargetStore}.
 */
public class CachingTargetStoreTest {

    private static final String FIELD_NAME = "targetIdentifier";

    private static final long DERIVED_PROBE_ID = 1L;

    private static final AccountDefEntry PLAIN_ACCT_DEF = AccountDefEntry.newBuilder()
        .setCustomDefinition(
            CustomAccountDefEntry.newBuilder()
                .setName(PredefinedAccountDefinition.Address.name().toLowerCase())
                .setDisplayName("this is my address")
                .setDescription("The address")
                .setIsSecret(false))
        .setMandatory(true)
        .build();

    private static final AccountDefEntry NAME_ACCT_DEF = AccountDefEntry.newBuilder()
        .setCustomDefinition(
            CustomAccountDefEntry.newBuilder()
                .setName("name")
                .setDisplayName("this is my name")
                .setDescription("Display Name")
                .setIsSecret(false))
        .setMandatory(true)
        .setIsTargetDisplayName(true)
        .build();

    private static final ProbeInfo parentProbeInfo = createProbeInfo(ProbeCategory.HYPERVISOR.getCategory(),
        SDKProbeType.VCENTER.toString(),
        PredefinedAccountDefinition.Address.name().toLowerCase(),
        Collections.singletonList(PLAIN_ACCT_DEF));

    private static final ProbeInfo derivedProbeInfo = createProbeInfo(ProbeCategory.COST.getCategory(), "vc_cost",
        NAME_ACCT_DEF.getCustomDefinition().getName(),
        ImmutableList.of(NAME_ACCT_DEF, PLAIN_ACCT_DEF));

    private TargetDao targetDao;

    private ProbeStore probeStore;

    private GroupScopeResolver groupScopeResolver;

    private IdentityStore<TargetSpec> targetIdentityStore;

    private CachingTargetStore targetStore;

    private ProbeInfo probeInfo;

    private final TopologyHandler topologyHandler = mock(TopologyHandler.class);

    private final Scheduler scheduler = mock(Scheduler.class);

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setup() throws Exception{
        probeInfo = Probes.emptyProbe;
        targetDao = mock(TargetDao.class);
        probeStore = mock(ProbeStore.class);
        groupScopeResolver = mock(GroupScopeResolver.class);
        when(groupScopeResolver.processGroupScope(any(), any(), any()))
                .then(AdditionalAnswers.returnsSecondArg());
        targetIdentityStore = new TestIdentityStore<>(new TargetSpecAttributeExtractor(probeStore));
        targetStore = new CachingTargetStore(targetDao, probeStore, targetIdentityStore);
    }

    /**
     * Test that creating a target calls the expected methods on the {@link KeyValueStore}.
     *
     * @throws Exception If something goes wrong.
     */
    @Test
    public void testCreateTarget() throws Exception {
        when(probeStore.getProbe(Mockito.anyLong())).thenReturn(Optional.of(probeInfo));

        final TargetRESTApi.TargetSpec spec = new TargetRESTApi.TargetSpec(0L, Collections.emptyList());

        final Target target = targetStore.createTarget(spec.toDto());

        verify(targetDao).store(target);

        targetStore.getTarget(target.getId()).get();
    }

    /**
     * Test that creating a target with the same identifying values as an existing target throws
     * an exception.
     *
     * @throws Exception If something goes wrong.
     */
    @Test(expected = DuplicateTargetException.class)
    public void testCreateTargetExists() throws Exception {
        when(probeStore.getProbe(Mockito.anyLong())).thenReturn(Optional.of(probeInfo));

        final TargetRESTApi.TargetSpec spec = new TargetRESTApi.TargetSpec(0L, Collections.emptyList());

        final Target target = targetStore.createTarget(spec.toDto());

        verify(targetDao).store(target);

        targetStore.createTarget(spec.toDto());
    }

    /**
     * Test that creating a target with the same identifying values as a previously existing target
     * assigns the same OID.
     *
     * @throws Exception If something goes wrong.
     */
    @Test
    public void testRecreateTargetWithStableId() throws Exception {
        when(probeStore.getProbe(Mockito.anyLong())).thenReturn(Optional.of(probeInfo));

        final TargetRESTApi.TargetSpec spec = new TargetRESTApi.TargetSpec(0L, Collections.emptyList());

        final Target target = targetStore.createTarget(spec.toDto());

        targetStore.removeAllTargets();

        final Target newTarget = targetStore.createTarget(spec.toDto());
        assertEquals(target.getId(), newTarget.getId());
    }

    /**
     * Test that when we create a target with bad account values, we get an exception and the
     * identity store does not have a target with that ID in it afterwards.
     *
     * @throws Exception when something goes wrong.
     */
    @Test(expected = InvalidTargetException.class)
    public void testCreateTargetWrongAccountValues() throws Exception {
        ProbeInfo pi = createProbeInfo("test", "vc",
            PredefinedAccountDefinition.Address.name().toLowerCase(),
            Collections.emptyList());
        when(probeStore.getProbe(Mockito.anyLong())).thenReturn(Optional.of(pi));

        final TargetRESTApi.TargetSpec spec = new TargetRESTApi.TargetSpec(0L,
            Collections.singletonList(new InputField(
                PredefinedAccountDefinition.Username.name().toLowerCase(),
                "foo",
                Optional.empty())));
        // we expect the create target to fail since the account values don't match the account
        // definition of the probe.
        targetStore.createTarget(spec.toDto());
    }

    private static ProbeInfo createProbeInfo(@Nonnull String category,
                                      @Nonnull String type,
                                      @Nonnull String targetIdentifyingFieldName,
                                      @Nonnull List<AccountDefEntry> accountDefs) {
        return ProbeInfo.newBuilder()
            .setProbeCategory(category)
            .setProbeType(type)
            .addTargetIdentifierField(targetIdentifyingFieldName)
            .addAllAccountDefinition(accountDefs)
            .build();
    }

    @Test
    public void testGetProbeName() throws Exception {
        ProbeInfo pi = createProbeInfo("test", "vc",
            PredefinedAccountDefinition.Address.name().toLowerCase(),
            Collections.singletonList(PLAIN_ACCT_DEF));

        when(probeStore.getProbe(Mockito.anyLong())).thenReturn(Optional.of(pi));

        final TargetRESTApi.TargetSpec spec = new TargetRESTApi.TargetSpec(0L, Collections.singletonList(
            new InputField(PredefinedAccountDefinition.Address.name().toLowerCase(), "foo", Optional.empty())));
        final Target target = targetStore.createTarget(spec.toDto());

        Assert.assertEquals("foo", target.getDisplayName());
    }

    @Test
    public void testGetProbeNameWithIsTargetDisplayNameAttr() throws Exception {
        ProbeInfo pi = createProbeInfo("test", "vc",
            PredefinedAccountDefinition.Address.name().toLowerCase(),
            ImmutableList.of(PLAIN_ACCT_DEF, NAME_ACCT_DEF));

        when(probeStore.getProbe(Mockito.anyLong())).thenReturn(Optional.of(pi));

        final TargetRESTApi.TargetSpec spec = new TargetRESTApi.TargetSpec(0L, Arrays.asList(
                new InputField(PredefinedAccountDefinition.Address.name().toLowerCase(), "foo", Optional.empty()),
                new InputField(NAME_ACCT_DEF.getCustomDefinition().getName(), "my name", Optional.empty())));
        final Target target = targetStore.createTarget(spec.toDto());

        Assert.assertEquals("my name", target.getDisplayName());
    }

    @Test
    public void testCreateTargetNotifiesListeners() throws Exception {
        when(probeStore.getProbe(Mockito.anyLong())).thenReturn(Optional.of(probeInfo));
        TargetStoreListener listener = mock(TargetStoreListener.class);
        targetStore.addListener(listener);

        final TargetRESTApi.TargetSpec spec = new TargetRESTApi.TargetSpec(0L, Collections.emptyList());
        Target target = targetStore.createTarget(spec.toDto());

        verify(listener).onTargetAdded(target);
    }

    /**
     * Test that constructing a {@link CachingTargetStore} retrieves saved targets from
     * the associated {@link KeyValueStore}.
     *
     * @throws Exception If something goes wrong.
     */
    @Test
    public void testInitialization() throws Exception {
        when(probeStore.getProbe(Mockito.anyLong())).thenReturn(Optional.of(probeInfo));
        final long targetId = 0L;
        final TargetRESTApi.TargetSpec spec = new TargetRESTApi.TargetSpec(targetId, Arrays.asList(
                new InputField("name", "foo", Optional.empty())));

        final Target target = new Target(targetId, probeStore, spec.toDto(), false);

        final TargetDao targetDao = mock(TargetDao.class);
        when(targetDao.getAll()).thenReturn(Collections.singletonList(target));
        final CachingTargetStore newTargetStore = new CachingTargetStore(targetDao, probeStore,
                targetIdentityStore);
        newTargetStore.getTarget(0L).get();
    }

    @Test
    public void testRemoveListenerWhenPresent() throws Exception {
        final TargetStoreListener listener = mock(TargetStoreListener.class);

        targetStore.addListener(listener);
        assertTrue(targetStore.removeListener(listener));
    }

    @Test
    public void testRemoveListenerWhenAbsent() throws Exception {
        assertFalse(targetStore.removeListener(mock(TargetStoreListener.class)));
    }

    private void prepareInitialProbe() {
        final ProbeInfo probeInfo = ProbeInfo.newBuilder(this.probeInfo)
                        .addAccountDefinition(AccountDefEntry.newBuilder()
                                        .setCustomDefinition(CustomAccountDefEntry.newBuilder()
                                                        .setName(FIELD_NAME)
                                                        .setDisplayName("displayName")
                                                        .setDescription("desc"))
                                        .setMandatory(true))
                        .build();
        when(probeStore.getProbe(Mockito.anyLong())).thenReturn(Optional.of(probeInfo));
    }

    private TargetSpec createTargetSpec(long probeId, int fieldValue) {
        final TargetSpec.Builder builder = TargetSpec.newBuilder().setProbeId(probeId);
        builder.addAllAccountValue(createAccountValue(fieldValue));
        return builder.build();
    }

    private TargetSpec createTargetSpec(long probeId,
                                        @Nonnull String fieldName,
                                        @Nonnull String fieldValue) {
        return TargetSpec.newBuilder()
            .setProbeId(probeId)
            .addAccountValue(createAccountValue(fieldName, fieldValue))
            .build();
    }

    private Collection<TopologyProcessorDTO.AccountValue> createAccountValue(int fieldValue) {
        return Collections.singleton(TopologyProcessorDTO.AccountValue.newBuilder().setKey(FIELD_NAME)
                        .setStringValue(Integer.toString(fieldValue)).build());
    }

    private TopologyProcessorDTO.AccountValue createAccountValue(String name, String value) {
        return TopologyProcessorDTO.AccountValue.newBuilder()
            .setKey(name)
            .setStringValue(value)
            .build();
    }

    /**
     * Tests changing target in the target store.
     *
     * @throws Exception on exceptions occur
     */
    @Test
    public void testUpdateTarget() throws Exception {
        prepareInitialProbe();
        final TargetSpec targetSpec = createTargetSpec(0, 1);
        Target target = targetStore.createTarget(targetSpec);
        TargetStoreListener listener = mock(TargetStoreListener.class);
        targetStore.addListener(listener);
        Assert.assertEquals("1",
                        target.getMediationAccountVals(groupScopeResolver).iterator().next().getStringValue());
        target = targetStore.updateTarget(target.getId(), targetSpec.getAccountValueList());
        // No update message sent since account values did not change
        verify(listener, never()).onTargetUpdated(target);
        Assert.assertEquals("1",
                        target.getMediationAccountVals(groupScopeResolver).iterator().next().getStringValue());

        final Collection<TopologyProcessorDTO.AccountValue> targetFieldsNew = createAccountValue(2);
        target = targetStore.updateTarget(target.getId(), targetFieldsNew);
        Assert.assertEquals("2",
                        target.getMediationAccountVals(groupScopeResolver)
                                .iterator().next().getStringValue());
        // Update message was sent since account values changed
        verify(listener).onTargetUpdated(target);
    }

    /**
     * Tests that derivedTargetIds field has populated correctly after "CreateOrUpdateDerivedTargets"
     * getting called.
     *
     * @throws Exception Not expected to happen.
     */
    @Test
    public void testDerivedTargetIdsPopulation() throws Exception {
        prepareInitialProbe();
        final Target parent = targetStore.createTarget(createTargetSpec(0L, 666));
        final TargetSpec derivedTargetSpec1 = TargetSpec.newBuilder()
                .setProbeId(DERIVED_PROBE_ID)
                .setIsHidden(true)
                .addAllAccountValue(createAccountValue(100))
                .build();
        final Target derived1 = targetStore.createTarget(derivedTargetSpec1);
        final TargetSpec derivedTargetSpec2 = TargetSpec.newBuilder()
                .setProbeId(DERIVED_PROBE_ID)
                .addAllAccountValue(createAccountValue(200))
                .setIsHidden(true)
                .build();
        final Target derived2 = targetStore.createTarget(derivedTargetSpec2);

        targetStore.createOrUpdateDerivedTargets(
            Lists.newArrayList(derivedTargetSpec1, derivedTargetSpec2), parent.getId());

        // "derived1" and "derived2" are derived targets of "parent".
        verifyDerivedTargetIdsList(
                Lists.newArrayList(derived1.getId(), derived2.getId()),
                getDerivedTargetIds(parent));

        final TargetSpec derivedTargetSpec3 = TargetSpec.newBuilder()
                .setProbeId(DERIVED_PROBE_ID)
                .addAllAccountValue(createAccountValue(300))
                .setIsHidden(true)
                .build();
        final Target derived3 = targetStore.createTarget(derivedTargetSpec3);

        targetStore.createOrUpdateDerivedTargets(Lists.newArrayList(derivedTargetSpec3),
            derived1.getId());

        // "derived3" is a derived target of "derived1" (similar to cost targets in Azure).
        verifyDerivedTargetIdsList(
                Lists.newArrayList(derived3.getId()),
                getDerivedTargetIds(derived1));
    }

    /**
     * Verifies that all derivedTargetIds got populated as expected.
     * @param expectedDerivedTargetIds List of expected derivedTargetIds.
     * @param derivedTargetIds List of derivedTargetIds of a {@link Target} from the target store.
     *
     */
    private void verifyDerivedTargetIdsList(final List<Long> expectedDerivedTargetIds,
                                            final List<Long> derivedTargetIds) {
        Assert.assertTrue(
                derivedTargetIds.size() == expectedDerivedTargetIds.size() &&
                expectedDerivedTargetIds.containsAll(derivedTargetIds));
    }

    /**
     * Get derivedTargetIds of a given {@link Target}.
     *
     * @param target A {@link Target} from the target store.
     * @return derivedTargetIds of a given {@link Target}.
     */
    private List<Long> getDerivedTargetIds(Target target) {
        return targetStore.getTarget(target.getId()).get().getSpec().getDerivedTargetIdsList();
    }

    /**
     * Test that if we have a derived target that has incorrect account values, we don't leave
     * behind an entry for it in the identity store.
     *
     * @throws Exception when something goes wrong.
     */
    @Test
    public void testBadDerivedTargetPopulation() throws Exception {
        prepareInitialProbe();
        final Target parent = targetStore.createTarget(createTargetSpec(0L, 666));
        final TargetSpec derivedTargetSpec1 = TargetSpec.newBuilder()
            .setProbeId(DERIVED_PROBE_ID)
            .setIsHidden(true)
            .addAllAccountValue(Collections.singleton(TopologyProcessorDTO.AccountValue.newBuilder()
                .setKey("badfield")
                .setStringValue("whocares")
                .build()))
            .build();
        // this call should fail to create a derived target, since the account value doesn't
        // match the probe's account definition
        targetStore.createOrUpdateDerivedTargets(Lists.newArrayList(derivedTargetSpec1),
            parent.getId());
    }

        @Test
    public void testPartialUpdateTarget() throws Exception {
        final String fooName = "foo";
        final String barName = "bar";

        TopologyProcessorDTO.AccountValue fooAccountValue =
            TopologyProcessorDTO.AccountValue.newBuilder().setKey(fooName)
                .setStringValue("foo-original").build();
        TopologyProcessorDTO.AccountValue barAccountValue =
            TopologyProcessorDTO.AccountValue.newBuilder().setKey(barName)
                .setStringValue("bar-original").build();

        final ProbeInfo probeInfo = ProbeInfo.newBuilder(this.probeInfo)
            .addAccountDefinition(AccountDefEntry.newBuilder()
                .setCustomDefinition(CustomAccountDefEntry.newBuilder()
                    .setName(fooName)
                    .setDisplayName("foo-displayName")
                    .setDescription("foo-desc"))
                .setMandatory(true))
            .addAccountDefinition(AccountDefEntry.newBuilder()
                .setCustomDefinition(CustomAccountDefEntry.newBuilder()
                    .setName(barName)
                    .setDisplayName("bar-displayName")
                    .setDescription("bar-desc"))
                .setMandatory(false))
            .build();
        when(probeStore.getProbe(Mockito.anyLong())).thenReturn(Optional.of(probeInfo));

        final TargetSpec targetSpec = TargetSpec.newBuilder().setProbeId(0)
            .addAllAccountValue(Arrays.asList(fooAccountValue, barAccountValue)).build();
        Target target = targetStore.createTarget(targetSpec);
        assertAccountValueEquals(target, fooName, "foo-original");
        assertAccountValueEquals(target, barName, "bar-original");

        Target updatedTarget = target.withUpdatedFields(
            Collections.singleton(barAccountValue.toBuilder().setStringValue("bar-updated").build()),
            probeStore);

        assertAccountValueEquals(updatedTarget, fooName, "foo-original");
        assertAccountValueEquals(updatedTarget, barName, "bar-updated");
    }

    /**
     * Test that when we update a target with an empty string value for an existing numeric field,
     * the respective account value is removed from the target.  If we update a string field,
     * however, the empty string does get sent as the new account value.
     *
     * @throws Exception when there is a problem updating the target.
     */
    @Test
    public void testEmptyValuesUpdateTarget() throws Exception {
        final String fooName = "foo";
        final String barName = "bar";
        final String carName = "car";
        final String originalFooValue = "foo-original";
        final String originalBarValue = "8080";
        final String originalCarValue = "car-original";

        final TopologyProcessorDTO.AccountValue fooAccountValue =
            TopologyProcessorDTO.AccountValue.newBuilder().setKey(fooName)
                .setStringValue(originalFooValue).build();
        final TopologyProcessorDTO.AccountValue barAccountValue =
            TopologyProcessorDTO.AccountValue.newBuilder().setKey(barName)
                .setStringValue(originalBarValue).build();
        final TopologyProcessorDTO.AccountValue carAccountValue =
            TopologyProcessorDTO.AccountValue.newBuilder().setKey(carName)
                .setStringValue(originalCarValue).build();

        final ProbeInfo probeInfo = ProbeInfo.newBuilder(this.probeInfo)
            .addAccountDefinition(AccountDefEntry.newBuilder()
                .setCustomDefinition(CustomAccountDefEntry.newBuilder()
                    .setName(fooName)
                    .setDisplayName("foo-displayName")
                    .setDescription("foo-desc"))
                .setMandatory(true))
            .addAccountDefinition(AccountDefEntry.newBuilder()
                .setCustomDefinition(CustomAccountDefEntry.newBuilder()
                    .setName(barName)
                    .setDisplayName("bar-displayName")
                    .setDescription("bar-desc")
                    .setPrimitiveValue(PrimitiveValue.NUMERIC))
                .setMandatory(false))
            .addAccountDefinition(AccountDefEntry.newBuilder()
                .setCustomDefinition(CustomAccountDefEntry.newBuilder()
                    .setName(carName)
                    .setDisplayName("car-displayName")
                    .setDescription("car-desc")
                    .setPrimitiveValue(PrimitiveValue.STRING))
                .setMandatory(false))
            .build();
        when(probeStore.getProbe(Mockito.anyLong())).thenReturn(Optional.of(probeInfo));

        final TargetSpec targetSpec = TargetSpec.newBuilder().setProbeId(0)
            .addAllAccountValue(Arrays.asList(fooAccountValue, barAccountValue)).build();
        final Target target = targetStore.createTarget(targetSpec);
        assertAccountValueEquals(target, fooName, originalFooValue);
        assertAccountValueEquals(target, barName, originalBarValue);

        final Target updatedTargetEmptyBarAndCarValues = target.withUpdatedFields(
            ImmutableList.of(barAccountValue.toBuilder().setStringValue("").build(),
                carAccountValue.toBuilder().setStringValue("").build()),
            probeStore);
        // assert that account value that was set to empty string does not exist
        assertFalse(updatedTargetEmptyBarAndCarValues.getMediationAccountVals(groupScopeResolver).stream()
            .filter(acctValue -> acctValue.getKey().equals(barAccountValue.getKey()))
            .findAny()
            .isPresent());
        assertAccountValueEquals(updatedTargetEmptyBarAndCarValues, fooName, originalFooValue);
        assertAccountValueEquals(updatedTargetEmptyBarAndCarValues, carName, "");
    }

    private void assertAccountValueEquals(@Nonnull final Target target,
                                          @Nonnull final String keyName,
                                          @Nonnull final String expectedValueName) {
        Assert.assertEquals(expectedValueName, target.getMediationAccountVals(groupScopeResolver)
                .stream()
                .filter(val -> val.getKey().equals(keyName))
                .findFirst()
                .get()
                .getStringValue());
    }

    /**
     * Tests changing non existing target from the target store. Store is expected to throw
     * exception
     *
     * @throws Exception on exceptions occur
     */
    @Test
    public void testUpdateNonExistingTarget() throws Exception {
        prepareInitialProbe();
        final TargetSpec spec = createTargetSpec(0, 1);
        expectedException.expect(TargetNotFoundException.class);
        expectedException.expectMessage("does not exist");
        targetStore.updateTarget(-1, spec.getAccountValueList());
    }

    /**
     * Tests target removal from target store, and trigger broadcast
     * topology and reset schedule.
     *
     * @throws Exception on exceptions occur
     */
    @Test
    public void testRemoveTargetAndBroadcastTopology() throws Exception {
        prepareInitialProbe();
        final TargetStoreListener targetListener = mock(TargetStoreListener.class);
        final TargetSpec targetSpec = createTargetSpec(0, 1);
        final Target target = targetStore.createTarget(targetSpec);
        targetStore.addListener(targetListener);
        Assert.assertEquals("1",
                target.getMediationAccountVals(groupScopeResolver).iterator().next()
                        .getStringValue());
        Assert.assertEquals(1, targetStore.getAll().size());

        targetStore.removeTargetAndBroadcastTopology(target.getId(), topologyHandler, scheduler);
        Assert.assertEquals(0, targetStore.getAll().size());
        Mockito.verify(targetListener).onTargetRemoved(target);
        verify(topologyHandler).broadcastLatestTopology(any(StitchingJournalFactory.class));
        verify(scheduler).resetBroadcastSchedule();
    }

    /**
     * Tests removal of not existing target. Target store is expected to throw exception.
     *
     * @throws Exception on exceptions occur
     */
    @Test
    public void testRemoveNonExistingTarget() throws Exception {
        prepareInitialProbe();
        expectedException.expect(TargetNotFoundException.class);
        expectedException.expectMessage("does not exist");
        targetStore.removeTargetAndBroadcastTopology(-1, topologyHandler, scheduler);
    }

    /**
     * Tests derived targets deletion. If the derived target is dependent on a parent target, then we need
     * to remove the derived target after the parent target is deleted.
     */
    @Test
    public void testDerivedTargetsDeletion() throws Exception {
        prepareInitialProbe();
        final Target parent = targetStore.createTarget(createTargetSpec(0L, 666));
        final TargetSpec derivedTargetSpec1 = TargetSpec.newBuilder()
                .setProbeId(DERIVED_PROBE_ID)
                .addAllAccountValue(createAccountValue(555))
                .build();
        final TargetSpec derivedTargetSpec2 = TargetSpec.newBuilder()
                .setProbeId(DERIVED_PROBE_ID)
                .addAllAccountValue(createAccountValue(2333))
                .setIsHidden(true)
                .build();

        // Derived target 1 is not dependent on parent target.
        final Target derived1 = targetStore.createTarget(derivedTargetSpec1);
        // Derived target 2 is dependent on parent target.
        targetStore.createOrUpdateDerivedTargets(Collections.singletonList(derivedTargetSpec2),
            parent.getId());
        final Long derivedTarget2Id = verifyDerivedTargetCreation(derivedTargetSpec2);
        assertNotNull(derivedTarget2Id);
        Assert.assertEquals(3, targetStore.getAll().size());

        targetStore.removeTargetAndBroadcastTopology(parent.getId(), topologyHandler, scheduler);
        Assert.assertTrue(targetStore.getTarget(derived1.getId()).isPresent());
        Assert.assertFalse(targetStore.getTarget(derivedTarget2Id).isPresent());
        Assert.assertEquals(1, targetStore.getAll().size());
    }

    /**
     * Test that derived target is not deleted when updateTarget is called on the parent target.
     *
     * @throws Exception if createTarget or updateTarget throws an exception.
     */
    @Test
    public void testParentTargetUpdateRetainsDerivedTarget() throws Exception {
        prepareInitialProbe();
        final Target parent = targetStore.createTarget(createTargetSpec(0L, 666));
        final long derivedProbeId = 1L;
        final TargetSpec derivedTargetSpec = TargetSpec.newBuilder()
                .setProbeId(derivedProbeId)
                .addAllAccountValue(createAccountValue(555))
                .build();
        targetStore.createOrUpdateDerivedTargets(Collections.singletonList(derivedTargetSpec),
            parent.getId());
        final Long derivedTargetId = verifyDerivedTargetCreation(derivedTargetSpec);
        assertNotNull(derivedTargetId);
        targetStore.updateTarget(parent.getId(), parent.getSpec().getAccountValueList());
        Assert.assertTrue(targetStore.getTarget(derivedTargetId).isPresent());
    }

    @Test
    public void testGetProbeCategoryForTarget() throws Exception {
        ProbeInfo probeInfo = createProbeInfo(ProbeCategory.HYPERVISOR.getCategory(),
            SDKProbeType.VCENTER.toString(),
            PredefinedAccountDefinition.Address.name().toLowerCase(),
            Collections.singletonList(PLAIN_ACCT_DEF));

        when(probeStore.getProbe(Mockito.anyLong())).thenReturn(Optional.of(probeInfo));

        final TargetRESTApi.TargetSpec spec = new TargetRESTApi.TargetSpec(0L, Collections.singletonList(
            new InputField(PredefinedAccountDefinition.Address.name().toLowerCase(), FIELD_NAME, Optional.empty())));
        final Target target = targetStore.createTarget(spec.toDto());
        Assert.assertEquals(ProbeCategory.HYPERVISOR, targetStore.getProbeCategoryForTarget(target.getId()).get());

    }

    /**
     * Verify that a target matching the passed in TargetSpec exists and that there is only one
     * match for it.
     *
     * @param derivedTargetSpec spec for the derived target.
     * @return targetId of the derived target.
     * @throws IdentityStoreException if the identity store throws an exception while processing
     * the target spec.
     */
    private Long verifyDerivedTargetCreation(TargetSpec derivedTargetSpec)
        throws IdentityStoreException {
        final IdentityStoreUpdate<TargetSpec> identityStoreUpdate = targetIdentityStore
            .fetchOrAssignItemOids(Arrays.asList(derivedTargetSpec));
        assertTrue(identityStoreUpdate.getNewItems().isEmpty());
        assertTrue(identityStoreUpdate.getOldItems().size() == 1);
        final Long derivedTargetId1 = identityStoreUpdate.getOldItems().get(derivedTargetSpec);
        return derivedTargetId1;
    }

    /**
     * Verify that the address passed in matches the address account value of the target.
     *
     * @param targetAddress String giving the expected account value for the address field.
     * @param target the target pulled from the target store.
     */
    private void verifyAddressAccountValue(String targetAddress, Target target) {
        assertEquals(targetAddress,
            target.getSpec().getAccountValueList().stream()
                .filter(acctVal ->
                    PredefinedAccountDefinition.Address.name().toLowerCase()
                        .equals(acctVal.getKey()))
                .findFirst()
                .map(acctValue -> acctValue.getStringValue())
                .get());
    }

    /**
     * Test that we correctly handle derived targets that are shared between two parents.
     *
     * @throws Exception if the targetStore throws one.
     */
    @Test
    public void testMultipleParentDerivedTarget() throws Exception {
        final long parentProbeId = 1L;
        final long derivedProbeId = 2L;
        final String commonNameValue = "foo";
        final String derivedTargetAddress1 = "Address1";
        final String derivedTargetAddress2 = "Address2";

        when(probeStore.getProbe(parentProbeId)).thenReturn(Optional.of(parentProbeInfo));
        when(probeStore.getProbe(derivedProbeId)).thenReturn(Optional.of(derivedProbeInfo));

        final Target parentTarget1 = targetStore.createTarget(createTargetSpec(parentProbeId,
            PredefinedAccountDefinition.Address.name().toLowerCase(),
            "Parent1"));
        final Target parentTarget2 = targetStore.createTarget(createTargetSpec(parentProbeId,
            PredefinedAccountDefinition.Address.name().toLowerCase(),
            "Parent2"));
        // Create 2 derived targets specs that should resolve to the same target since they have
        // the same value in the target identifying field
        final TargetSpec derivedTargetSpec1 = TargetSpec.newBuilder()
            .setProbeId(derivedProbeId)
            .setIsHidden(true)
            .addAllAccountValue(ImmutableList.of(
                createAccountValue(NAME_ACCT_DEF.getCustomDefinition().getName(), commonNameValue),
                createAccountValue(PredefinedAccountDefinition.Address.name().toLowerCase(),
                    derivedTargetAddress1)))
            .build();
        final TargetSpec derivedTargetSpec2 = TargetSpec.newBuilder()
            .setProbeId(derivedProbeId)
            .setIsHidden(true)
            .addAllAccountValue(ImmutableList.of(
                createAccountValue(NAME_ACCT_DEF.getCustomDefinition().getName(), commonNameValue),
                createAccountValue(PredefinedAccountDefinition.Address.name().toLowerCase(),
                    derivedTargetAddress2)))
            .build();
        targetStore.createOrUpdateDerivedTargets(Lists.newArrayList(derivedTargetSpec1),
            parentTarget1.getId());

        final Long derivedTargetId1 = verifyDerivedTargetCreation(derivedTargetSpec1);
        targetStore.createOrUpdateDerivedTargets(Lists.newArrayList(derivedTargetSpec2),
            parentTarget2.getId());
        final Long derivedTargetId2 = verifyDerivedTargetCreation(derivedTargetSpec2);
        assertNotNull(derivedTargetId1);
        assertEquals(derivedTargetId1, derivedTargetId2);
        final Target parentTarget1Updated = targetStore.getTarget(parentTarget1.getId()).get();
        final Target parentTarget2Updated = targetStore.getTarget(parentTarget2.getId()).get();
        assertEquals(1, parentTarget1Updated.getSpec().getDerivedTargetIdsCount());
        assertEquals(1, parentTarget2Updated.getSpec().getDerivedTargetIdsCount());
        // test that derived target shows up under both parents
        assertEquals(
            (Long)parentTarget1Updated.getSpec().getDerivedTargetIdsList().iterator().next(),
            derivedTargetId1);
        assertEquals(
            (Long)parentTarget2Updated.getSpec().getDerivedTargetIdsList().iterator().next(),
            derivedTargetId1);
        // Reprocess the derived target from Parent1 to force it to use the account values from
        // Parent1's target spec and check that the derived target is using the address specified
        // its target spec
        targetStore.createOrUpdateDerivedTargets(Lists.newArrayList(derivedTargetSpec1),
            parentTarget1.getId());
        final Target derivedTargetUpdate1 = targetStore.getTarget(derivedTargetId1).get();
        verifyAddressAccountValue(derivedTargetAddress1, derivedTargetUpdate1);
        // now remove one parent of the derived target and ensure that parent fields are
        // updated properly
        targetStore.createOrUpdateDerivedTargets(Collections.emptyList(),
            parentTarget1.getId());
        final Target derivedTargetUpdate2 = targetStore.getTarget(derivedTargetId1).get();
        final Target parentTarget1Update2 = targetStore.getTarget(parentTarget1.getId()).get();
        final Target parentTarget2Update2 = targetStore.getTarget(parentTarget2.getId()).get();
        assertEquals(0, parentTarget1Update2.getSpec().getDerivedTargetIdsCount());
        assertEquals(1, parentTarget2Update2.getSpec().getDerivedTargetIdsCount());
        assertEquals((Long)derivedTargetId1,
            parentTarget2Update2.getSpec().getDerivedTargetIdsList().iterator().next());
        // confirm that we are now using the account values from the derived target spec that came
        // from parentTarget2
        verifyAddressAccountValue(derivedTargetAddress2, derivedTargetUpdate2);
        // now delete the derived target for the only remaining parent and make sure it gets deleted
        targetStore.createOrUpdateDerivedTargets(Collections.emptyList(),
            parentTarget2.getId());
        assertFalse(targetStore.getTarget(derivedTargetId1).isPresent());

        // Test that Parent target deletion affects shared derived targets properly
        // Create derived target with 2 parent targets
        targetStore.createOrUpdateDerivedTargets(Lists.newArrayList(derivedTargetSpec1),
            parentTarget1.getId());
        targetStore.createOrUpdateDerivedTargets(Lists.newArrayList(derivedTargetSpec2),
            parentTarget2.getId());
        // delete first parent target
        targetStore.removeTargetAndBroadcastTopology(parentTarget1.getId(), topologyHandler,
            scheduler);
        // confirm that derived target is still in the target store
        final Long derivedTargetId3 = verifyDerivedTargetCreation(derivedTargetSpec2);
        final Target parentTarget2Update3 = targetStore.getTarget(parentTarget2.getId()).get();
        assertEquals(1, parentTarget2Update3.getSpec().getDerivedTargetIdsCount());
        assertEquals((Long)derivedTargetId3,
            parentTarget2Update3.getSpec().getDerivedTargetIdsList().iterator().next());
        assertTrue(targetStore.getTarget(derivedTargetId3).isPresent());
        // Delete only remaining parent of derived target
        targetStore.removeTargetAndBroadcastTopology(parentTarget2.getId(), topologyHandler,
            scheduler);
        // confirm that derived target is deleted
        assertFalse(targetStore.getTarget(derivedTargetId3).isPresent());
    }

    /**
     * Tests that when we update a parent target with a new derived target that differs from the old
     * one in a target identifying field, we get a new derived target and the old one is removed.
     *
     * @throws IdentityStoreException if the targetStore throws one.
     * @throws DuplicateTargetException if the targetStore throws one.
     * @throws InvalidTargetException if the targetStore throws one.
     */
    @Test
    public void testDerivedTargetChangesIdentifyingFieldValue()
        throws IdentityStoreException, DuplicateTargetException, InvalidTargetException {
        final long parentProbeId = 1L;
        final long derivedProbeId = 2L;
        final String firstNameValue = "foo";
        final String secondNameValue = "bar";
        final String derivedTargetAddress = "Address1";

        when(probeStore.getProbe(parentProbeId)).thenReturn(Optional.of(parentProbeInfo));
        when(probeStore.getProbe(derivedProbeId)).thenReturn(Optional.of(derivedProbeInfo));

        final Target parentTarget = targetStore.createTarget(createTargetSpec(parentProbeId,
            PredefinedAccountDefinition.Address.name().toLowerCase(),
            "Parent1"));
        // Create 2 derived targets specs that should resolve to different derived targets
        final TargetSpec derivedTargetSpec1 = TargetSpec.newBuilder()
            .setProbeId(derivedProbeId)
            .setIsHidden(true)
            .addAllAccountValue(ImmutableList.of(
                createAccountValue(NAME_ACCT_DEF.getCustomDefinition().getName(), firstNameValue),
                createAccountValue(PredefinedAccountDefinition.Address.name().toLowerCase(),
                    derivedTargetAddress)))
            .build();
        final TargetSpec derivedTargetSpec2 = TargetSpec.newBuilder()
            .setProbeId(derivedProbeId)
            .setIsHidden(true)
            .addAllAccountValue(ImmutableList.of(
                createAccountValue(NAME_ACCT_DEF.getCustomDefinition().getName(), secondNameValue),
                createAccountValue(PredefinedAccountDefinition.Address.name().toLowerCase(),
                    derivedTargetAddress)))
            .build();
        targetStore.createOrUpdateDerivedTargets(Lists.newArrayList(derivedTargetSpec1),
            parentTarget.getId());

        final Long derivedTargetId1 = verifyDerivedTargetCreation(derivedTargetSpec1);
        targetStore.createOrUpdateDerivedTargets(Lists.newArrayList(derivedTargetSpec2),
            parentTarget.getId());
        final Long derivedTargetId2 = verifyDerivedTargetCreation(derivedTargetSpec2);
        assertTrue(derivedTargetId1 != derivedTargetId2);
        assertFalse(targetStore.getTarget(derivedTargetId1).isPresent());
        final Optional<Target> updatedParentTarget = targetStore.getTarget(parentTarget.getId());
        assertTrue(updatedParentTarget.isPresent());
        assertEquals(1, updatedParentTarget.get().getSpec().getDerivedTargetIdsCount());
        assertEquals(derivedTargetId2,
            updatedParentTarget.get().getSpec().getDerivedTargetIdsList().iterator().next());
    }
}