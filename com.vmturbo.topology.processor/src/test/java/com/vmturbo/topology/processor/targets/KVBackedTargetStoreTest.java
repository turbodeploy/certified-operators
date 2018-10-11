package com.vmturbo.topology.processor.targets;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.mockito.AdditionalAnswers;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableSet;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import com.google.protobuf.util.JsonFormat;
import com.vmturbo.components.crypto.CryptoFacility;
import com.vmturbo.identity.store.IdentityStore;
import com.vmturbo.kvstore.KeyValueStore;
import com.vmturbo.platform.common.dto.Discovery.AccountDefEntry;
import com.vmturbo.platform.common.dto.Discovery.AccountValue;
import com.vmturbo.platform.common.dto.Discovery.CustomAccountDefEntry;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.platform.sdk.common.PredefinedAccountDefinition;
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
 * Test the {@link KVBackedTargetStore}.
 */
public class KVBackedTargetStoreTest {

    private static final String FIELD_NAME = "targetId";

    private KeyValueStore keyValueStore;

    private ProbeStore probeStore;

    private GroupScopeResolver groupScopeResolver;

    private IdentityStore<TargetSpec> targetIdentityStore;

    private KVBackedTargetStore targetStore;

    private ProbeInfo probeInfo;

    final TopologyHandler topologyHandler = Mockito.mock(TopologyHandler.class);

    final Scheduler scheduler = Mockito.mock(Scheduler.class);

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Rule
    public TemporaryFolder testFolder = new TemporaryFolder();

    @Before
    public void setup() throws Exception{
        System.setProperty("com.vmturbo.keydir", testFolder.newFolder().getAbsolutePath());
        probeInfo = Probes.emptyProbe;
        keyValueStore = Mockito.mock(KeyValueStore.class);
        probeStore = Mockito.mock(ProbeStore.class);
        groupScopeResolver = Mockito.mock(GroupScopeResolver.class);
        Mockito.when(groupScopeResolver.processGroupScope(any(), any()))
                .then(AdditionalAnswers.returnsFirstArg());
        targetIdentityStore = new TestIdentityStore<>(new TargetSpecAttributeExtractor(probeStore));
        targetStore = new KVBackedTargetStore(keyValueStore, probeStore, targetIdentityStore,
                groupScopeResolver);
    }

    /**
     * Test that creating a target calls the expected methods on the {@link KeyValueStore}.
     *
     * @throws Exception If something goes wrong.
     */
    @Test
    public void testCreateTarget() throws Exception {
        Mockito.when(probeStore.getProbe(Mockito.anyLong())).thenReturn(Optional.of(probeInfo));

        final TargetRESTApi.TargetSpec spec = new TargetRESTApi.TargetSpec(0L, Collections.emptyList());

        final Target target = targetStore.createTarget(spec.toDto());

        verify(keyValueStore).put(Mockito.eq("targets/" + target.getId()), any());

        targetStore.getTarget(target.getId()).get();
    }

    @Test
    public void testGetProbeName() throws Exception {
        AccountDefEntry plain = AccountDefEntry.newBuilder()
            .setCustomDefinition(
                CustomAccountDefEntry.newBuilder()
                    .setName(PredefinedAccountDefinition.Address.name().toLowerCase())
                    .setDisplayName("this is my address")
                    .setDescription("The address")
                    .setIsSecret(false))
            .setMandatory(true)
            .build();

        ProbeInfo pi = ProbeInfo.newBuilder()
            .setProbeCategory("test")
            .setProbeType("vc")
            .addTargetIdentifierField(PredefinedAccountDefinition.Address.name().toLowerCase())
            .addAccountDefinition(plain)
            .build();

        Mockito.when(probeStore.getProbe(Mockito.anyLong())).thenReturn(Optional.of(pi));

        final TargetRESTApi.TargetSpec spec = new TargetRESTApi.TargetSpec(0L, Collections.singletonList(
            new InputField(PredefinedAccountDefinition.Address.name().toLowerCase(), "foo", Optional.empty())));
        final Target target = targetStore.createTarget(spec.toDto());

        Assert.assertEquals("foo", targetStore.getTargetAddress(target.getId()).get());
    }

    @Test
    public void testCreateTargetNotifiesListeners() throws Exception {
        Mockito.when(probeStore.getProbe(Mockito.anyLong())).thenReturn(Optional.of(probeInfo));
        TargetStoreListener listener = Mockito.mock(TargetStoreListener.class);
        targetStore.addListener(listener);

        final TargetRESTApi.TargetSpec spec = new TargetRESTApi.TargetSpec(0L, Collections.emptyList());
        Target target = targetStore.createTarget(spec.toDto());

        verify(listener).onTargetAdded(target);
    }

    /**
     * Test that constructing a {@link KVBackedTargetStore} retrieves saved targets from
     * the associated {@link KeyValueStore}.
     *
     * @throws Exception If something goes wrong.
     */
    @Test
    public void testInitialization() throws Exception {
        Mockito.when(probeStore.getProbe(Mockito.anyLong())).thenReturn(Optional.of(probeInfo));
        final long targetId = 0L;
        final TargetRESTApi.TargetSpec spec = new TargetRESTApi.TargetSpec(targetId, Collections.emptyList());

        final Target target = new Target(targetId, probeStore, spec.toDto(), false);

        final KeyValueStore kvStore = Mockito.mock(KeyValueStore.class);

        Mockito.when(kvStore.getByPrefix(Mockito.eq("targets/"))).thenReturn(
                Collections.singletonMap(String.valueOf(targetId), target.toJsonString()));
        final KVBackedTargetStore newTargetStore = new KVBackedTargetStore(kvStore, probeStore,
                targetIdentityStore, groupScopeResolver);
        verify(kvStore).getByPrefix(Mockito.eq("targets/"));
        newTargetStore.getTarget(0L).get();
    }

    /**
     * Test that constructing a {@link KVBackedTargetStore} retrieves saved targets from
     * the associated {@link KeyValueStore}, while making sure that encrypted fields are in fact encrypted.
     *
     * @throws Exception If something goes wrong.
     */
    @Test
    public void testEncryptedFields() throws Exception {
        AccountDefEntry secret = AccountDefEntry.newBuilder()
                                                .setCustomDefinition(
                                                        CustomAccountDefEntry.newBuilder()
                                                                             .setName("password")
                                                                             .setDisplayName(
                                                                                 "Password")
                                                                             .setDescription("The user password")
                                                                             .setIsSecret(true))
                                                .setMandatory(true)
                                                .build();
        AccountDefEntry plain = AccountDefEntry.newBuilder()
                                               .setCustomDefinition(
                                                       CustomAccountDefEntry.newBuilder()
                                                                            .setName("user")
                                                                            .setDisplayName(
                                                                                "userName")
                                                                            .setDescription("The user name")
                                                                            .setIsSecret(false))
                                               .setMandatory(true)
                                               .build();
        ProbeInfo pi = ProbeInfo.newBuilder()
                                .setProbeCategory("test")
                                .setProbeType("vc")
                                .addTargetIdentifierField("name")
                                .addAccountDefinition(secret)
                                .addAccountDefinition(plain)
                                .build();

        Mockito.when(probeStore.getProbe(Mockito.anyLong())).thenReturn(Optional.of(pi));

        final TargetRESTApi.TargetSpec spec = new TargetRESTApi.TargetSpec(0L, Arrays.asList(
                new InputField("password", "ThePassValue", Optional.empty()),
                new InputField("user", "theUserName", Optional.empty())));
        final Target target = new Target(0L, probeStore, spec.toDto(), true);
        final KeyValueStore kvStore = Mockito.mock(KeyValueStore.class);

        Mockito.when(kvStore.getByPrefix(Mockito.eq("targets/"))).thenReturn(
                Collections.singletonMap("0", target.toJsonString()));
        String value = kvStore.getByPrefix("targets/").get("0");
        Gson gson = new GsonBuilder()
                .registerTypeAdapter(InternalTargetInfo.class, new InternalTargetInfoAdapter())
                .create();
        InternalTargetInfo itf = gson.fromJson(value, InternalTargetInfo.class);
        // The secret fields are encrypted
        Assert.assertFalse("password".equals(itf.secretFields.iterator().next()));
        String decryptedSecretField = CryptoFacility.decrypt(itf.secretFields.iterator().next());
        Assert.assertEquals("password", decryptedSecretField);

        for (TopologyProcessorDTO.AccountValue av : itf.targetInfo.getSpec().getAccountValueList()) {
            if (av.getKey().equals(decryptedSecretField)) {
                // The password field is encrypted
                Assert.assertFalse("ThePassValue".equals(av.getStringValue()));
                Assert.assertEquals("ThePassValue", CryptoFacility.decrypt(av.getStringValue()));
            } else {
                // The user name field is not encrypted.
                Assert.assertEquals("theUserName", av.getStringValue());
            }
        }

    }

    private static class InternalTargetInfo {
        final TopologyProcessorDTO.TargetInfo targetInfo;

        final Set<String> secretFields;

        InternalTargetInfo(@Nonnull final TopologyProcessorDTO.TargetInfo targetInfo,
                           @Nonnull final Set<String> secretFields) {
            this.targetInfo = targetInfo;
            this.secretFields = secretFields;
        }
    }

    /**
     * GSON adapter to serialize {@link Target.InternalTargetInfo}.
     */
    private static class InternalTargetInfoAdapter extends TypeAdapter<InternalTargetInfo> {
        @Override
        public void write(JsonWriter out, InternalTargetInfo value) throws IOException {
        }

        @Override
        public InternalTargetInfo read(JsonReader in) throws IOException {
            final ImmutableSet.Builder<String> secretFieldBuilder = new ImmutableSet.Builder<>();
            in.beginObject();
            in.nextName();
            in.beginArray();
            while (in.hasNext()) {
                secretFieldBuilder.add(in.nextString());
            }
            in.endArray();
            in.endObject();

            in.beginObject();
            in.nextName();
            final String serializedTarget = in.nextString();
            final TopologyProcessorDTO.TargetInfo.Builder builder = TopologyProcessorDTO.TargetInfo
                    .newBuilder();
            JsonFormat.parser().merge(serializedTarget, builder);
            in.endObject();
            return new InternalTargetInfo(builder.build(), secretFieldBuilder.build());
        }
    }

    /**
     * Test that constructing a {@link KVBackedTargetStore} works even when there are
     * targets with serialized {@link AccountValue} fields.
     *
     * @throws Exception If something goes wrong.
     */
    @Test
    public void testInitializationWithAccountVals() throws Exception {
        final Target target = prepareTestTarget();
        final KeyValueStore kvStore = prepareKvStoreWithTarget(target);

        final KVBackedTargetStore newTargetStore = new KVBackedTargetStore(kvStore, probeStore,
                targetIdentityStore, groupScopeResolver);
        verify(kvStore).getByPrefix(Mockito.eq("targets/"));

        final Target retTarget = newTargetStore.getTarget(0L).get();
        Assert.assertEquals(target.getId(), retTarget.getId());
        Assert.assertEquals(target.getProbeId(), retTarget.getProbeId());
        Assert.assertEquals(1, target.getMediationAccountVals().size());

        final AccountValue accountVal = target.getMediationAccountVals().get(0);
        Assert.assertEquals("name", accountVal.getKey());
        Assert.assertEquals("value", accountVal.getStringValue());
        Assert.assertEquals(1, accountVal.getGroupScopePropertyValuesCount());
        Assert.assertEquals("test", accountVal.getGroupScopePropertyValues(0).getValue(0));
    }

    @Test
    public void testInvalidSerializedTarget() throws Exception {
        Mockito.when(keyValueStore.getByPrefix(Mockito.eq("targets/")))
                .thenReturn(Collections.singletonMap("targets/0", "aoishtioa"));

        // Instantiating a KVBackedStore should work.
        new KVBackedTargetStore(keyValueStore, probeStore, targetIdentityStore, groupScopeResolver);
    }

    @Test
    public void testRemoveListenerWhenPresent() throws Exception {
        final TargetStoreListener listener = Mockito.mock(TargetStoreListener.class);

        targetStore.addListener(listener);
        assertTrue(targetStore.removeListener(listener));
    }

    @Test
    public void testRemoveListenerWhenAbsent() throws Exception {
        assertFalse(targetStore.removeListener(Mockito.mock(TargetStoreListener.class)));
    }

    private Target prepareTestTarget() throws Exception {
        final ProbeInfo probeInfo = ProbeInfo.newBuilder(this.probeInfo)
            .addAccountDefinition(AccountDefEntry.newBuilder().setCustomDefinition(
                 CustomAccountDefEntry.newBuilder()
                     .setName("name")
                     .setDisplayName("displayName")
                     .setDescription("desc"))
                .setMandatory(true))
            .build();
        Mockito.when(probeStore.getProbe(Mockito.anyLong())).thenReturn(Optional.of(probeInfo));

        final TargetRESTApi.TargetSpec spec = new TargetRESTApi.TargetSpec(0L,
            Collections.singletonList(
                new InputField("name",
                    "value",
                    Optional.of(Collections.singletonList(Collections.singletonList("test"))))));
        return new Target(0L, probeStore, spec.toDto(), true);
    }

    private KeyValueStore prepareKvStoreWithTarget(Target target) throws Exception {
        final KeyValueStore kvStore = Mockito.mock(KeyValueStore.class);
        Mockito.when(kvStore.getByPrefix(Mockito.eq("targets/"))).thenReturn(Collections.singletonMap("0", target.toJsonString()));

        return kvStore;
    }

    private void prepareInitialProbe() throws Exception {
        final ProbeInfo probeInfo = ProbeInfo.newBuilder(this.probeInfo)
                        .addTargetIdentifierField(FIELD_NAME)
                        .addAccountDefinition(AccountDefEntry.newBuilder()
                                        .setCustomDefinition(CustomAccountDefEntry.newBuilder()
                                                        .setName(FIELD_NAME)
                                                        .setDisplayName("displayName")
                                                        .setDescription("desc"))
                                        .setMandatory(true))
                        .build();
        Mockito.when(probeStore.getProbe(Mockito.anyLong())).thenReturn(Optional.of(probeInfo));
    }

    private TargetSpec createTargetSpec(long probeId, int fieldValue) throws Exception {
        final TargetSpec.Builder builder = TargetSpec.newBuilder().setProbeId(probeId);
        builder.addAllAccountValue(createAccountValue(fieldValue));
        return builder.build();
    }

    private Collection<TopologyProcessorDTO.AccountValue> createAccountValue(int fieldValue) {
        return Collections.singleton(TopologyProcessorDTO.AccountValue.newBuilder().setKey(FIELD_NAME)
                        .setStringValue(Integer.toString(fieldValue)).build());
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
        Assert.assertEquals("1",
                        target.getMediationAccountVals().iterator().next().getStringValue());
        target = targetStore.updateTarget(target.getId(), targetSpec.getAccountValueList());
        Assert.assertEquals("1",
                        target.getMediationAccountVals().iterator().next().getStringValue());

        final Collection<TopologyProcessorDTO.AccountValue> targetFieldsNew = createAccountValue(2);
        target = targetStore.updateTarget(target.getId(), targetFieldsNew);
        Assert.assertEquals("2",
                        target.getMediationAccountVals().iterator().next().getStringValue());
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
                .setMandatory(true))
            .build();
        Mockito.when(probeStore.getProbe(Mockito.anyLong())).thenReturn(Optional.of(probeInfo));

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

    private void assertAccountValueEquals(@Nonnull final Target target,
                                          @Nonnull final String keyName,
                                          @Nonnull final String expectedValueName) {
        Assert.assertEquals(expectedValueName, target.getMediationAccountVals().stream()
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
        final TargetStoreListener targetListener = Mockito.mock(TargetStoreListener.class);
        final TargetSpec targetSpec = createTargetSpec(0, 1);
        final Target target = targetStore.createTarget(targetSpec);
        targetStore.addListener(targetListener);
        Assert.assertEquals("1",
                target.getMediationAccountVals().iterator().next().getStringValue());
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
        final long derivedProbeId = 1L;
        final TargetSpec derivedTargetSpec1 = TargetSpec.newBuilder()
                .setProbeId(derivedProbeId)
                .addAllAccountValue(createAccountValue(555))
                .build();
        final TargetSpec derivedTargetSpec2 = TargetSpec.newBuilder()
                .setProbeId(derivedProbeId)
                .addAllAccountValue(createAccountValue(2333))
                .setParentId(parent.getId())
                .setIsHidden(true)
                .build();

        // Derived target 1 is not dependent on parent target.
        final Target derived1 = targetStore.createTarget(derivedTargetSpec1);
        // Derived target 2 is dependent on parent target.
        final Target derived2 = targetStore.createTarget(derivedTargetSpec2);
        Assert.assertEquals(3, targetStore.getAll().size());

        targetStore.removeTargetAndBroadcastTopology(parent.getId(), topologyHandler, scheduler);
        Assert.assertTrue(targetStore.getTarget(derived1.getId()).isPresent());
        Assert.assertFalse(targetStore.getTarget(derived2.getId()).isPresent());
        Assert.assertEquals(1, targetStore.getAll().size());
    }
}