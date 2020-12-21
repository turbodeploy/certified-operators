package com.vmturbo.topology.processor.targets;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import com.google.protobuf.util.JsonFormat;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import com.vmturbo.components.crypto.CryptoFacility;
import com.vmturbo.kvstore.KeyValueStore;
import com.vmturbo.kvstore.MapKeyValueStore;
import com.vmturbo.platform.common.dto.Discovery.AccountDefEntry;
import com.vmturbo.platform.common.dto.Discovery.AccountValue;
import com.vmturbo.platform.common.dto.Discovery.CustomAccountDefEntry;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO;
import com.vmturbo.topology.processor.api.dto.InputField;
import com.vmturbo.topology.processor.api.impl.TargetRESTApi;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.targets.Target.InternalTargetInfo;
import com.vmturbo.topology.processor.util.Probes;

/**
 * Unit tests for {@link KvTargetDao}.
 */
public class KvTargetDaoTest {

    private ProbeStore probeStore;

    private GroupScopeResolver groupScopeResolver;

    /**
     * Temporary folder to use for encryption/decryption.
     */
    @Rule
    public TemporaryFolder testFolder = new TemporaryFolder();

    /**
     * Common code to run before every test.
     *
     * @throws Exception To satisfy compiler.
     */
    @Before
    public void setup() throws Exception {
        System.setProperty("com.vmturbo.keydir", testFolder.newFolder().getAbsolutePath());
        probeStore = mock(ProbeStore.class);
        groupScopeResolver = mock(GroupScopeResolver.class);
    }

    /**
     * Test adding and retrieving entries to/from the DAO.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testPutGet() throws Exception {
        final Target target = prepareTestTarget();
        final KeyValueStore kvStore = new MapKeyValueStore();
        final KvTargetDao dao = new KvTargetDao(kvStore, probeStore);
        dao.store(target);
        final Target retrievedTarget = dao.getAll().get(0);
        assertThat(retrievedTarget.getNoSecretDto(), is(target.getNoSecretDto()));
    }

    /**
     * Test removing entries from the DAO.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testRemove() throws Exception {
        final Target target = prepareTestTarget();
        final KeyValueStore kvStore = new MapKeyValueStore();
        final KvTargetDao dao = new KvTargetDao(kvStore, probeStore);
        dao.store(target);
        assertThat(dao.getAll().size(), is(1));
        dao.remove(target.getId());
        assertThat(dao.getAll().size(), is(0));
    }

    /**
     * Test that constructing a {@link CachingTargetStore} works even when there are
     * targets with serialized {@link AccountValue} fields.
     *
     * @throws Exception If something goes wrong.
     */
    @Test
    public void testInitializationWithAccountVals() throws Exception {
        final Target target = prepareTestTarget();
        final KeyValueStore kvStore = new MapKeyValueStore();
        final KvTargetDao kvTargetDao = new KvTargetDao(kvStore, probeStore);

        kvTargetDao.store(target);

        final Target retTarget = kvTargetDao.getAll().get(0);
        Assert.assertEquals(target.getId(), retTarget.getId());
        Assert.assertEquals(target.getProbeId(), retTarget.getProbeId());
        Assert.assertEquals(2, target.getMediationAccountVals(groupScopeResolver).size());

        final AccountValue accountVal = target.getMediationAccountVals(groupScopeResolver).get(0);
        Assert.assertEquals("name", accountVal.getKey());
        Assert.assertEquals("value", accountVal.getStringValue());
        Assert.assertEquals(1, accountVal.getGroupScopePropertyValuesCount());
        Assert.assertEquals("test", accountVal.getGroupScopePropertyValues(0).getValue(0));
    }

    private static final String SECRET_FIELD_NAME = "password";
    private static final String PLAIN_FIELD_NAME = "user";

    private static final AccountDefEntry SECRET_ACCOUNT_ENTRY = AccountDefEntry.newBuilder()
        .setCustomDefinition(
            CustomAccountDefEntry.newBuilder()
                .setName(SECRET_FIELD_NAME)
                .setDisplayName(
                    "Password")
                .setDescription("The user password")
                .setIsSecret(true))
        .setMandatory(true)
        .build();

    private static final AccountDefEntry PLAIN_ACCOUNT_ENTRY = AccountDefEntry.newBuilder()
        .setCustomDefinition(
            CustomAccountDefEntry.newBuilder()
                .setName(PLAIN_FIELD_NAME)
                .setDisplayName(
                    "userName")
                .setDescription("The user name")
                .setIsSecret(false))
        .setMandatory(true)
        .build();

    private static final ProbeInfo PROBE_INFO = createProbeInfo("test", "vc",
        PLAIN_FIELD_NAME, ImmutableList.of(SECRET_ACCOUNT_ENTRY, PLAIN_ACCOUNT_ENTRY));

    /**
     * Test that constructing a {@link CachingTargetStore} retrieves saved targets from
     * the associated {@link KeyValueStore}, while making sure that encrypted fields are in fact encrypted.
     *
     * @throws Exception If something goes wrong.
     */
    @Test
    public void testEncryptedFields() throws Exception {

        Mockito.when(probeStore.getProbe(Mockito.anyLong())).thenReturn(Optional.of(PROBE_INFO));

        final TargetRESTApi.TargetSpec spec = new TargetRESTApi.TargetSpec(0L, Arrays.asList(
            new InputField(SECRET_FIELD_NAME, "ThePassValue", Optional.empty()),
            new InputField(PLAIN_FIELD_NAME, "theUserName", Optional.empty())), Optional.empty());
        final Target target = new Target(0L, probeStore, spec.toDto(), true);


        KeyValueStore kvStore = new MapKeyValueStore();
        KvTargetDao dao = new KvTargetDao(kvStore, probeStore);
        dao.store(target);

        // Test to verify that IN THE KvStore the secret fields were encrypted.
        String value = kvStore.getByPrefix(TargetStore.TARGET_KV_STORE_PREFIX).values().iterator().next();
        Gson gson = new GsonBuilder()
            .registerTypeAdapter(InternalTargetInfo.class, new NonDecryptingInternalTargetInfoAdapter())
            .create();
        InternalTargetInfo itf = gson.fromJson(value, InternalTargetInfo.class);
        // The secret fields are encrypted
        Assert.assertFalse(SECRET_FIELD_NAME.equals(itf.secretFields.iterator().next()));
        String decryptedSecretField = CryptoFacility.decrypt(itf.secretFields.iterator().next());
        Assert.assertEquals(SECRET_FIELD_NAME, decryptedSecretField);

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

    private Target prepareTestTarget() throws Exception {
        final ProbeInfo probeInfo = ProbeInfo.newBuilder(Probes.emptyProbe)
            .addAccountDefinition(AccountDefEntry.newBuilder().setCustomDefinition(
                CustomAccountDefEntry.newBuilder()
                    .setName("name")
                    .setDisplayName("displayName")
                    .setDescription("desc"))
                .setMandatory(true))
            .build();
        when(probeStore.getProbe(Mockito.anyLong())).thenReturn(Optional.of(probeInfo));

        final TargetRESTApi.TargetSpec spec = new TargetRESTApi.TargetSpec(0L,
            Arrays.asList(new InputField("name", "value",
                Optional.of(Collections.singletonList(Collections.singletonList("test")))),
                new InputField("targetId", "value",
                    Optional.of(Collections.singletonList(Collections.singletonList("test"))))), Optional.empty());
        return new Target(0L, probeStore, spec.toDto(), true);
    }

    /**
     * GSON adapter to serialize {@link Target.InternalTargetInfo}.
     */
    private static class NonDecryptingInternalTargetInfoAdapter extends TypeAdapter<InternalTargetInfo> {
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

    private static ProbeInfo createProbeInfo(@Nonnull String category,
                                             @Nonnull String type,
                                             @Nonnull String targetIdentifyingFieldName,
                                             @Nonnull List<AccountDefEntry> accountDefs) {
        return ProbeInfo.newBuilder()
            .setUiProbeCategory(category)
            .setProbeCategory(category)
            .setProbeType(type)
            .addTargetIdentifierField(targetIdentifyingFieldName)
            .addAllAccountDefinition(accountDefs)
            .build();
    }

}
