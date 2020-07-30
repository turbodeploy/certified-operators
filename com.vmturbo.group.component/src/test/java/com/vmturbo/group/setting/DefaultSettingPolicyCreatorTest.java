package com.vmturbo.group.setting;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.lessThan;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import com.google.common.collect.Sets;

import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.setting.SettingProto.BooleanSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.BooleanSettingValueType;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingScope;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingScope.AllEntityType;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingScope.EntityTypeSet;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingSpec;
import com.vmturbo.common.protobuf.setting.SettingProto.EnumSettingValueType;
import com.vmturbo.common.protobuf.setting.SettingProto.GlobalSettingSpec;
import com.vmturbo.common.protobuf.setting.SettingProto.NumericSettingValueType;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy.Type;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicyInfo;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingSpec;
import com.vmturbo.common.protobuf.setting.SettingProto.SortedSetOfOidSettingValueType;
import com.vmturbo.common.protobuf.setting.SettingProto.StringSettingValueType;
import com.vmturbo.group.group.IGroupStore;
import com.vmturbo.group.identity.IdentityProvider;
import com.vmturbo.group.service.MockTransactionProvider;
import com.vmturbo.platform.common.dto.CommonDTOREST.EntityDTO.EntityType;
import com.vmturbo.topology.processor.api.util.ThinTargetCache;

/**
 * Tests creation of default setting policies.
 */
public class DefaultSettingPolicyCreatorTest {

    private ISettingPolicyStore settingStore;
    private SettingSpecStore settingSpecStore;
    private DefaultSettingPolicyCreator settingPolicyCreator;
    private static final BooleanSettingValueType TRUE =
            BooleanSettingValueType.newBuilder().setDefault(true).build();
    private static final String SPEC_NAME = "specNameFoo";
    private SettingSpec defaultSetting;
    private MockTransactionProvider transactionProvider;

    /**
     * Initialises the tests.
     */
    @Before
    public void setUp() {
        transactionProvider = new MockTransactionProvider();
        settingStore = transactionProvider.getSettingPolicyStore();
        settingSpecStore = Mockito.mock(SettingSpecStore.class);
        defaultSetting = SettingSpec.newBuilder()
                .setName(SPEC_NAME)
                .setBooleanSettingValueType(TRUE)
                .setEntitySettingSpec(entitySettingSpec(11))
                .build();
    }

    /**
     * Tests default setting creation for boolean setting.
     *
     * @throws Exception if exception occurs
     */
    @Test
    public void testDefSettingFromSpecBoolean() throws Exception {
        final SettingSpec spec = SettingSpec.newBuilder(defaultSetting)
                .setBooleanSettingValueType(TRUE)
                .build();
        final SettingPolicyInfo info = getPolicyInfo(spec);
        Assert.assertEquals(1, info.getSettingsCount());
        final Setting setting = info.getSettings(0);
        Assert.assertEquals(spec.getName(), setting.getSettingSpecName());
        Assert.assertEquals(true, setting.getBooleanSettingValue().getValue());
    }

    /**
     * Tests default setting creation for numeric setting.
     *
     * @throws Exception if exception occurs
     */
    @Test
    public void testDefSettingFromSpecNumeric() throws Exception {
        final SettingSpec spec = SettingSpec.newBuilder(defaultSetting)
                .setNumericSettingValueType(NumericSettingValueType.newBuilder().setDefault(11f))
                .build();
        final SettingPolicyInfo info = getPolicyInfo(spec);
        Assert.assertEquals(1, info.getSettingsCount());
        final Setting setting = info.getSettings(0);
        Assert.assertEquals(spec.getName(), setting.getSettingSpecName());
        Assert.assertEquals(11f, setting.getNumericSettingValue().getValue(), 0.00001);
    }

    /**
     * Tests default setting creation for string setting.
     *
     * @throws Exception if exception occurs
     */
    @Test
    public void testDefSettingFromSpecString() throws Exception {
        final SettingSpec spec = SettingSpec.newBuilder(defaultSetting)
                .setStringSettingValueType(StringSettingValueType.newBuilder().setDefault("bar"))
                .build();
        final SettingPolicyInfo info = getPolicyInfo(spec);
        final Setting setting = info.getSettings(0);
        Assert.assertEquals(spec.getName(), setting.getSettingSpecName());
        Assert.assertEquals("bar", setting.getStringSettingValue().getValue());
    }

    /**
     * Tests default setting creation for enum setting.
     *
     * @throws Exception if exception occurs
     */
    @Test
    public void testDefSettingFromSpecEnum() throws Exception {
        final SettingSpec spec = SettingSpec.newBuilder(defaultSetting)
                .setEnumSettingValueType(EnumSettingValueType.newBuilder()
                        .setDefault("ENUM")
                        .addAllEnumValues(Arrays.asList("ENUM", "VALUE1")))
                .build();
        final SettingPolicyInfo info = getPolicyInfo(spec);
        Assert.assertEquals(1, info.getSettingsCount());
        final Setting setting = info.getSettings(0);
        Assert.assertEquals(spec.getName(), setting.getSettingSpecName());
        Assert.assertEquals("ENUM", setting.getEnumSettingValue().getValue());
    }

    /**
     * Tests default setting creation for list setting.
     *
     * @throws Exception if exception occurs
     */
    @Test
    public void testDefSettingFromSpecList() throws Exception {
        final SettingSpec spec = SettingSpec.newBuilder(defaultSetting)
                .setSortedSetOfOidSettingValueType(SortedSetOfOidSettingValueType.newBuilder()
                        .setType(SortedSetOfOidSettingValueType.Type.ENTITY)
                        .addAllDefault(Collections.singletonList(1L)))
                .build();
        final SettingPolicyInfo info = getPolicyInfo(spec);
        Assert.assertEquals(1, info.getSettingsCount());
        final Setting setting = info.getSettings(0);
        Assert.assertEquals(spec.getName(), setting.getSettingSpecName());
        Assert.assertEquals(Sets.newHashSet(1L),
                Sets.newHashSet(setting.getSortedSetOfOidSettingValue().getOidsList()));
    }

    /**
     * Tests non-entity specific settings specification. No setting policies are expected to be
     * produced.
     *
     * @throws Exception if exception occurs
     */
    @Test
    public void testDefSettingPoliciesFromSpecsIgnoresNonEntitySpecs() throws Exception {
        final SettingSpec globalSpec = SettingSpec.newBuilder()
                .setGlobalSettingSpec(GlobalSettingSpec.getDefaultInstance())
                .setBooleanSettingValueType(BooleanSettingValueType.getDefaultInstance())
                .build();
        final SettingSpec allEntityTypeSpec = SettingSpec.newBuilder()
                .setEntitySettingSpec(EntitySettingSpec.newBuilder()
                        .setEntitySettingScope(EntitySettingScope.newBuilder()
                                .setAllEntityType(AllEntityType.getDefaultInstance())))
                .build();
        Mockito.when(settingSpecStore.getAllSettingSpecs())
                .thenReturn(Arrays.asList(globalSpec, allEntityTypeSpec));
        settingPolicyCreator =
                new DefaultSettingPolicyCreator(settingSpecStore, transactionProvider, 10,
                        new IdentityProvider(0));
        settingPolicyCreator.run();
        @SuppressWarnings("unchecked")
        final ArgumentCaptor<Collection<SettingPolicy>> policyCaptor =
                (ArgumentCaptor)ArgumentCaptor.forClass(Collection.class);
        Mockito.verify(settingStore).createSettingPolicies(policyCaptor.capture());
        Assert.assertEquals(Collections.emptySet(), new HashSet<>(policyCaptor.getValue()));
    }

    /**
     * Tests settings specifications with different default values (entity-specific). It is expected
     * that every of the specified entity types will have its own default value.
     *
     * @throws Exception if exception occurs
     */
    @Test
    public void testEntitySpecificDefault() throws Exception {
        final SettingSpec spec = SettingSpec.newBuilder(defaultSetting)
                .setNumericSettingValueType(NumericSettingValueType.newBuilder()
                        .setMin(10f)
                        .setMax(20f)
                        .setDefault(11f)
                        .putEntityDefaults(1, 12)
                        .putEntityDefaults(2, 13))
                .setEntitySettingSpec(entitySettingSpec(1, 2, 3))
                .build();
        final Collection<SettingPolicyInfo> policyInfos = getPolicyInfo(3, spec);
        final SettingPolicyInfo policyInfo1 = policyInfos.stream()
                .filter(policy -> policy.getEntityType() == 1)
                .findFirst()
                .get();
        Assert.assertEquals(12, policyInfo1.getSettings(0).getNumericSettingValue().getValue(),
                0.0001);
        final SettingPolicyInfo policyInfo2 = policyInfos.stream()
                .filter(policy -> policy.getEntityType() == 2)
                .findFirst()
                .get();
        Assert.assertEquals(13, policyInfo2.getSettings(0).getNumericSettingValue().getValue(),
                0.0001);
        final SettingPolicyInfo policyInfo3 = policyInfos.stream()
                .filter(policy -> policy.getEntityType() == 3)
                .findFirst()
                .get();
        Assert.assertEquals(11, policyInfo3.getSettings(0).getNumericSettingValue().getValue(),
                0.0001);
    }

    /**
     * Tests creating of default setting policies from specifications.
     *
     * @throws Exception on exceptions faced
     */
    @Test
    public void testDefSettingPoliciesFromSpec() throws Exception {
        final SettingSpec spec1 = SettingSpec.newBuilder()
                .setName("foo")
                .setBooleanSettingValueType(TRUE)
                .setEntitySettingSpec(EntitySettingSpec.newBuilder()
                        .setEntitySettingScope(EntitySettingScope.newBuilder()
                                .setEntityTypeSet(EntityTypeSet.newBuilder().addEntityType(10))))
                .build();
        final SettingSpec spec2 = SettingSpec.newBuilder()
                .setName("bar")
                .setBooleanSettingValueType(TRUE)
                .setEntitySettingSpec(EntitySettingSpec.newBuilder()
                        .setEntitySettingScope(EntitySettingScope.newBuilder()
                                .setEntityTypeSet(EntityTypeSet.newBuilder().addEntityType(10))))
                .build();
        final Setting setting1 = Setting.newBuilder()
                .setSettingSpecName(spec1.getName())
                .setBooleanSettingValue(BooleanSettingValue.newBuilder().setValue(true))
                .build();
        final Setting setting2 = Setting.newBuilder()
                .setSettingSpecName(spec2.getName())
                .setBooleanSettingValue(BooleanSettingValue.newBuilder().setValue(true))
                .build();

        final SettingPolicyInfo info = getPolicyInfo(spec1, spec2);
        Assert.assertEquals("Virtual Machine Defaults", info.getName());
        Assert.assertEquals(10, info.getEntityType());
        Assert.assertEquals(true, info.getEnabled());
        Assert.assertFalse(info.hasScope());
        Assert.assertEquals(2, info.getSettingsCount());
        Assert.assertThat(info.getSettingsList(), Matchers.containsInAnyOrder(setting1, setting2));

        // Make sure the produced setting policy infos pass validation.
        // Technically this means if there's a bug in the validator this test
        // can also fail, but the benefit is worth the test inter-dependency.
        final IGroupStore groupStore = Mockito.mock(IGroupStore.class);
        SettingSpecStore settingSpecStore = Mockito.mock(SettingSpecStore.class);
        when(settingSpecStore.getSettingSpec(spec1.getName())).thenReturn(Optional.of(spec1));
        when(settingSpecStore.getSettingSpec(spec2.getName())).thenReturn(Optional.of(spec2));
        DefaultSettingPolicyValidator validator =
                new DefaultSettingPolicyValidator(settingSpecStore, groupStore);
        validator.validateSettingPolicy(info, Type.DEFAULT);
    }

    /**
     * Tests when the setting already has associated default setting policy created
     * with the same spec name.
     *
     * @throws Exception if exceptions occurred
     */
    @Test
    public void testDefaultSettingPolicyExist() throws Exception {
        final SettingPolicyInfo info = SettingPolicyInfo.newBuilder()
                .setName("Virtual Machine Defaults")
                .setDisplayName("Virtual Machine Defaults")
                .setEntityType(10)
                .addSettings(Setting.newBuilder()
                        .setSettingSpecName(SPEC_NAME)
                        .setBooleanSettingValue(
                                BooleanSettingValue.newBuilder().setValue(true).build()))
                .setEnabled(true)
                .build();

        final SettingSpec spec = SettingSpec.newBuilder(defaultSetting)
                .setEntitySettingSpec(EntitySettingSpec.newBuilder()
                        .setEntitySettingScope(EntitySettingScope.newBuilder()
                                .setEntityTypeSet(EntityTypeSet.newBuilder().addEntityType(10))))
                .build();
        transactionProvider.getSettingPolicyStore()
                .addSettingPolicies(SettingPolicy.newBuilder()
                        .setInfo(info)
                        .setId(3L)
                        .setSettingPolicyType(Type.DEFAULT)
                        .build());
        getPolicyInfo(0, spec);
    }

    /**
     * Tests when the setting already has associated default setting policy created,
     * but a new policy spec is added.
     *
     * @throws Exception if exceptions occurred
     */
    @Test
    public void testDefaultSettingPolicyExistButNewSpec() throws Exception {
        final SettingPolicyInfo info =
                SettingPolicyInfo.newBuilder().setName("test").setEntityType(10).build();

        final SettingSpec spec = SettingSpec.newBuilder(defaultSetting)
                .setEntitySettingSpec(EntitySettingSpec.newBuilder()
                        .setEntitySettingScope(EntitySettingScope.newBuilder()
                                .setEntityTypeSet(EntityTypeSet.newBuilder().addEntityType(10))))
                .build();
        transactionProvider.getSettingPolicyStore()
                .addSettingPolicies(SettingPolicy.newBuilder()
                        .setInfo(info)
                        .setId(100L)
                        .setSettingPolicyType(Type.DEFAULT)
                        .build());
        getPolicyInfo(1, spec);
    }

    /**
     * Tests default setting creation for setting spec with multiple entity types. Separate setting
     * policy is expected for each entity type.
     *
     * @throws Exception if exceptions occur
     */
    @Test
    public void testDefSettingPoliciesFromSpecWithMultipleEntityTypes() throws Exception {
        final SettingSpec spec1 = SettingSpec.newBuilder()
                .setName("foo")
                .setBooleanSettingValueType(TRUE)
                .setEntitySettingSpec(EntitySettingSpec.newBuilder()
                        .setEntitySettingScope(EntitySettingScope.newBuilder()
                                .setEntityTypeSet(EntityTypeSet.newBuilder()
                                        .addEntityType(10)
                                        .addEntityType(2))))
                .build();
        final Setting setting1 = Setting.newBuilder()
                .setSettingSpecName(spec1.getName())
                .setBooleanSettingValue(BooleanSettingValue.newBuilder().setValue(true))
                .build();

        final Collection<SettingPolicyInfo> defaultPolicyInfos = getPolicyInfo(2, spec1);
        Assert.assertTrue(
                defaultPolicyInfos.stream().anyMatch(policy -> policy.getEntityType() == 10));
        Assert.assertTrue(
                defaultPolicyInfos.stream().anyMatch(policy -> policy.getEntityType() == 2));

        final SettingPolicyInfo info1 = defaultPolicyInfos.stream()
                .filter(policy -> policy.getEntityType() == 10)
                .findAny()
                .get();
        Assert.assertEquals("Virtual Machine Defaults", info1.getName());
        Assert.assertEquals(10, info1.getEntityType());
        Assert.assertEquals(true, info1.getEnabled());
        Assert.assertFalse(info1.hasScope());
        Assert.assertEquals(1, info1.getSettingsCount());
        Assert.assertEquals(setting1, info1.getSettings(0));

        final SettingPolicyInfo info2 = defaultPolicyInfos.stream()
                .filter(policy -> policy.getEntityType() == 2)
                .findAny()
                .get();
        Assert.assertEquals("Storage Defaults", info2.getName());
        Assert.assertEquals(2, info2.getEntityType());
        Assert.assertEquals(true, info2.getEnabled());
        Assert.assertFalse(info2.hasScope());
        Assert.assertEquals(1, info2.getSettingsCount());
        Assert.assertEquals(setting1, info2.getSettings(0));

        // Make sure the produced setting policy infos pass validation.
        // Technically this means if there's a bug in the validator this test
        // can also fail, but the benefit is worth the test inter-dependency.
        final IGroupStore groupStore = Mockito.mock(IGroupStore.class);
        SettingSpecStore settingSpecStore = Mockito.mock(SettingSpecStore.class);
        when(settingSpecStore.getSettingSpec(spec1.getName())).thenReturn(Optional.of(spec1));
        DefaultSettingPolicyValidator validator =
                new DefaultSettingPolicyValidator(settingSpecStore, groupStore);
        validator.validateSettingPolicy(info1, Type.DEFAULT);
        validator.validateSettingPolicy(info2, Type.DEFAULT);
    }

    /**
     * Tests invalid default setting policy.
     *
     * @throws Exception on exceptions faced
     */
    @Test
    public void testDefaultSettingPolicyInvalid() throws Exception {

        final SettingSpec spec = defaultSetting = SettingSpec.newBuilder()
                .setName(SPEC_NAME)
                .setBooleanSettingValueType(TRUE)
                .build();

        getPolicyInfo(0, spec);
    }

    /**
     * Tests default setting policy with duplicated name.
     *
     * @throws Exception on exceptions faced
     */
    @Test
    public void testDefaultSettingPolicyDuplicateName() throws Exception {
        final SettingSpec spec2 =
                SettingSpec.newBuilder().setDisplayName("new display name").build();
        getPolicyInfo(defaultSetting, spec2);
    }

    /**
     * Tests default setting creation for setting, declared not to create a default policy setting
     * for it.
     *
     * @throws Exception if exception occurs
     */
    @Test
    public void testSettingNotCreatingDefaultPolicy() throws Exception {
        final SettingSpec spec = SettingSpec.newBuilder(defaultSetting)
                .setEntitySettingSpec(EntitySettingSpec.newBuilder(entitySettingSpec(2))
                        .setAllowGlobalDefault(false))
                .build();
        getPolicyInfo(0, spec);
    }

    /**
     * Tests that unnecessary policies are not created.
     *
     * @throws Exception on exceptions faced
     */
    @Test
    public void testDoesNotCreateUnnecessaryPolicies() throws Exception {
        final ThinTargetCache thinTargetCache = Mockito.mock(ThinTargetCache.class);
        final Map<Integer, SettingPolicyInfo> defaultsMap =
            DefaultSettingPolicyCreator.defaultSettingPoliciesFromSpecs(
                new EnumBasedSettingSpecStore(false, false, thinTargetCache).getAllSettingSpecs());

        assertThat(defaultsMap.size(), is(lessThan(EntityType.values().length)));
        assertThat(defaultsMap.keySet(), not(contains(EntityType.UNKNOWN.getValue())));
    }

    /**
     * Ensures, that policy is removed from the DB if new policy configuration does not contain the
     * policy any more.
     *
     * @throws Exception on exceptions occurred.
     */
    @Test
    public void testRemoveDefaultPolicyFromDB() throws Exception {
        final SettingPolicyInfo info =
                SettingPolicyInfo.newBuilder().setName("test").setEntityType(10).build();
        final SettingPolicy existingPolicy = SettingPolicy.newBuilder()
                .setInfo(info)
                .setId(3L)
                .setInfo(info)
                .setSettingPolicyType(Type.DEFAULT)
                .build();
        transactionProvider.getSettingPolicyStore().addSettingPolicies(existingPolicy);
        getPolicyInfo(0);
        Mockito.verify(settingStore).deletePolicies(Collections.singleton(3L), Type.DEFAULT);
    }

    /**
     * Tests new setting spec added to a default policy. It is expected that DB representation
     * will be reviewed.
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testNewSettingAddedToPolicy() throws Exception {
        final SettingPolicyInfo info = SettingPolicyInfo.newBuilder()
                .setName("Policy Info")
                .setEntityType(11)
                .addSettings(Setting.newBuilder()
                        .setSettingSpecName(SPEC_NAME)
                        .setBooleanSettingValue(
                                BooleanSettingValue.newBuilder().setValue(false).build())
                        .build())
                .build();
        final SettingPolicy existingPolicy = SettingPolicy.newBuilder()
                .setId(3L)
                .setInfo(info)
                .setSettingPolicyType(Type.DEFAULT)
                .build();
        final SettingSpec spec1 = SettingSpec.newBuilder(defaultSetting)
                .setName("additional spec")
                .setEntitySettingSpec(entitySettingSpec(11))
                .build();
        transactionProvider.getSettingPolicyStore().addSettingPolicies(existingPolicy);
        final SettingPolicyInfo policy = getPolicyInfo(1, defaultSetting, spec1).iterator().next();
        Assert.assertEquals(Sets.newHashSet(SPEC_NAME, "additional spec"), policy.getSettingsList()
                .stream()
                .map(Setting::getSettingSpecName)
                .collect(Collectors.toSet()));
        Mockito.verify(settingStore).deletePolicies(Collections.singleton(3L), Type.DEFAULT);
    }

    /**
     * Tests covers the case, when setting spec is removed from the default settings. We have to
     * remote the setting spec from the DB.
     *
     * @throws Exception on exceptions faced
     */
    @Test
    public void testSettingSpecRemoved() throws Exception {
        final SettingPolicyInfo info = SettingPolicyInfo.newBuilder()
                .setName("Policy Info")
                .setEntityType(11)
                .addSettings(Setting.newBuilder()
                        .setSettingSpecName(SPEC_NAME)
                        .setBooleanSettingValue(
                                BooleanSettingValue.newBuilder().setValue(false).build())
                        .build())
                .addSettings(Setting.newBuilder()
                        .setSettingSpecName("new setting")
                        .setBooleanSettingValue(
                                BooleanSettingValue.newBuilder().setValue(false).build())
                        .build())
                .build();
        final SettingPolicy existingPolicy = SettingPolicy.newBuilder()
                .setId(3L)
                .setInfo(info)
                .setSettingPolicyType(Type.DEFAULT)
                .build();
        transactionProvider.getSettingPolicyStore().addSettingPolicies(existingPolicy);
        Assert.assertEquals(Sets.newHashSet(SPEC_NAME, "new setting"), settingStore.getPolicy(3L)
                .get()
                .getInfo()
                .getSettingsList()
                .stream()
                .map(Setting::getSettingSpecName)
                .collect(Collectors.toSet()));
        final SettingPolicyInfo policy = getPolicyInfo(1, defaultSetting).iterator().next();
        Assert.assertEquals(Collections.singleton(SPEC_NAME), policy.getSettingsList()
                .stream()
                .map(Setting::getSettingSpecName)
                .collect(Collectors.toSet()));
    }

    /**
     * Test that we don't create settings without default values.
     *
     * @throws Exception if something goes wrong
     */
    @Test
    public void testSkipCreatingSettingsWithoutDefaultValue() throws Exception {
        final SettingSpec settingSpec = SettingSpec.newBuilder()
                .setName(SPEC_NAME)
                .setStringSettingValueType(StringSettingValueType.getDefaultInstance())
                .setSortedSetOfOidSettingValueType(
                        SortedSetOfOidSettingValueType.getDefaultInstance())
                .setEnumSettingValueType(EnumSettingValueType.getDefaultInstance())
                .setBooleanSettingValueType(BooleanSettingValueType.getDefaultInstance())
                .setNumericSettingValueType(NumericSettingValueType.getDefaultInstance())
                .setEntitySettingSpec(entitySettingSpec(11))
                .build();
        final SettingPolicyInfo policyInfo1 = getPolicyInfo(settingSpec);
        Assert.assertEquals(0, policyInfo1.getSettingsCount());
    }

    private Collection<SettingPolicyInfo> getPolicyInfo(int expectedCount, SettingSpec... specs)
            throws Exception {
        Mockito.when(settingSpecStore.getAllSettingSpecs()).thenReturn(Arrays.asList(specs));
        for (SettingSpec spec : specs) {
            Mockito.when(settingSpecStore.getSettingSpec(spec.getName()))
                    .thenReturn(Optional.of(spec));
        }
        settingPolicyCreator =
                new DefaultSettingPolicyCreator(settingSpecStore, transactionProvider, 10,
                        new IdentityProvider(0));
        settingPolicyCreator.run();
        @SuppressWarnings("unchecked")
        final ArgumentCaptor<Collection<SettingPolicy>> policyCaptor =
                (ArgumentCaptor)ArgumentCaptor.forClass(Collection.class);
        Mockito.verify(settingStore).createSettingPolicies(policyCaptor.capture());
        Assert.assertEquals(expectedCount, policyCaptor.getValue().size());
        return policyCaptor.getValue()
                .stream()
                .map(SettingPolicy::getInfo)
                .collect(Collectors.toList());
    }

    private SettingPolicyInfo getPolicyInfo(SettingSpec... specs) throws Exception {
        return getPolicyInfo(1, specs).iterator().next();
    }

    private static EntitySettingSpec entitySettingSpec(Integer... entityTypes) {
        final List<Integer> entityTypesList = Arrays.asList(entityTypes);
        return EntitySettingSpec.newBuilder()
                .setEntitySettingScope(EntitySettingScope.newBuilder()
                        .setEntityTypeSet(
                                EntityTypeSet.newBuilder().addAllEntityType(entityTypesList)))
                .build();
    }
}
