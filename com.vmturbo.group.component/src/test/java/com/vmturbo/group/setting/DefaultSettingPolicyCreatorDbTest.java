package com.vmturbo.group.setting;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.vmturbo.common.protobuf.schedule.ScheduleProto;
import com.vmturbo.common.protobuf.setting.SettingProto;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingScope;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingScope.EntityTypeSet;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingSpec;
import com.vmturbo.common.protobuf.setting.SettingProto.EnumSettingValueType;
import com.vmturbo.common.protobuf.setting.SettingProto.NumericSettingValueType;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicyInfo;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingSpec;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingTiebreaker;
import com.vmturbo.common.protobuf.setting.SettingProto.SortedSetOfOidSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.SortedSetOfOidSettingValueType;
import com.vmturbo.common.protobuf.setting.SettingProto.StringSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.StringSettingValueType;
import com.vmturbo.group.common.DuplicateNameException;
import com.vmturbo.group.common.InvalidItemException;
import com.vmturbo.group.db.GroupComponent;
import com.vmturbo.group.group.pagination.GroupPaginationParams;
import com.vmturbo.group.identity.IdentityProvider;
import com.vmturbo.group.schedule.ScheduleStore;
import com.vmturbo.group.schedule.ScheduleValidator;
import com.vmturbo.group.service.StoreOperationException;
import com.vmturbo.group.service.TransactionProvider;
import com.vmturbo.group.service.TransactionProviderImpl;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.sql.utils.DbCleanupRule;
import com.vmturbo.sql.utils.DbConfigurationRule;

/**
 * Tests {@link DefaultSettingPolicyCreator} class with actual db interactions.
 */
public class DefaultSettingPolicyCreatorDbTest {

    private static final String WORKFLOW_SETTING = "resizeVmemUpInBetweenThresholdsActionWorkflow";
    private static final String WORKFLOW_SETTING_VALUE = "TestWorkflow";
    private static final String IGNORE_DIRECTORIES = "ignoreDirectories";
    private static final String IGNORE_DIRECTORIES_VALUE = "\\\\.dvsData.*|\\\\.snapshot.*";
    private static final String PERCENTILE_AGGRESSIVENESS = "percentileAggressivenessVirtualMachine";
    private static final float PERCENTILE_AGGRESSIVENESS_VALUE = 95f;
    private static final String VMEM_RESIZE_MODE = "resizeVmemUpInBetweenThresholds";
    private static final String VMEM_RESIZE_MODE_VALUE = "MANUAL";
    private static final String EXECUTION_SCHEDULE = "resizeVmemUpInBetweenThresholdsExecutionSchedule";


    /**
     * Rule to create the DB schema and migrate it.
     */
    @ClassRule
    public static DbConfigurationRule dbConfig = new DbConfigurationRule(GroupComponent.GROUP_COMPONENT);
    /**
     * Rule to automatically cleanup DB data before each test.
     */
    @Rule
    public DbCleanupRule dbCleanup = dbConfig.cleanupRule();
    /**
     * Rule to expect exceptions, if required.
     */
    @Rule
    public ExpectedException expectedException = ExpectedException.none();


    /**
     * Tests a case where defaults settings get created in db for the first time. Then, it
     * gets updated and there is simulation of restart where we ensure the default settings
     * that has been changed persisted in DB.
     *
     * @throws StoreOperationException if something goes wrong.
     * @throws DuplicateNameException if something goes wrong.
     * @throws InvalidItemException if something goes wrong.
     */
    @Test
    public void testDefaultPolicyMigrations() throws StoreOperationException, DuplicateNameException, InvalidItemException {
        // ARRANGE
        List<SettingSpec> specList = createSpecStore();
        SettingSpecStore specStore = mock(SettingSpecStore.class);
        when(specStore.getAllSettingSpecs()).thenReturn(specList);
        IdentityProvider identityProvider = new IdentityProvider(0);

        final SettingStore settingStore = new SettingStore(specStore,
            dbConfig.getDslContext(),
            mock(SettingPolicyValidator.class), mock(SettingsUpdatesSender.class));

        final ScheduleStore scheduleStore = new ScheduleStore(dbConfig.getDslContext(),
            mock(ScheduleValidator.class), identityProvider);

        final TransactionProvider transactionProvider = new TransactionProviderImpl(settingStore,
            dbConfig.getDslContext(), identityProvider, new GroupPaginationParams(100, 500));

        // ACT

        // Create the default policies
        DefaultSettingPolicyCreator creator = new DefaultSettingPolicyCreator(specStore,
            transactionProvider, 0, identityProvider);

        creator.run();

        // ASSERT

        Collection<SettingPolicy> currentSetting = settingStore.getSettingPolicies(SettingPolicyFilter
            .newBuilder()
            .withType(SettingPolicy.Type.DEFAULT)
            .build());

        assertThat(currentSetting.size(), is(1));
        SettingPolicy policy = currentSetting.iterator().next();
        assertThat(policy.getSettingPolicyType(), is(SettingPolicy.Type.DEFAULT));
        SettingPolicyInfo policyInfo = policy.getInfo();
        assertThat(policyInfo.getEntityType(), is(EntityType.VIRTUAL_MACHINE_VALUE));
        assertThat(policyInfo.getSettingsCount(), is(3));
        Setting ignoreDirectorySetting = getSetting(policyInfo.getSettingsList(),
            IGNORE_DIRECTORIES);
        assertThat(ignoreDirectorySetting
            .getStringSettingValue().getValue(), is(IGNORE_DIRECTORIES_VALUE));
        Setting percentileSetting = getSetting(policyInfo.getSettingsList(),
            PERCENTILE_AGGRESSIVENESS);
        assertThat((double)percentileSetting.getNumericSettingValue().getValue(),
            closeTo(PERCENTILE_AGGRESSIVENESS_VALUE, 0.001f));
        Setting modeSetting = getSetting(policyInfo.getSettingsList(), VMEM_RESIZE_MODE);
        assertThat(modeSetting.getEnumSettingValue().getValue(), is(VMEM_RESIZE_MODE_VALUE));

        //ARRANGE

        // Now we give values to to the settings with default values and make sure it will there
        // next time
        ScheduleProto.Schedule schedule = scheduleStore.createSchedule(ScheduleProto.Schedule.newBuilder()
            .setDisplayName("TestSchedule")
            .setStartTime(1920233602000L)
            .setEndTime(1920233604000L)
            .setLastDate(1920234604000L)
            .setTimezoneId("America/Toronto")
            .build());

        SettingPolicyInfo updatedPolicyInfo =
            SettingPolicyInfo.newBuilder(policyInfo)
                .clearSettings()
                .addSettings(ignoreDirectorySetting)
                .addSettings(percentileSetting)
                .addSettings(modeSetting.toBuilder()
                    .setEnumSettingValue(SettingProto.EnumSettingValue.newBuilder()
                        .setValue("RECOMMEND").build()).build())
                .addSettings(Setting.newBuilder()
                    .setSettingSpecName(WORKFLOW_SETTING)
                    .setStringSettingValue(
                        StringSettingValue.newBuilder().setValue(WORKFLOW_SETTING_VALUE)))
                .addSettings(Setting.newBuilder()
                    .setSettingSpecName(EXECUTION_SCHEDULE)
                    .setSortedSetOfOidSettingValue(
                        SortedSetOfOidSettingValue.newBuilder().addOids(schedule.getId())))
                .build();
        settingStore.updateSettingPolicy(policy.getId(), updatedPolicyInfo);

        // ACT

        // Create the default policies
        creator = new DefaultSettingPolicyCreator(specStore,
            transactionProvider, 0, identityProvider);

        creator.run();

        // ASSERT
        currentSetting = settingStore.getSettingPolicies(SettingPolicyFilter
            .newBuilder()
            .withType(SettingPolicy.Type.DEFAULT)
            .build());

        assertThat(currentSetting.size(), is(1));
        policy = currentSetting.iterator().next();
        assertThat(policy.getSettingPolicyType(), is(SettingPolicy.Type.DEFAULT));
        policyInfo = policy.getInfo();
        assertThat(policyInfo.getEntityType(), is(EntityType.VIRTUAL_MACHINE_VALUE));
        assertThat(policyInfo.getSettingsCount(), is(5));
        ignoreDirectorySetting = getSetting(policyInfo.getSettingsList(),
            IGNORE_DIRECTORIES);
        assertThat(ignoreDirectorySetting
            .getStringSettingValue().getValue(), is(IGNORE_DIRECTORIES_VALUE));
        percentileSetting = getSetting(policyInfo.getSettingsList(),
            PERCENTILE_AGGRESSIVENESS);
        assertThat((double)percentileSetting.getNumericSettingValue().getValue(),
            closeTo(PERCENTILE_AGGRESSIVENESS_VALUE, 0.001f));
        modeSetting = getSetting(policyInfo.getSettingsList(), VMEM_RESIZE_MODE);
        assertThat(modeSetting.getEnumSettingValue().getValue(), is("RECOMMEND"));
        Setting workflowSetting = getSetting(policyInfo.getSettingsList(), WORKFLOW_SETTING);
        assertThat(workflowSetting.getStringSettingValue().getValue(), is(WORKFLOW_SETTING_VALUE));
        Setting scheduleSetting = getSetting(policyInfo.getSettingsList(), EXECUTION_SCHEDULE);
        assertThat(scheduleSetting.getSortedSetOfOidSettingValue().getOidsList(),
            is(Collections.singletonList(schedule.getId())));

    }

    private Setting getSetting(List<Setting> settingsList,
                               String id) {
        return settingsList
            .stream()
            .filter(setting -> setting.getSettingSpecName().equals(id))
            .findAny()
            .get();

    }

    private List<SettingSpec> createSpecStore() {
        List<SettingSpec> list = new ArrayList<>();
        // Add a string setting with no default value
        list.add(SettingSpec.newBuilder()
            .setName(WORKFLOW_SETTING)
            .setDisplayName("Workflow to run instead of native execution for a Resize Up VM VMEM "
                + "action")
            .setEntitySettingSpec(EntitySettingSpec.newBuilder()
                .setEntitySettingScope(EntitySettingScope
                    .newBuilder()
                    .setEntityTypeSet(EntityTypeSet
                        .newBuilder()
                        .addEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                        .build())
                    .build())
                .setTiebreaker(SettingTiebreaker.SMALLER)
                .setAllowGlobalDefault(true)
                .build())
            .setStringSettingValueType(StringSettingValueType.newBuilder()
                .setValidationRegex(".*")
                .build())
            .build()
        );

        // Add a string setting with with a default value
        list.add(SettingSpec.newBuilder()
            .setName(IGNORE_DIRECTORIES)
            .setDisplayName("Directories to ignore")
            .setEntitySettingSpec(EntitySettingSpec.newBuilder()
                .setEntitySettingScope(EntitySettingScope
                    .newBuilder()
                    .setEntityTypeSet(EntityTypeSet
                        .newBuilder()
                        .addEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                        .build())
                    .build())
                .setTiebreaker(SettingTiebreaker.SMALLER)
                .setAllowGlobalDefault(true)
                .build())
            .setStringSettingValueType(StringSettingValueType.newBuilder()
                .setValidationRegex(".*")
                .setDefault(IGNORE_DIRECTORIES_VALUE)
                .build())
            .build()
        );

        // Add a numeric setting with default value
        list.add(SettingSpec.newBuilder()
            .setName(PERCENTILE_AGGRESSIVENESS)
            .setDisplayName("Aggressiveness")
            .setEntitySettingSpec(EntitySettingSpec.newBuilder()
                .setEntitySettingScope(EntitySettingScope
                    .newBuilder()
                    .setEntityTypeSet(EntityTypeSet
                        .newBuilder()
                        .addEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                        .build())
                    .build())
                .setTiebreaker(SettingTiebreaker.BIGGER)
                .setAllowGlobalDefault(true)
                .build())
            .setNumericSettingValueType(NumericSettingValueType.newBuilder()
                .setMin(90.0f)
                .setMax(100.f)
                .setDefault(PERCENTILE_AGGRESSIVENESS_VALUE)
                .build())
            .build()
        );

        // Add a enum setting with default value
        list.add(SettingSpec.newBuilder()
            .setName(VMEM_RESIZE_MODE)
            .setDisplayName("VMem Resize Up")
            .setEntitySettingSpec(EntitySettingSpec.newBuilder()
                .setEntitySettingScope(EntitySettingScope
                    .newBuilder()
                    .setEntityTypeSet(EntityTypeSet
                        .newBuilder()
                        .addEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                        .build())
                    .build())
                .setTiebreaker(SettingTiebreaker.SMALLER)
                .setAllowGlobalDefault(true)
                .build())
            .setEnumSettingValueType(EnumSettingValueType.newBuilder()
                .addEnumValues("RECOMMEND")
                .addEnumValues("MANUAL")
                .setDefault(VMEM_RESIZE_MODE_VALUE)
                .build())
            .build()
        );

        // Add a sort oid setting without default value
        list.add(SettingSpec.newBuilder()
            .setName(EXECUTION_SCHEDULE)
            .setDisplayName("Execution window for VMem Resize Up action")
            .setEntitySettingSpec(EntitySettingSpec.newBuilder()
                .setEntitySettingScope(EntitySettingScope
                    .newBuilder()
                    .setEntityTypeSet(EntityTypeSet
                        .newBuilder()
                        .addEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                        .build())
                    .build())
                .setTiebreaker(SettingTiebreaker.UNION)
                .setAllowGlobalDefault(true)
                .build())
            .setSortedSetOfOidSettingValueType(SortedSetOfOidSettingValueType.newBuilder()
                .setType(SortedSetOfOidSettingValueType.Type.ENTITY)
                .build())
            .build()
        );

        return list;
    }
}