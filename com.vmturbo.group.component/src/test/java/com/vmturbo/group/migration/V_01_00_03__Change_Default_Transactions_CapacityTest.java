package com.vmturbo.group.migration;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;

import org.jooq.DSLContext;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import com.vmturbo.common.protobuf.common.Migration.MigrationProgressInfo;
import com.vmturbo.common.protobuf.common.Migration.MigrationStatus;
import com.vmturbo.common.protobuf.setting.SettingProto.NumericSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy.Type;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicyInfo;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.group.db.GroupComponent;
import com.vmturbo.group.db.Tables;
import com.vmturbo.group.db.enums.SettingPolicyPolicyType;
import com.vmturbo.group.db.tables.records.SettingPolicyRecord;
import com.vmturbo.group.identity.IdentityProvider;
import com.vmturbo.group.setting.FileBasedSettingsSpecStore;
import com.vmturbo.group.setting.SettingPolicyFilter;
import com.vmturbo.group.setting.SettingPolicyValidator;
import com.vmturbo.group.setting.SettingSpecStore;
import com.vmturbo.group.setting.SettingStore;
import com.vmturbo.group.setting.SettingsUpdatesSender;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.sql.utils.DbCleanupRule;
import com.vmturbo.sql.utils.DbConfigurationRule;

public class V_01_00_03__Change_Default_Transactions_CapacityTest {
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

    private static final String SETTING_TEST_JSON_SETTING_SPEC_JSON = "setting-test-json/setting-spec.json";

    private DSLContext dslContext = dbConfig.getDslContext();

    private SettingStore settingStore;

    private SettingSpecStore settingSpecStore;

    private SettingPolicyValidator settingPolicyValidator = mock(SettingPolicyValidator.class);

    private IdentityProvider identityProviderSpy = spy(new IdentityProvider(0));

    private V_01_00_03__Change_Default_Transactions_Capacity migration;

    private static final long POLICY_ID_1 = 1L;

    private static final long POLICY_ID_2 = 2L;

    private SettingsUpdatesSender settingsUpdatesSender = mock(SettingsUpdatesSender.class);

    @Before
    public void setup() {
        settingSpecStore = new FileBasedSettingsSpecStore(SETTING_TEST_JSON_SETTING_SPEC_JSON);

        settingStore = new SettingStore(settingSpecStore, dslContext, identityProviderSpy,
            settingPolicyValidator, settingsUpdatesSender);
        migration = new V_01_00_03__Change_Default_Transactions_Capacity(settingStore);
    }

    @Test
    public void testChangeTransactionsCapacityDefaultValue() {
        // insert policy record into db: transaction capacity is still default value
        makeSettingPolicyRecord(POLICY_ID_1, EntityType.DATABASE_SERVER_VALUE, ImmutableMap.of(
            EntitySettingSpecs.TransactionsCapacity.getSettingName(), 10000.0f,
            EntitySettingSpecs.SLACapacity.getSettingName(), 10000.0f)
        ).store();

        // insert policy record into db: user changes transaction capacity to other value
        makeSettingPolicyRecord(POLICY_ID_2, EntityType.APPLICATION_VALUE, ImmutableMap.of(
            EntitySettingSpecs.TransactionsCapacity.getSettingName(), 30.0f)
        ).store();

        // db migration
        final MigrationProgressInfo migrationResult = migration.startMigration();
        assertThat(migrationResult.getStatus(), is(MigrationStatus.SUCCEEDED));
        assertThat(migrationResult.getCompletionPercentage(), is(100.0f));

        // fetch all the default policies from db
        final Map<Long, SettingPolicy> settingPolicyMap = settingStore.getSettingPolicies(
            SettingPolicyFilter.newBuilder().withType(Type.DEFAULT).build())
            .collect(Collectors.toMap(SettingPolicy::getId, Function.identity()));

        final Map<String, NumericSettingValue> settingValueByName1 = settingPolicyMap.get(POLICY_ID_1)
            .getInfo().getSettingsList().stream()
            .collect(Collectors.toMap(Setting::getSettingSpecName, Setting::getNumericSettingValue));
        assertThat(settingValueByName1.size(), is(2));
        // verify that default value of transactions capacity is changed to 20
        assertThat(settingValueByName1.get(EntitySettingSpecs.TransactionsCapacity.getSettingName())
            .getValue(), is(20.0f));
        // verify that default value of sla capacity is not changed
        assertThat(settingValueByName1.get(EntitySettingSpecs.SLACapacity.getSettingName())
            .getValue(), is(10000.0f));

        final Map<String, NumericSettingValue> settingValueByName2 = settingPolicyMap.get(POLICY_ID_2)
            .getInfo().getSettingsList().stream()
            .collect(Collectors.toMap(Setting::getSettingSpecName, Setting::getNumericSettingValue));
        assertThat(settingValueByName2.size(), is(1));
        // verify that default value of transactions capacity is still 30 since user changed it
        assertThat(settingValueByName2.get(EntitySettingSpecs.TransactionsCapacity.getSettingName())
            .getValue(), is(30.0f));
    }

    /**
     * Insert a setting policy record into db based on provided values.
     */
    private SettingPolicyRecord makeSettingPolicyRecord(final long id,
                                                        final int entityType,
                                                        Map<String, Float> settings) {
        final SettingPolicyRecord record = dslContext.newRecord(Tables.SETTING_POLICY);
        record.setId(id);
        record.setName("Default Policy " + id);
        record.setEntityType(entityType);
        record.setPolicyType(SettingPolicyPolicyType.default_);
        record.setSettingPolicyData(SettingPolicyInfo.newBuilder()
            .setEntityType(entityType)
            .addAllSettings(settings.entrySet().stream()
                .map(entry -> Setting.newBuilder()
                    .setSettingSpecName(entry.getKey())
                    .setNumericSettingValue(NumericSettingValue.newBuilder()
                        .setValue(entry.getValue()))
                    .build())
                .collect(Collectors.toList()))
            .build());
        return record;
    }
}
