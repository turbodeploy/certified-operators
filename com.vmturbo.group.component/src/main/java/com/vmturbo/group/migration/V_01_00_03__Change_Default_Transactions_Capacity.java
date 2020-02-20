package com.vmturbo.group.migration;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.common.Migration.MigrationProgressInfo;
import com.vmturbo.common.protobuf.common.Migration.MigrationStatus;
import com.vmturbo.common.protobuf.setting.SettingProto.NumericSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting.Builder;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy.Type;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicyInfo;
import com.vmturbo.components.common.migration.AbstractMigration;
import com.vmturbo.components.common.migration.Migration;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.group.common.DuplicateNameException;
import com.vmturbo.group.common.InvalidItemException;
import com.vmturbo.group.common.ItemNotFoundException.SettingPolicyNotFoundException;
import com.vmturbo.group.setting.SettingPolicyFilter;
import com.vmturbo.group.setting.SettingStore;

/**
 * This migration script goes through all the default setting policies saved in DB, and find those
 * policies which has TransactionsCapacity setting, and change the default value from 10000.0 to
 * 20.0. If its default is not 10000.0, it means it's already changed to other meaningful value by
 * user, we should keep it and not change it.
 */
public class V_01_00_03__Change_Default_Transactions_Capacity extends AbstractMigration {

    private static final Logger logger = LogManager.getLogger();

    private static final float DEFAULT_VALUE_OLD = 10000.0f;

    private static final float DEFAULT_VALUE_NEW = 20.0f;

    private final SettingStore settingStore;


    public V_01_00_03__Change_Default_Transactions_Capacity(@Nonnull final SettingStore settingStore) {
        this.settingStore = Objects.requireNonNull(settingStore);
    }

    @Override
    public MigrationProgressInfo doStartMigration() {
        // keep track of policies whose transactions capacity is changed
        final List<String> migratedPolicies = new ArrayList<>();

        // fetch all the default policies related to transactions capacity from db
        final SettingPolicyFilter.Builder policyFilter = SettingPolicyFilter.newBuilder()
            .withType(Type.DEFAULT);
        EntitySettingSpecs.TransactionsCapacity.getEntityTypeScope().forEach(entityType ->
            policyFilter.withEntityType(entityType.getNumber()));
        final Stream<SettingPolicy> defaultSettingPolicies = settingStore.getSettingPolicies(
            policyFilter.build());

        // finds the policies which contains TransactionsCapacity setting and change default value
        // to 20 if old default value is 10000
        defaultSettingPolicies.forEach(settingPolicy -> {
            final SettingPolicyInfo.Builder newSettingPolicyInfo = settingPolicy.getInfo().toBuilder();
            Optional<Builder> settingBuilder = newSettingPolicyInfo.getSettingsBuilderList().stream()
                .filter(setting -> EntitySettingSpecs.TransactionsCapacity.getSettingName()
                    .equals(setting.getSettingSpecName()))
                // if the user happens to set the value to 10000 we will still overwrite it,
                // but it's ok because the likelihood of that is almost 0.
                .filter(setting -> setting.getNumericSettingValue().getValue() == DEFAULT_VALUE_OLD)
                .findAny();
            if (settingBuilder.isPresent()) {
                settingBuilder.get().setNumericSettingValue(NumericSettingValue.newBuilder()
                    .setValue(DEFAULT_VALUE_NEW));
                try {
                    logger.info("Changing default value of transactions capacity " +
                            "from {} to {} for setting policy: {}", DEFAULT_VALUE_OLD,
                        DEFAULT_VALUE_NEW, settingPolicy.getInfo().getName());
                    settingStore.updateSettingPolicy(settingPolicy.getId(), newSettingPolicyInfo.build());
                    migratedPolicies.add(settingPolicy.getInfo().getName());
                } catch (SettingPolicyNotFoundException | InvalidItemException | DuplicateNameException e) {
                    logger.error("Failed to change default value of transactions capacity " +
                            "from {} to {} for setting policy: {} due to error: {}",
                        DEFAULT_VALUE_OLD, DEFAULT_VALUE_NEW,
                        settingPolicy.getInfo().getName(), e.getMessage());
                }
            }
        });

        logger.info("Finished migration!");

        return updateMigrationProgress(MigrationStatus.SUCCEEDED, 100,
            "Changed default value of transactions capacity from " +
                    DEFAULT_VALUE_OLD + " to " + DEFAULT_VALUE_NEW + " for setting policies: " +
                    String.join(", ", migratedPolicies));
    }
}
