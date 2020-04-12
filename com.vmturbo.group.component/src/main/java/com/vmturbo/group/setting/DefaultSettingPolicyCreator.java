package com.vmturbo.group.setting;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.setting.SettingProto.BooleanSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.BooleanSettingValueType;
import com.vmturbo.common.protobuf.setting.SettingProto.EnumSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.EnumSettingValueType;
import com.vmturbo.common.protobuf.setting.SettingProto.NumericSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.NumericSettingValueType;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy.Type;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicyInfo;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingSpec;
import com.vmturbo.common.protobuf.setting.SettingProto.SortedSetOfOidSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.SortedSetOfOidSettingValueType;
import com.vmturbo.common.protobuf.setting.SettingProto.StringSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.StringSettingValueType;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.group.identity.IdentityProvider;
import com.vmturbo.group.service.StoreOperationException;
import com.vmturbo.group.service.TransactionProvider;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Responsible for creating the default {@link SettingPolicyInfo} objects for the setting
 * specs loaded by the {@link SettingStore} at startup. It's abstracted into a runnable
 * (instead of a lambda) to encapsulate the retry logic.
 *
 * <p>Also responsible for merging the loaded defaults with any existing defaults in
 * the database.
 */
public class DefaultSettingPolicyCreator implements Runnable {

    private final Logger logger = LogManager.getLogger();
    private final Map<Integer, SettingPolicyInfo> policies;
    private final TransactionProvider transactionProvider;
    private final long timeBetweenIterationsMs;
    private final IdentityProvider identityProvider;

    /**
     * HACK here since we need to show for example "Host Default" as the physical machine policy
     * setting name instead of "Physical Machine Default". However since we parse the
     * {@link EntityType} which is defined in classic as the setting policy name, and the physical
     * machine name is "PHYSICAL_MACHINE" not "HOST", we need to do the convert here to let the
     * API show the setting display name as "Host Default".
     */
    private static final Map<Integer, String> SETTING_NAMES_MAPPER = ImmutableMap.of(
        EntityType.PHYSICAL_MACHINE_VALUE, StringConstants.HOST);

    /**
     * Constructs default setting policies creator.
     *
     * @param specStore setting specs store
     * @param transactionProvider transaction provider to operate with storage backend
     * @param timeBetweenIterationsMs iterations time
     * @param identityProvider identity provider to assign OIDs to new created default settings
     */
    public DefaultSettingPolicyCreator(@Nonnull final SettingSpecStore specStore,
            @Nonnull final TransactionProvider transactionProvider,
            final long timeBetweenIterationsMs, @Nonnull IdentityProvider identityProvider) {
        Objects.requireNonNull(specStore);
        this.policies = defaultSettingPoliciesFromSpecs(specStore.getAllSettingSpecs());
        this.transactionProvider = Objects.requireNonNull(transactionProvider);
        this.timeBetweenIterationsMs = timeBetweenIterationsMs;
        this.identityProvider = Objects.requireNonNull(identityProvider);
    }

    private boolean runIteration() throws InterruptedException {
        if (Thread.interrupted()) {
            throw new InterruptedException();
        }
        try {
            return transactionProvider.transaction(
                    stores -> runIteration(stores.getSettingPolicyStore()));
        } catch (StoreOperationException e) {
            logger.error("Failed to create default policies", e);
        }
        return true;
    }

    /**
     * Attempt to save the default policies into the database. Only save policies that
     * are different from those already in the DB (to handle upgrades). New set of
     * default settings is expected to be exactly the one that is in {@code policies}.
     *
     * @param settingStore setting policy store to operate with
     * @return True if another iteration is required. False otherwise (i.e. when all policies
     * have either been written, or failed with unrecoverable errors).
     * @throws StoreOperationException if failed operating with setting store
     */
    private boolean runIteration(@Nonnull ISettingPolicyStore settingStore)
            throws StoreOperationException {
        final Collection<SettingPolicy> existingPolicies = settingStore.getPolicies(
                SettingPolicyFilter.newBuilder().withType(Type.DEFAULT).build());
        final Collection<SettingPolicy> policiesToInsert = new ArrayList<>();
        final Set<Long> policiesToDelete = new HashSet<>(
                existingPolicies.stream().map(SettingPolicy::getId).collect(Collectors.toSet()));
        final Set<Integer> missedEntityTypes = new HashSet<>(policies.keySet());
        for (SettingPolicy existingPolicy: existingPolicies) {
            final int entityType = existingPolicy.getInfo().getEntityType();
            missedEntityTypes.remove(entityType);
            final SettingPolicyInfo info = policies.get(entityType);
            if (info == null) {
                // Policy no longer exist. Removing it.
                policiesToDelete.add(existingPolicy.getId());
            } else {
                if (defaultPolicyInfoEquals(info, existingPolicy.getInfo())) {
                    policiesToDelete.remove(existingPolicy.getId());
                } else {
                    policiesToInsert.add(SettingPolicy.newBuilder()
                            .setId(existingPolicy.getId())
                            .setInfo(mergePolicies(existingPolicy.getInfo(), info))
                            .setSettingPolicyType(Type.DEFAULT)
                            .build());
                }
            }
        }
        for (Integer entityType : missedEntityTypes) {
            final SettingPolicyInfo info = policies.get(entityType);
            policiesToInsert.add(SettingPolicy.newBuilder()
                    .setId(identityProvider.next())
                    .setInfo(info)
                    .setSettingPolicyType(Type.DEFAULT)
                    .build());
        }
        logger.debug("Removing obsolete/updated setting policies: {}", policiesToDelete);
        settingStore.deletePolicies(policiesToDelete, Type.DEFAULT);
        logger.debug("Creating/updating default setting policies: {}", () -> policies.values()
                .stream()
                .map(SettingPolicyInfo::getName)
                .collect(Collectors.toList()));
        settingStore.createSettingPolicies(policiesToInsert);
        return false;
    }

    private static boolean defaultPolicyInfoEquals(@Nonnull SettingPolicyInfo left, @Nonnull SettingPolicyInfo right) {
        if (!(left.getDisplayName().equals(right.getDisplayName()))) {
            return false;
        }
        if (!(left.getName().equals(right.getName()))) {
            return false;
        }
        return new HashSet<>(left.getSettingsList()).equals(new HashSet<>(right.getSettingsList()));
    }

    /**
     * Given a policy from the DB, and policy generated from
     * EntitySettingSpec for the same entity type. If it has the
     * same specs as the one from the DB then don't do anything. If it has extra specs then add
     * the extra specs to the db policy.
     * The cases where there is a policy for an entity type in the db, but no corresponding
     * policy in EntitySettingSpec, or where the one in the db has more specs then the one in
     * EntitySettingSpec (i.e. policy specs were removed) are not handled.
     *
     * @param dbPolicyInfo a policy info from the database
     * @param policyFromSpec a policy from setting spec store
     * @return a resulting setting policy
     */
    @Nonnull
    private static SettingPolicyInfo mergePolicies(@Nonnull SettingPolicyInfo dbPolicyInfo,
            @Nonnull SettingPolicyInfo policyFromSpec) {
        final Set<String> dbSpecsNames = specNames(dbPolicyInfo);
        final Set<String> defaultSpecsNames = specNames(policyFromSpec);
        final Set<String> settingsToAdd = new HashSet<>(defaultSpecsNames);
        // Create a new policy with all specs from the DB and new specs from EntitySettingSpec
        settingsToAdd.removeAll(dbSpecsNames);
        final Set<String> settingsToRemove = new HashSet<>(dbSpecsNames);
        settingsToRemove.removeAll(defaultSpecsNames);
        final SettingPolicyInfo merged;
        merged = SettingPolicyInfo.newBuilder(dbPolicyInfo)
                .setDisplayName(policyFromSpec.getDisplayName())
                .setName(policyFromSpec.getName())
                .clearSettings()
                .addAllSettings(dbPolicyInfo.getSettingsList()
                        .stream()
                        .filter(setting -> !settingsToRemove.contains(setting.getSettingSpecName()))
                        .collect(Collectors.toList()))
                .addAllSettings(policyFromSpec.getSettingsList()
                        .stream()
                        .filter(setting -> settingsToAdd.contains(setting.getSettingSpecName()))
                        .collect(Collectors.toList()))
                .build();
        return merged;
    }

    @Nonnull
    private static Set<String> specNames(@Nullable SettingPolicyInfo policy) {
        return policy == null
            ? Collections.emptySet()
            : policy.getSettingsList().stream()
                .map(Setting::getSettingSpecName)
                .collect(Collectors.toSet());
    }

    /**
     * Creates the default setting policies in the {@link SettingStore}.
     * Exits after all default setting policies are created, or fail to be created
     * with unrecoverable errors.
     */
    @Override
    public void run() {
        logger.info("Creating default setting policies...");
        try {
            while (runIteration()) {
                Thread.sleep(timeBetweenIterationsMs);
            }
            logger.info("Done creating default setting policies!");
        } catch (InterruptedException e) {
            final String policiesList = policies.values()
                    .stream()
                    .map(SettingPolicyInfo::getName)
                    .collect(Collectors.joining(", "));
            logger.error("Interrupted creation of policies! The following default" +
                    "policies have not been created: " + policiesList);
        }
    }

    /**
     * Convert a collection of {@link SettingSpec}s into the default {@link SettingPolicyInfo}s
     * that represent the specs. There will be a single {@link SettingPolicyInfo} per entity
     * type - so a single {@link SettingSpec} that applies to multiple entity types will be
     * referenced in more than one {@link SettingPolicyInfo}.
     *
     * @param specs The {@link SettingSpec}s to extract defaults from.
     * @return The {@link SettingPolicyInfo}s representing the defaults for the specs, arranged by
     * entity type.
     */
    @Nonnull
    public static Map<Integer, SettingPolicyInfo> defaultSettingPoliciesFromSpecs(
            @Nonnull final Collection<SettingSpec> specs) {
        // Arrange the setting specs by entity type,
        // removing irrelevant ones.
        final Map<Integer, List<SettingSpec>> specsByEntityType = new HashMap<>();
        specs.stream()
                .filter(SettingSpec::hasEntitySettingSpec)
                // For now we will ignore settings with "AllEntityType", because it's not clear if we
                // will have those settings in the MVP, and if we do have them we will need to come up with
                // a list of possible entity types - we almost certainly can't use ALL EntityType values!
                .filter(spec ->
                        spec.getEntitySettingSpec().getEntitySettingScope().hasEntityTypeSet() &&
                                spec.getEntitySettingSpec().getAllowGlobalDefault())
                .forEach(spec -> spec.getEntitySettingSpec()
                        .getEntitySettingScope()
                        .getEntityTypeSet()
                        .getEntityTypeList()
                        .forEach(type -> {
                            final List<SettingSpec> curTypeList =
                                    specsByEntityType.computeIfAbsent(type,
                                            k -> new LinkedList<>());
                            curTypeList.add(spec);
                        }));

        // Convert the list of setting specs for each entity type
        // to a setting policy info.
        return specsByEntityType.entrySet()
                .stream()
                .collect(Collectors.toMap(Entry::getKey, entry -> {
                    final String settingName = SETTING_NAMES_MAPPER.containsKey(entry.getKey())
                        ? SETTING_NAMES_MAPPER.get(entry.getKey())
                        : EntityType.forNumber(entry.getKey()).name();
                    final String displayName = Stream.of(settingName.split("_"))
                            .map(String::toLowerCase)
                            .map(StringUtils::capitalize)
                            .collect(Collectors.joining(" ", "", " Defaults"));

                    final SettingPolicyInfo.Builder policyBuilder = SettingPolicyInfo.newBuilder()
                            .setName(displayName)
                            .setDisplayName(displayName)
                            .setEntityType(entry.getKey())
                            .setEnabled(true);
                    final List<SettingSpec> specsForType = entry.getValue();
                    specsForType.stream()
                            .map(spec -> defaultSettingFromSpec(spec, entry.getKey()))
                            .forEach(policyBuilder::addSettings);
                    return policyBuilder.build();
                }));
    }

    /**
     * Create a {@link Setting} representing the default value in a {@link SettingSpec}.
     *
     * @param spec The {@link SettingSpec}.
     * @param entityType entity type to create a setting for
     * @return The {@link Setting} representing the spec's default value, or an empty
     * optional if the {@link SettingSpec} is malformed.
     */
    @Nonnull
    private static Setting defaultSettingFromSpec(@Nonnull final SettingSpec spec,
            int entityType) {
        final Setting.Builder retBuilder = Setting.newBuilder().setSettingSpecName(spec.getName());
        switch (spec.getSettingValueTypeCase()) {
            case BOOLEAN_SETTING_VALUE_TYPE: {
                final BooleanSettingValueType valueType = spec.getBooleanSettingValueType();
                final Optional<Boolean> specificTypeDefault =
                        Optional.ofNullable(valueType.getEntityDefaultsMap().get(entityType));
                retBuilder.setBooleanSettingValue(BooleanSettingValue.newBuilder()
                        .setValue(specificTypeDefault.orElse(valueType.getDefault())));
                break;
            }
            case NUMERIC_SETTING_VALUE_TYPE: {
                final NumericSettingValueType valueType = spec.getNumericSettingValueType();
                final Optional<Float> specificTypeDefault =
                        Optional.ofNullable(valueType.getEntityDefaultsMap().get(entityType));
                retBuilder.setNumericSettingValue(NumericSettingValue.newBuilder()
                        .setValue(specificTypeDefault.orElse(valueType.getDefault())));
                break;
            }
            case STRING_SETTING_VALUE_TYPE: {
                final StringSettingValueType valueType = spec.getStringSettingValueType();
                final Optional<String> specificTypeDefault =
                        Optional.ofNullable(valueType.getEntityDefaultsMap().get(entityType));
                retBuilder.setStringSettingValue(StringSettingValue.newBuilder()
                        .setValue(specificTypeDefault.orElse(valueType.getDefault())));
                break;
            }
            case ENUM_SETTING_VALUE_TYPE: {
                final EnumSettingValueType valueType = spec.getEnumSettingValueType();
                final Optional<String> specificTypeDefault =
                        Optional.ofNullable(valueType.getEntityDefaultsMap().get(entityType));
                retBuilder.setEnumSettingValue(EnumSettingValue.newBuilder()
                        .setValue(specificTypeDefault.orElse(valueType.getDefault())));
                break;
            }
            case SORTED_SET_OF_OID_SETTING_VALUE_TYPE:
                final SortedSetOfOidSettingValueType valueType = spec.getSortedSetOfOidSettingValueType();
                retBuilder.setSortedSetOfOidSettingValue(SortedSetOfOidSettingValue.newBuilder()
                    .addAllOids(valueType.getDefaultList()));
                break;
            default: {
                /*
                 * It is a error, if we have pre-defined settings wrongly configured.
                 */
                throw new RuntimeException("Setting spec " + spec.getName() +
                        " is not properly formatted - no value type set!");
            }
        }
        return retBuilder.build();
    }

}
