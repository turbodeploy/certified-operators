package com.vmturbo.group.setting;

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
import org.jooq.exception.DataAccessException;

import com.vmturbo.common.protobuf.setting.SettingProto;
import com.vmturbo.common.protobuf.setting.SettingProto.BooleanSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.BooleanSettingValueType;
import com.vmturbo.common.protobuf.setting.SettingProto.EnumSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.EnumSettingValueType;
import com.vmturbo.common.protobuf.setting.SettingProto.NumericSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.NumericSettingValueType;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy.Type;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicyInfo;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingSpec;
import com.vmturbo.common.protobuf.setting.SettingProto.SortedSetOfOidSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.SortedSetOfOidSettingValueType;
import com.vmturbo.common.protobuf.setting.SettingProto.StringSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.StringSettingValueType;
import com.vmturbo.components.common.utils.StringConstants;
import com.vmturbo.group.common.DuplicateNameException;
import com.vmturbo.group.common.InvalidItemException;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Responsible for creating the default {@link SettingPolicyInfo} objects for the setting
 * specs loaded by the {@link SettingStore} at startup. It's abstracted into a runnable
 * (instead of a lambda) to encapsulate the retry logic.
 * <p>
 * Also responsible for merging the loaded defaults with any existing defaults in
 * the database.
 */
public class DefaultSettingPolicyCreator implements Runnable {

    private final Logger logger = LogManager.getLogger();
    private final Map<Integer, SettingPolicyInfo> policies;
    private final SettingStore settingStore;
    private final long timeBetweenIterationsMs;

    /**
     * HACK here since we need to show for example "Host Default" as the physical machine policy
     * setting name instead of "Physical Machine Default". However since we parse the
     * {@link EntityType} which is defined in classic as the setting policy name, and the physical
     * machine name is "PHYSICAL_MACHINE" not "HOST", we need to do the convert here to let the
     * API show the setting display name as "Host Default".
     */
    private static final Map<Integer, String> SETTING_NAMES_MAPPER = ImmutableMap.of(
        EntityType.PHYSICAL_MACHINE_VALUE, StringConstants.HOST);

    public DefaultSettingPolicyCreator(@Nonnull final SettingSpecStore specStore,
            @Nonnull final SettingStore settingStore, final long timeBetweenIterationsMs) {
        Objects.requireNonNull(specStore);
        this.policies = defaultSettingPoliciesFromSpecs(specStore.getAllSettingSpecs());
        this.settingStore = Objects.requireNonNull(settingStore);
        this.timeBetweenIterationsMs = timeBetweenIterationsMs;
    }

    /**
     * Attempt to save the default policies into the database. Only save policies that
     * are different from those already in the DB (to handle upgrades). Don't remove
     * existing policies or policy specs (even if they were removed from EntitySettingSpec,
     * which is not supposed to happen) - only add new ones.
     *
     * @return True if another iteration is required. False otherwise (i.e. when all policies
     * have either been written, or failed with unrecoverable errors).
     */
    private boolean runIteration() {
        settingStore.getSettingPolicies(
                SettingPolicyFilter.newBuilder().withType(Type.DEFAULT).build())
                    .map(SettingProto.SettingPolicy::getInfo)
                    .forEach(this::mergePolicies);
        logger.debug("Creating default setting policies: {}", () -> policies.values()
                .stream()
                .map(SettingPolicyInfo::getName)
                .collect(Collectors.toList()));
        final Set<Integer> retrySet = new HashSet<>();
        policies.forEach((entityType, policyInfo) -> {
            try {
                settingStore.createDefaultSettingPolicy(policyInfo);
            } catch (InvalidItemException e) {
                // This indicates a problem with the code!
                // No point trying to create it again.
                logger.error("Failed to create policy " + policyInfo.getName() +
                        " because the default policy was invalid!", e);
            } catch (DuplicateNameException e) {
                // This indicates that the setting policy already exists.
                // This should never happen, because we should have filtered out the
                // policy earlier in the iteration. However, it's not fatal, so no need
                // to throw an IllegalStateException.
                logger.error("The policy: {} already exists! This should never happen!",
                        policyInfo.getName(), e);
            } catch (DataAccessException e) {
                // Some other error connecting to the database - worth trying again!
                retrySet.add(entityType);
                // Stack trace for DataAccessException is useless, just print the error.
                logger.error("Failed to create policy " + policyInfo.getName() + " due to DB error",
                        e);
            }
        });
        // Retain the policies we want to retry.
        policies.keySet().retainAll(retrySet);
        return !policies.isEmpty();
    }

    /**
     * Given a policy from the DB, look for the corresponding entry in policies (generated from
     * EntitySettingSpec for the same entity type - it is assumed that one exists). If it has the
     * same specs as the one from the DB then don't do anything. If it has extra specs then add
     * the extra specs to the db policy.
     * The cases where there is a policy for an entity type in the db, but no corresponding
     * policy in EntitySettingSpec, or where the one in the db has more specs then the one in
     * EntitySettingSpec (i.e. policy specs were removed) are not handled.
     *
     * @param dbPolicyInfo a policy info from the database
     */
    private void mergePolicies(@Nonnull SettingProto.SettingPolicyInfo dbPolicyInfo) {
        int entityType = dbPolicyInfo.getEntityType();
        SettingPolicyInfo defaultPolicy = policies.get(entityType);
        List<String> dbSpecsNames = specNames(dbPolicyInfo);
        List<String> defaultSpecsNames = specNames(defaultPolicy);
        if (dbSpecsNames.equals(defaultSpecsNames)) {
            // Default policies in EntitySettingSpec has the same names as in the DB
            policies.remove(dbPolicyInfo.getEntityType());
        } else {
            // Create a new policy with all specs from the DB and new specs from EntitySettingSpec
            defaultSpecsNames.removeAll(dbSpecsNames);
            SettingPolicyInfo merged = SettingPolicyInfo.newBuilder(dbPolicyInfo)
                    .addAllSettings(defaultPolicy.getSettingsList().stream()
                        .filter(setting -> defaultSpecsNames.contains(setting.getSettingSpecName()) )
                        .collect(Collectors.toList()))
                    .build();
            policies.put(entityType, merged);
        }
    }

    @Nonnull
    private static List<String> specNames(@Nullable SettingPolicyInfo policy) {
        return policy == null
            ? Collections.emptyList()
            : policy.getSettingsList().stream()
                .map(Setting::getSettingSpecName)
                .sorted()
                .collect(Collectors.toList());
    }

    /**
     * Creates the default setting policies in the {@link SettingStore}.
     * Exits after all default setting policies are created, or fail to be created
     * with unrecoverable errors.
     */
    @Override
    public void run() {
        logger.info("Creating default setting policies...");
        while (runIteration()) {
            try {
                Thread.sleep(timeBetweenIterationsMs);
            } catch (InterruptedException e) {
                final String policiesList = policies.values()
                        .stream()
                        .map(SettingPolicyInfo::getName)
                        .collect(Collectors.joining(", "));
                logger.error("Interrupted creation of policies! The following default" +
                        "policies have not been created: " + policiesList);
                Thread.currentThread().interrupt();
                break;
            }
        }
        logger.info("Done creating default setting policies!");
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
                /**
                 * It is a error, if we have pre-defined settings wrongly configured.
                 */
                throw new RuntimeException("Setting spec " + spec.getName() +
                        " is not properly formatted - no value type set!");
            }
        }
        return retBuilder.build();
    }

}
