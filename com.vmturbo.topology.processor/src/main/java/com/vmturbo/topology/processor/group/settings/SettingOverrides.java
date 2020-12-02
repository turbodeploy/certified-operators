package com.vmturbo.topology.processor.group.settings;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;

import gnu.trove.iterator.TLongIterator;
import gnu.trove.set.TLongSet;
import gnu.trove.set.hash.TLongHashSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.PlanChanges.MaxUtilizationLevel;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.SettingOverride;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingScope;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettings.SettingToPolicyId;
import com.vmturbo.common.protobuf.setting.SettingProto.NumericSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingSpec;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTOOrBuilder;
import com.vmturbo.components.common.setting.ActionSettingSpecs;
import com.vmturbo.components.common.setting.ConfigurableActionSettings;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.topology.processor.group.ResolvedGroup;

/**
 * A helper class to capture the {@link SettingOverride}s defined for a scenario and index
 * them in a way that makes application of overrides more straightforward.
 */
public class SettingOverrides {
    private static final Logger logger = LogManager.getLogger();

    // this map is used for creating settings based on max utilization plan configurations. The list
    // of settings chosen is based on the classic implementation for this plan configuration, which
    // is hardcoded to set commodity utilization thresholds for these three commodity types.
    private static final Map<String, SettingSpec> MAX_UTILIZATION_SETTING_SPECS = ImmutableMap.of(
        EntitySettingSpecs.CpuUtilization.getSettingName(), EntitySettingSpecs.CpuUtilization.getSettingSpec(),
        EntitySettingSpecs.MemoryUtilization.getSettingName(), EntitySettingSpecs.MemoryUtilization.getSettingSpec(),
        EntitySettingSpecs.StorageAmountUtilization.getSettingName(), EntitySettingSpecs.StorageAmountUtilization.getSettingSpec()
    );

    /**
     * settingName -> setting
     *
     *<p> These overrides apply to all entities that have a setting matching settingName.
     */
    private Map<String, Setting> globalOverrides = new HashMap<>();

    /**
     * entityType -> settingName -> setting
     *
     *<p> These overrides apply to all entities of a particular type that have a setting matching
     * settingName.
     */
    @VisibleForTesting
    protected Map<Integer, Map<String, Setting>> overridesForEntityType = new HashMap<>();

    /**
     * groupOid -> settingName -> setting
     *
     *<p>These overrides apply to a specific group.
     */
    @VisibleForTesting
    protected final Map<Long, Map<String, Setting>> overridesForGroup = new HashMap<>();

    /**
     * entityOid -> settingName -> setting
     *
     *<p> These overrides apply to a specific entity.
     */
    @VisibleForTesting
    protected Map<Long, Map<String, Setting>> overridesForEntity = new HashMap<>();

    public SettingOverrides(@Nonnull final List<ScenarioChange> changes) {
        // find any global or entity-type based setting overrides
        changes.stream()
            .filter(ScenarioChange::hasSettingOverride)
            .map(ScenarioChange::getSettingOverride)
            .filter(SettingOverride::hasSetting)
            .forEach(settingOverride -> {
                final Map<String, Setting> settingByNameMap;
                if (settingOverride.hasEntityType()) {
                    if (settingOverride.hasGroupOid()) {
                        // Group based override
                        settingByNameMap = overridesForGroup.computeIfAbsent(
                                settingOverride.getGroupOid(), k -> new HashMap<>());
                    } else {
                        // Entity based override
                        settingByNameMap = overridesForEntityType.computeIfAbsent(
                                settingOverride.getEntityType(), k -> new HashMap<>());
                    }
                } else {
                    // Global Override
                    settingByNameMap = globalOverrides;
                }
                final Setting overridenSetting = settingOverride.getSetting();
                settingByNameMap.put(overridenSetting.getSettingSpecName(), overridenSetting);
            });
    }

    /**
     * Get the list of groups involved in settings overrides.
     *
     * @return a list of group id's present in the settings changes.
     */
    public Set<Long> getInvolvedGroups() {
        return overridesForGroup.keySet();
    }

    /**
     * For group-based or plan full scope overrides, we will create setting overrides on a per-entity basis,
     * based on max utilization settings.
     *
     *<p> We are creating them as entity setting overrides rather than SettingPolicy because we want
     * these to override the existing policies, rather than co-exist with them.
     *
     * @param groupsById    A map of group id -> Groups, used for group resolution.
     * @param scopeEvaluator An evaluator for setting scopes
     */
    public void resolveGroupOverrides(@Nonnull final Map<Long, ResolvedGroup> groupsById,
                                      @Nonnull final EntitySettingsScopeEvaluator scopeEvaluator) {
        final Map<Setting, TLongSet> groupOverrideSettings = new HashMap<>();
        final Map<String, SettingSpec> grpOverrideSpecs = new HashMap<>();
        overridesForGroup.forEach((groupOid, settingsMap) -> {
            final ResolvedGroup group = groupsById.get(groupOid);
            if (group != null) {
                settingsMap.forEach((settingName, setting) -> {
                    EntitySettingSpecs.getSettingByName(settingName).ifPresent(entitySpec -> {
                        final TLongSet scope = scopeEvaluator.evaluateScopeForGroup(
                            group, settingName, entitySpec.getEntityTypeScope());

                        grpOverrideSpecs.putIfAbsent(settingName, entitySpec.getSettingSpec());
                        groupOverrideSettings.computeIfAbsent(setting, k -> new TLongHashSet())
                            .addAll(scope);
                    });
                    ConfigurableActionSettings configurableActionSettings = ConfigurableActionSettings.fromSettingName(settingName);
                    if (configurableActionSettings != null) {
                        final TLongSet scope = scopeEvaluator.evaluateScopeForGroup(
                            group, settingName, configurableActionSettings.getEntityTypeScope());
                        grpOverrideSpecs.putIfAbsent(settingName, ActionSettingSpecs.getSettingSpec(settingName));
                        groupOverrideSettings.computeIfAbsent(setting, k -> new TLongHashSet())
                            .addAll(scope);
                    }
                });
            }
        });

        resolveEntitySettings(groupOverrideSettings, grpOverrideSpecs);
    }

    /**
     * Construct a max utilization setting.
     *
     * @param spec the setting spec
     * @param maxUtil the max utilization level
     * @return a max utilization setting
     */
    private @Nonnull Setting createSetting(@Nonnull final SettingSpec spec,
                                           @Nonnull final MaxUtilizationLevel maxUtil) {
        return Setting.newBuilder()
                .setNumericSettingValue(NumericSettingValue
                        .newBuilder()
                        .setValue(maxUtil.getPercentage()))
                .setSettingSpecName(spec.getName())
                .build();
    }

    /**
     * Resolve settings for each entity based on the entitiesToApplySetting map.
     *
     * @param entitiesToApplySetting a map of a setting and the entities to apply that setting
     * @param settingSpecsToConsider mapping of settingName->settingSpec for settings contained in @entitiesToApplySetting
     */
    private void resolveEntitySettings(Map<Setting, TLongSet> entitiesToApplySetting,
            Map<String, SettingSpec> settingSpecsToConsider) {
        for (Map.Entry<Setting, TLongSet> entry : entitiesToApplySetting.entrySet()) {
            Setting setting = entry.getKey();
            for (TLongIterator it = entry.getValue().iterator(); it.hasNext();) {
                final long oid = it.next();

                logger.debug("Creating max utilization settings of {}% for entity {}",
                    setting.getNumericSettingValue().getValue(), oid);
                Map<String, Setting> entitySettingOverrides = overridesForEntity
                    .computeIfAbsent(oid, k -> new HashMap<>());
                // add this setting to the map, using the tiebreaker if there is a conflict
                entitySettingOverrides.merge(setting.getSettingSpecName(), setting, (setting1, setting2) -> {
                    // use the tiebreaker if there is a conflict
                    final TopologyProcessorSetting<?> topologyProcessorSetting1 =
                            TopologyProcessorSettingsConverter.toTopologyProcessorSetting(
                                    Collections.singleton(setting1));
                    final TopologyProcessorSetting<?> topologyProcessorSetting2 =
                            TopologyProcessorSettingsConverter.toTopologyProcessorSetting(
                                    Collections.singleton(setting2));
                    final TopologyProcessorSetting<?> winner =
                            EntitySettingsResolver.SettingResolver.applyTiebreaker(
                            topologyProcessorSetting1, topologyProcessorSetting2,
                            settingSpecsToConsider).getFirst();
                    logger.trace("Plan override of max utilization settings for entity {}"
                            + " selected {}% from ({}%,{}%) for setting {}", oid,
                        winner, setting1, setting2, setting.getSettingSpecName());
                    return TopologyProcessorSettingsConverter.toProtoSettings(winner).iterator().next();
                });
            }
        }
    }

    // does the setting spec apply to the entity type? Yes, if the entity type is in the scope of
    // setting spec.
    private boolean isSettingSpecForEntityType(SettingSpec settingSpec, Collection<ApiEntityType> entityTypes) {
        EntitySettingScope scope = settingSpec.getEntitySettingSpec().getEntitySettingScope();
        // if scope is "all entity type" then we are true
        if (scope.hasAllEntityType()) {
            return true;
        }

        // otherwise scope may be a set of entity types.
        if (scope.hasEntityTypeSet()) {
            // return true if the entity type is in the entity type set.
            return scope.getEntityTypeSet().getEntityTypeList().stream()
                            .map(ApiEntityType::fromType)
                            .anyMatch(entityTypes::contains);
        }
        // default = no
        return false;
    }

    /**
     * Override the settings with the settings that apply to the entity.
     *
     * @param entity The entity that the settings apply to.
     * @param settings Settings spec name to {@link SettingToPolicyId} map
     * @return Collections of overrided {@link SettingToPolicyId}.
     */
    @Nonnull
    public Collection<SettingToPolicyId> overrideSettings(@Nonnull final TopologyEntityDTOOrBuilder entity,
                                 @Nonnull final Map<String, SettingToPolicyId> settings) {
        final Map<String, SettingToPolicyId> result = new HashMap<>(settings);
        // add the overridden settings, in order of priority.
        // Per-entity settings have highest priority, then entity type-based settings, then finally
        // global settings.
        Set<String> settingsAdded = new HashSet<>(); // set of settings added so far
        Stream.of(overridesForEntity.getOrDefault(entity.getOid(), Collections.emptyMap()).values(),
            overridesForEntityType.getOrDefault(entity.getEntityType(), Collections.emptyMap()).values(),
            globalOverrides.values())
            .flatMap(Collection::stream)
            .forEach(setting -> {
                // add setting overrides that haven't been added yet. They should be handled
                // in order of priority.
                final String settingSpecName = setting.getSettingSpecName();
                if (!settingsAdded.contains(settingSpecName)) {
                    result.put(settingSpecName, SettingToPolicyId.newBuilder()
                               // no policy id is associated with global setting or setting overrides.
                               .setSetting(setting)
                               .build());
                    settingsAdded.add(settingSpecName);
                    logger.trace("Setting overrides adding setting {} value {}",
                        settingSpecName, setting);
                }
            });
        return result.values();
    }
}
