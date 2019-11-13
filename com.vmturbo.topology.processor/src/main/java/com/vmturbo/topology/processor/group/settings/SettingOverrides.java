package com.vmturbo.topology.processor.group.settings;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.validation.constraints.Max;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.GroupProtoUtil;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.PlanChanges;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.PlanChanges.MaxUtilizationLevel;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.SettingOverride;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingScope;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettings;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettings.SettingToPolicyId;
import com.vmturbo.common.protobuf.setting.SettingProto.NumericSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingSpec;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTOOrBuilder;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.graph.TopologyGraphEntity;
import com.vmturbo.topology.processor.group.GroupResolver;

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
    private Map<Integer, Map<String, Setting>> overridesForEntityType = new HashMap<>();

    /**
     * Keep a list of max utilization levels found in the plan scenario. We will translate these to
     * entity-specific setting overrides during the settings resolution stage of the topology pipeline.
     * TODO: Refactor these to use a generic SettingOverride
     */
    private List<MaxUtilizationLevel> maxUtilizationLevels;

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
                    settingByNameMap = overridesForEntityType.computeIfAbsent(
                        settingOverride.getEntityType(), k -> new HashMap<>());
                } else {
                    settingByNameMap = globalOverrides;
                }
                final Setting overridenSetting = settingOverride.getSetting();
                settingByNameMap.put(overridenSetting.getSettingSpecName(), overridenSetting);
            });

        // find any max utilization overrides in the set of changes -- we will resolve these later,
        // when we have a topology graph available.
        maxUtilizationLevels = changes.stream()
            .filter(ScenarioChange::hasPlanChanges)
            .map(ScenarioChange::getPlanChanges)
            .filter(PlanChanges::hasMaxUtilizationLevel)
            .map(PlanChanges::getMaxUtilizationLevel)
            .collect(Collectors.toList());
    }

    /**
     * Get the list of groups involved in settings overrides.
     *
     * @return a list of group id's present in the settings changes.
     */
    public Set<Long> getInvolvedGroups() {
        if (maxUtilizationLevels.stream().anyMatch(MaxUtilizationLevel::hasGroupOid)) {
            return maxUtilizationLevels.stream()
                .map(MaxUtilizationLevel::getGroupOid)
                .collect(Collectors.toSet());
        } else {
            return Collections.emptySet();
        }

    }

    /**
     * For group-based or plan full scope overrides, we will create setting overrides on a per-entity basis,
     * based on max utilization settings.
     *
     *<p> We are creating them as entity setting overrides rather than SettingPolicy because we want
     * these to override the existing policies, rather than co-exist with them.
     *
     *<p> TODO: Instead of specifically handling MaxUtilization settings, we should treat these more
     * generically, by enhancing the existing SettingOverride object to allow support for group-specific
     * or even entity-specific targeting. Then, instead of MaxUtilizationLevel objects we can work
     * with the more widely usable SettingOverride objects.
     *
     * @param groupsById    A map of group id -> Groups, used for group resolution.
     * @param groupResolver the group resolver to use
     * @param topologyGraph the topology graph used for finding group members
     */
    public void resolveGroupOverrides(@Nonnull Map<Long, Grouping> groupsById,
                                      @Nonnull GroupResolver groupResolver, TopologyGraph<TopologyEntity> topologyGraph) {
        Map<Setting, Set<Long>> entitiesToApplySetting = new HashMap<>();
        for (MaxUtilizationLevel u : maxUtilizationLevels) {
            if (maxUtilizationLevels.stream().noneMatch(MaxUtilizationLevel::hasGroupOid)) {
                // this is a full scope utilization setting
                final Set<Long> entitiesOid = topologyGraph.entitiesOfType(u.getSelectedEntityType())
                        .map(TopologyGraphEntity::getOid)
                        .collect(Collectors.toSet());
                MAX_UTILIZATION_SETTING_SPECS.values().stream()
                    // use selected entity type of full scope to filter setting spec
                    .filter(spec -> isSettingSpecForEntityType(spec,
                            Collections.singletonList(UIEntityType.fromType(u.getSelectedEntityType()))))
                    .forEach(spec -> entitiesToApplySetting.put(createSetting(spec, u), entitiesOid));
            } else {
                // get the group members to apply this setting to
                Grouping group = groupsById.get(u.getGroupOid());
                Set<Long> groupMemberOids = groupResolver.resolve(group, topologyGraph);
                MAX_UTILIZATION_SETTING_SPECS.values().stream()
                        .filter(spec -> isSettingSpecForEntityType(spec,
                            Collections.singletonList(UIEntityType.fromType(u.getSelectedEntityType()))))
                        .forEach(spec -> entitiesToApplySetting.put(createSetting(spec, u), groupMemberOids));
            }
        }
        resolveEntitySettings(entitiesToApplySetting);
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
     */
    private void resolveEntitySettings(Map<Setting, Set<Long>> entitiesToApplySetting) {
        for (Map.Entry<Setting, Set<Long>> entry : entitiesToApplySetting.entrySet()) {
            Setting setting = entry.getKey();
            for (long oid : entry.getValue()) {
                logger.debug("Creating max utilization settings of {}% for entity {}",
                    setting.getNumericSettingValue().getValue(), oid);
                Map<String, Setting> entitySettingOverrides = overridesForEntity
                    .computeIfAbsent(oid, k -> new HashMap<>());
                // add this setting to the map, using the tiebreaker if there is a conflict
                entitySettingOverrides.merge(setting.getSettingSpecName(), setting, (setting1, setting2) -> {
                    // use the tiebreaker if there is a conflict
                    Setting winner = EntitySettingsResolver.SettingResolver.applyTiebreaker(setting1, setting2,
                            MAX_UTILIZATION_SETTING_SPECS);
                    logger.trace("Plan override of max utilization settings for entity {}"
                            + " selected {}% from ({}%,{}%) for setting {}", oid,
                        winner, setting1, setting2, setting.getSettingSpecName());
                    return winner;
                });
            }
        }
    }

    // does the setting spec apply to the entity type? Yes, if the entity type is in the scope of
    // setting spec.
    private boolean isSettingSpecForEntityType(SettingSpec settingSpec, Collection<UIEntityType> entityTypes) {
        EntitySettingScope scope = settingSpec.getEntitySettingSpec().getEntitySettingScope();
        // if scope is "all entity type" then we are true
        if (scope.hasAllEntityType()) return true;

        // otherwise scope may be a set of entity types.
        if (scope.hasEntityTypeSet()) {
            // return true if the entity type is in the entity type set.
            return scope.getEntityTypeSet().getEntityTypeList().stream()
                            .map(UIEntityType::fromType)
                            .anyMatch(entityTypes::contains);
        }
        // default = no
        return false;
    }

    /**
     * Override the settings in a {@link EntitySettings.Builder} with the settings
     * that apply to the entity.
     *
     * @param entity          The entity that the settings apply to.
     * @param settingsBuilder The {@link EntitySettings.Builder}. This builder can be modified
     *                        inside the function. We accept the builder as an
     *                        argument instead of returning a map so that we're not constructing
     *                        a lot of unnecessary map objects just to insert them into the DTO.
     */
    public void overrideSettings(final TopologyEntityDTOOrBuilder entity,
                                 @Nonnull final EntitySettings.Builder settingsBuilder) {
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
                if (!settingsAdded.contains(setting.getSettingSpecName())) {
                    settingsBuilder.addUserSettings(SettingToPolicyId.newBuilder()
                        // no policy id is associated with global setting or setting overrides.
                        .setSetting(setting)
                        .build());
                    settingsAdded.add(setting.getSettingSpecName());
                    logger.trace("Setting overrides adding setting {} value {}",
                        setting.getSettingSpecName(), setting);
                }
            });
    }
}
