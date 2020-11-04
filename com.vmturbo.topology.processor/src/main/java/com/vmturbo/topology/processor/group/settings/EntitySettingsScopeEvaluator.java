package com.vmturbo.topology.processor.group.settings;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableMap;

import gnu.trove.iterator.TLongIterator;
import gnu.trove.set.TLongSet;
import gnu.trove.set.hash.TLongHashSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicyInfo;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.components.common.setting.ConfigurableActionSettings;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.processor.group.ResolvedGroup;

/**
 * Evaluate the entities in the scope of a particular setting. Typically a setting applies
 * to the entities belonging to the {@link com.vmturbo.common.protobuf.setting.SettingProto.Scope}
 * on which the setting is attached, but sometimes the setting is attached to a different set
 * of entities (the attachment scope) than it applies to (the resolution scope).
 * <p/>
 * Certain specific settings on certain entity types apply to different entities than those belonging
 * to the explicit attachment scope. These settings have an implicit resolution scope reached by
 * traversal in the topology graph from the entity seeds in the attachment scope. For example,
 * the resize policy placed on CONTAINER_SPEC entity types should actually affect the CONTAINER
 * entities controlled by the CONTAINER_SPEC's.
 */
public class EntitySettingsScopeEvaluator {

    private final TopologyGraph<TopologyEntity> topologyGraph;

    private static final Logger logger = LogManager.getLogger();

    /**
     * Maps an entity seed inside a setting's attachment scope to the entities in the
     * setting's resolution scope.
     */
    @FunctionalInterface
    private interface ImplicitApplicationScope {
        Stream<Long> scopeForSeed(@Nonnull TopologyGraph<TopologyEntity> topologyGraph,
                                  @Nonnull Long scopeSeedOid);
    }

    /**
     * Returns the seed entity and all containers aggregated by that seed entity.
     */
    private static final ImplicitApplicationScope SELF_AND_AGGREGATED_CONTAINERS = (graph, seedOid) ->
        graph.getEntity(seedOid)
            .map(entity -> Stream.concat(entity.getAggregatedEntities().stream()
                .filter(aggregation -> aggregation.getEntityType() == EntityType.CONTAINER_VALUE)
                .map(TopologyEntity::getOid), Stream.of(seedOid)))
        .orElse(Stream.empty());

    /**
     * Implicit resolution scopes for specific settings on specific entity types.
     */
    private static final Map<Integer, Map<String, ImplicitApplicationScope>> IMPLICIT_SCOPES =
        ImmutableMap.of(EntityType.CONTAINER_SPEC_VALUE,
            ImmutableMap.<String, ImplicitApplicationScope>builder()
                .put(ConfigurableActionSettings.Resize.getSettingName(), SELF_AND_AGGREGATED_CONTAINERS)
                .put(EntitySettingSpecs.ContainerSpecVcpuIncrement.getSettingName(), SELF_AND_AGGREGATED_CONTAINERS)
                .put(EntitySettingSpecs.ContainerSpecVmemIncrement.getSettingName(), SELF_AND_AGGREGATED_CONTAINERS)
                .put(EntitySettingSpecs.RateOfResize.getSettingName(), SELF_AND_AGGREGATED_CONTAINERS)
                .put(EntitySettingSpecs.PercentileAggressivenessContainerSpec.getSettingName(), SELF_AND_AGGREGATED_CONTAINERS)
                .put(EntitySettingSpecs.MinObservationPeriodContainerSpec.getSettingName(), SELF_AND_AGGREGATED_CONTAINERS)
                .put(EntitySettingSpecs.MaxObservationPeriodContainerSpec.getSettingName(), SELF_AND_AGGREGATED_CONTAINERS)
                .build()
        );

    /**
     * Create a new {@link EntitySettingsScopeEvaluator} to evaluate the resolution scope for settings.
     *
     * @param topologyGraph The {@link TopologyGraph} to use when resolving scopes.
     */
    public EntitySettingsScopeEvaluator(@Nonnull final TopologyGraph<TopologyEntity> topologyGraph) {
        this.topologyGraph = Objects.requireNonNull(topologyGraph);
    }

    /**
     * Evaluate the scopes for multiple settings in a given {@link SettingPolicy}.
     * The returned {@link ScopedSettings} map one-to-one to the input {@code settingsInPolicy}.
     * When multiple settings share the same scope, we may share some of the sets returned in the
     * {@link ScopedSettings} collection for efficiency.
     *
     * @param settingPolicy The {@link SettingPolicy} containing the settings whose scope we wish to evaluate.
     * @param resolvedGroups A map of pre-resolved groups for this topology by their IDs.
     * @param settingsInPolicy The {@link TopologyProcessorSetting}s for the settings in the policy that we
     *                         wish to resolve.
     * @return the scopes for multiple settings in a given {@link SettingPolicy}.
     */
    public Collection<ScopedSettings> evaluateScopes(@Nonnull final SettingPolicy settingPolicy,
                                                     @Nonnull final Map<Long, ResolvedGroup> resolvedGroups,
                                                     @Nonnull final Collection<TopologyProcessorSetting<?>> settingsInPolicy) {
        Objects.requireNonNull(resolvedGroups);
        final SettingPolicyInfo info = settingPolicy.getInfo();

        if (info.hasEntityType() && IMPLICIT_SCOPES.containsKey(info.getEntityType())) {
            return buildScopes(settingPolicy, resolvedGroups,
                settingsInPolicy, IMPLICIT_SCOPES.get(info.getEntityType()));
        } else {
            return Collections.singleton(new ScopedSettings(
                resolveImplicitScope(settingPolicy, resolvedGroups, null), settingsInPolicy));
        }
    }

    /**
     * Evaluate the scope for a set of entity types for a particular setting
     * for a particular {@link ResolvedGroup}.
     *
     * @param resolvedGroup The group of entities for the setting whose scope should be evaluated.
     * @param settingSpecName The name of the settingSpec whose scope should be evaluated.
     * @param entityTypes The {@link EntityType}s the settingSpec is applied to.
     * @return A set of the entities in the resolution scope for the policy.
     */
    public TLongSet evaluateScopeForGroup(@Nonnull final ResolvedGroup resolvedGroup,
                                          @Nonnull final String settingSpecName,
                                          @Nonnull final Set<EntityType> entityTypes) {
        final TLongSet policyScope = new TLongHashSet();
        entityTypes.stream()
            .map(EntityType::getNumber)
            .forEach(entityType -> {
                final ImplicitApplicationScope implicitScope = IMPLICIT_SCOPES
                    .getOrDefault(entityType, Collections.emptyMap())
                    .get(settingSpecName);

                resolveScopeForGroup(policyScope, resolvedGroup, implicitScope, entityType);
            });


        return policyScope;
    }

    /**
     * Evaluate the scope for a single entity types for a particular setting
     * for a particular {@link ResolvedGroup}.
     *
     * @param resolvedGroup The group of entities for the setting whose scope should be evaluated.
     * @param settingSpecName The name of the settingSpec whose scope should be evaluated.
     * @param entityType The {@link EntityType} the settingSpec is applied to.
     * @return A set of the entities in the resolution scope for the policy.
     */
    public TLongSet evaluateScopeForGroup(@Nonnull final ResolvedGroup resolvedGroup,
                                          @Nonnull final String settingSpecName,
                                          final int entityType) {
        final ImplicitApplicationScope implicitScope = IMPLICIT_SCOPES
            .getOrDefault(entityType, Collections.emptyMap())
            .get(settingSpecName);

        final TLongSet policyScope = new TLongHashSet();
        return resolveScopeForGroup(policyScope, resolvedGroup, implicitScope, entityType);
    }

    /**
     * Build a collection of {@link ScopedSettings} for the settings in a particular {@link SettingPolicy}.
     *
     * @param settingPolicy The {@link SettingPolicy} containing the settings whose scope we wish to evaluate.
     * @param resolvedGroups A map of pre-resolved groups for this topology by their IDs.
     * @param settingsInPolicy The {@link TopologyProcessorSetting}s for the settings in the policy that we
     *                         wish to resolve.
     * @param scopeMapping A map of setting names to implicit scopes. If we wish to evaluate a setting whose name
     *                     is not explicitly in the map, then we return a resolution scope equal to its attachment
     *                     scope.
     * @return the scopes for multiple settings in a given {@link SettingPolicy}.
     */
    private Collection<ScopedSettings> buildScopes(
        @Nonnull final SettingPolicy settingPolicy,
        @Nonnull final Map<Long, ResolvedGroup> resolvedGroups,
        @Nonnull final Collection<TopologyProcessorSetting<?>> settingsInPolicy,
        @Nonnull final Map<String, ImplicitApplicationScope> scopeMapping) {
        // Keep a temporary map of implicit scopes we've already resolved in case the setting policy
        // has multiple setting policies with the same implicit scope for different settings.
        final Map<ImplicitApplicationScope, TLongSet> resolvedScopeCache = new HashMap<>();
        final Map<TLongSet, ScopedSettings> scopes = new IdentityHashMap<>();

        // Resolve the scope for each setting in the policy separately. Note that some of
        // the settings may not have an implicit scope in which case they will apply directly
        // to the explicit scope.
        for (TopologyProcessorSetting<?> setting : settingsInPolicy) {
            final ImplicitApplicationScope scopeLookup = scopeMapping.get(setting.getSettingSpecName());
            final TLongSet oidScope = resolvedScopeCache.computeIfAbsent(scopeLookup,
                implicitScope -> resolveImplicitScope(settingPolicy, resolvedGroups, implicitScope));

            final ScopedSettings scopedSettings = scopes.computeIfAbsent(oidScope,
                oids -> new ScopedSettings(oidScope, new ArrayList<>()));
            scopedSettings.settingsForScope.add(setting);
        }

        return scopes.values();
    }

    /**
     * Create a {@link TLongSet} of the members of the resolved scope for setting's implicit scope.
     * If the implicit scope is null, returns the attachment scope for the entity (this is just
     * the members of a given entity type on the group where the settingPolicy was placed).
     *
     * @param settingPolicy The {@link SettingPolicy} containing the settings whose scope we wish to evaluate.
     * @param resolvedGroups A map of pre-resolved groups for this topology by their IDs.
     * @param implicitScope The implicit scope for a particular setting. If null, returns the attachment scope.
     * @return a {@link TLongSet} of the members of the resolved scope for setting's implicit scope.
     */
    private TLongSet resolveImplicitScope(@Nonnull final SettingPolicy settingPolicy,
                                          @Nonnull final Map<Long, ResolvedGroup> resolvedGroups,
                                          @Nullable final ImplicitApplicationScope implicitScope) {
        final TLongSet policyScope = new TLongHashSet();
        for (Long groupId : settingPolicy.getInfo().getScope().getGroupsList()) {
            final ResolvedGroup resolvedGroup = resolvedGroups.get(groupId);
            if (resolvedGroup == null) {
                logger.error("Group {} referenced by policy {} ({}) is unresolved. Skipping.",
                    groupId, settingPolicy.getId(), settingPolicy.getInfo().getName());
            } else {
                resolveScopeForGroup(policyScope, resolvedGroup, implicitScope,
                    settingPolicy.getInfo().getEntityType());
            }
        }

        return policyScope;
    }

    /**
     * Resolve a setting's implicit scope whose attachment scope is partially
     * given by the {@link ResolvedGroup}.
     *
     * @param settingScope The scope for the setting. This may already contain members when a setting
     *                     is attached to multiple {@link ResolvedGroup}s.
     * @param resolvedGroup The group that the setting was attached to.
     * @param implicitScope The implicit scope for the particular setting and entity type.
     * @param entityType The entity type that the setting applies to.
     * @return a setting's implicit scope whose attachment scope is given by the {@link ResolvedGroup}.
     *         Note that this just returns the input {@link TLongSet} settingScope with additional
     *         members added based on the resolution.
     */
    private TLongSet resolveScopeForGroup(@Nonnull final TLongSet settingScope,
                                          @Nonnull final ResolvedGroup resolvedGroup,
                                          @Nullable final ImplicitApplicationScope implicitScope,
                                          final int entityType) {
        if (implicitScope == null) {
            settingScope.addAll(resolvedGroup.getEntitiesOfType(ApiEntityType.fromType(entityType)));
        } else {
            resolvedGroup.getEntitiesOfType(ApiEntityType.fromType(entityType))
                .forEach(oid -> implicitScope.scopeForSeed(topologyGraph, oid)
                    .forEach(settingScope::add));
        }

        return settingScope;
    }

    /**
     * A simple helper class bundling all the settings that resolve to a particular scope.
     */
    public static class ScopedSettings {
        /**
         * The shared resolution scope for all settings in {@code settingsForScope}.
         */
        public final TLongSet scope;

        /**
         * A collection of settings that all share the resolved {@code scope}.
         */
        public final Collection<TopologyProcessorSetting<?>> settingsForScope;

        /**
         * Create a new {@link ScopedSettings} to bundle a group of settings and a shared resolution
         * scope for those settings.
         *
         * @param scope The resolution scope shared by all the settings.
         * @param settingsForScope The settings that all share the resolution scope.
         */
        public ScopedSettings(@Nonnull final TLongSet scope,
                              @Nonnull final Collection<TopologyProcessorSetting<?>> settingsForScope) {
            this.scope = Objects.requireNonNull(scope);
            this.settingsForScope = Objects.requireNonNull(settingsForScope);
        }

        /**
         * Get an iterator to iterate the shared scope for all the settings.
         *
         * @return an iterator to iterate the shared scope for all the settings.
         */
        public TLongIterator iterator() {
            return scope.iterator();
        }
    }
}
