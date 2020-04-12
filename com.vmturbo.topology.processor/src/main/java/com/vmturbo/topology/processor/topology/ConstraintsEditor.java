package com.vmturbo.topology.processor.topology;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.enums.ConstraintType;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupFilter;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.PlanChanges.ConstraintGroup;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.PlanChanges.GlobalIgnoreEntityType;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.PlanChanges.IgnoreConstraint;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTOREST.CommodityDTO.CommodityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.processor.group.GroupResolutionException;
import com.vmturbo.topology.processor.group.GroupResolver;

/**
 * Edits constraints in Ignore Constraint stage
 */
public class ConstraintsEditor {

    private final Logger logger = LogManager.getLogger();

    private final GroupResolver groupResolver;

    private final GroupServiceBlockingStub groupService;

    private static final Map<String, Integer> COMMODITY_NAME_TO_COMMODITY_TYPE = ImmutableMap.of(
            "ClusterCommodity", CommodityType.CLUSTER.getValue(),
            "DataCenterCommodity", CommodityType.DATACENTER.getValue(),
            "StorageClusterCommodity", CommodityType.STORAGE_CLUSTER.getValue(),
            "NetworkCommodity", CommodityType.NETWORK.getValue()
    );

    private static final Set<Integer> IGNORED_COMMODITIES_FOR_PRESSURE_PLAN = Sets.newHashSet(
            CommodityType.NETWORK.getValue(), CommodityType.STORAGE_CLUSTER.getValue(),
            CommodityType.DATACENTER.getValue());

    private static final String ALL_COMMODITIES = "ALL_COMMODITIES";

    public ConstraintsEditor(@Nonnull GroupResolver groupResolver,
            @Nonnull GroupServiceBlockingStub groupService) {
        this.groupResolver = groupResolver;
        this.groupService = groupService;
    }

    /**
     * Disables commodities which changes contains for specified groups.
     *
     * @param graph to resolve groups members
     * @param changes with ignore constraint settings
     * @param isPressurePlan is true if plan is of type alleviate pressure
     * @throws ConstraintsEditorException thrown if {@link IgnoreConstraint} has unsupported configs.
     */
    public void editConstraints(@Nonnull final TopologyGraph<TopologyEntity> graph,
            @Nonnull final List<ScenarioChange> changes, boolean isPressurePlan)
            throws ConstraintsEditorException {
        final Multimap<Long, String> entitiesToIgnoredCommodities = HashMultimap.create();
        for (ScenarioChange change: changes) {
            if (change.hasPlanChanges()) {
                final List<IgnoreConstraint> ignoreConstraints = change
                        .getPlanChanges().getIgnoreConstraintsList();
                if (!CollectionUtils.isEmpty(ignoreConstraints)) {
                    getEntitiesOidsForIgnoredCommodities(ignoreConstraints, graph, isPressurePlan)
                            .forEach((entityId, ignoredCommodity) -> {
                                Collection<String> commodities = entitiesToIgnoredCommodities.get(entityId);
                                if (!commodities.contains(ALL_COMMODITIES)) {
                                    entitiesToIgnoredCommodities.put(entityId, ignoredCommodity);
                                }
                            });
                }
            }
        }

        deactivateCommodities(graph, entitiesToIgnoredCommodities);
    }

    /**
     * Checks if plan configured with unsupported {@link IgnoreConstraint}s options.
     *
     * @param ignoredCommodities the list of ignoreConstraints to check for proper configuration
     * @return true if any ignoreCommodities is not supported.
     */
    @VisibleForTesting
    static boolean hasUnsupportedIgnoreConstraintConfigurations(@Nonnull List<IgnoreConstraint> ignoredCommodities) {
        return ignoredCommodities.stream()
                .anyMatch(ignoreConstraint ->  {
                    boolean deprecatedFieldConfiguration = ignoreConstraint.hasDeprecatedIgnoreEntityTypes();
                    boolean misConfiguredGroup = ignoreConstraint.hasIgnoreGroup() && !ignoreConstraint.getIgnoreGroup().hasGroupUuid();
                    return deprecatedFieldConfiguration || misConfiguredGroup;
                });
    }

    /**
     * Maps entityOids to commodities to ignore based on ignoreConstraints.
     *
     * @param ignoredCommodities ignoreConstraint changes
     * @param graph to resolve groups members
     * @param isPressurePlan is true if plan is of type alleviate pressure
     * @return entityOids to commodities to ignore
     * @throws ConstraintsEditorException thrown if {@link IgnoreConstraint} has unsupported configs.
     */
    @Nonnull
    private Multimap<Long, String> getEntitiesOidsForIgnoredCommodities(
        @Nonnull List<IgnoreConstraint> ignoredCommodities,
        @Nonnull TopologyGraph<TopologyEntity> graph, boolean isPressurePlan)
            throws ConstraintsEditorException {

        final Multimap<Long, String> entitesToIgnoredCommodities = HashMultimap.create();
        boolean hasIgnoreAllEntities = ignoredCommodities.stream()
                .anyMatch(IgnoreConstraint::hasIgnoreAllEntities);

        // Older IgnoreConstraints configurations no longer supported.  We are trying
        // to detect them here, in which case we fail plan and prompt user to create a new one
        // rather then running a plan without applying ignoreConstraints as originally intended.
        if (hasUnsupportedIgnoreConstraintConfigurations(ignoredCommodities)) {
            throw new ConstraintsEditorException("Ignore Constraint configurations not longer supported," +
                    "please reconfigure a new plan");
        }

        // First check if all entities have to be ignored.
        if (hasIgnoreAllEntities) {
            graph.entities()
                    .forEach(entity -> {
                        entitesToIgnoredCommodities.put(entity.getOid(), ALL_COMMODITIES);
                        if (entity.getEntityType() == EntityType.VIRTUAL_MACHINE_VALUE) {
                            entity.getTopologyEntityDtoBuilder().getAnalysisSettingsBuilder()
                                .setShopTogether(true);
                        }
                    });
            return entitesToIgnoredCommodities;
        }
        
        // Check if all entities of specific types have to be ignored.
        ignoredCommodities.stream()
                .filter(IgnoreConstraint::hasGlobalIgnoreEntityType)
                .map(IgnoreConstraint::getGlobalIgnoreEntityType)
                .map(GlobalIgnoreEntityType::getEntityType)
                .map(entityType -> graph.entitiesOfType(entityType))
                .flatMap(Function.identity())
                .forEach(entity -> {
                    entitesToIgnoredCommodities.put(entity.getOid(), ALL_COMMODITIES);
                    if (entity.getEntityType() == EntityType.VIRTUAL_MACHINE_VALUE) {
                        entity.getTopologyEntityDtoBuilder().getAnalysisSettingsBuilder()
                                .setShopTogether(true);
                    }
                });

        Set<Long> groups = ignoredCommodities.stream()
                .filter(IgnoreConstraint::hasIgnoreGroup)
                .map(IgnoreConstraint::getIgnoreGroup)
                .map(ConstraintGroup::getGroupUuid)
                .collect(Collectors.toSet());

        groupService.getGroups(GetGroupsRequest.newBuilder()
                        .setGroupFilter(GroupFilter.newBuilder()
                                        .addAllId(groups))
                        .setReplaceGroupPropertyWithGroupMembershipFilter(true)
                        .build())
                .forEachRemaining(group -> {
                    try {
                        Set<Long> groupMembersOids = groupResolver.resolve(group, graph);
                        // Remove entityIds for which we have already determined that all commodity
                        // constraints have to be ignored.
                        groupMembersOids.removeAll(entitesToIgnoredCommodities.keySet());
                        if (isPressurePlan) {
                           // VMs on hosts in a cluster will have other constraints (e.g. Datacenter, Network)
                           // that may stop them from being moved. In an alleviate pressure plan,
                           // we need to ignore these constraints on VM consumers of the hot cluster members
                           // so that the market can simulate moves of these VMs to the cold cluster(s).
                           // Also, update commodities sold for PMs of hot cluster s.t there are no moves to
                           // hot cluster from cold cluster.
                            groupMembersOids.addAll(
                                updateCommoditiesSoldForHostAndGetVMCustomers(groupMembersOids, graph));
                        }
                        final Set<String> commoditiesOfGroup = ignoredCommodities.stream()
                                .filter(IgnoreConstraint::hasIgnoreGroup)
                                .map(IgnoreConstraint::getIgnoreGroup)
                                .filter(commodity -> commodity.getGroupUuid() == group.getId())
                                .map(ConstraintGroup::getCommodityType)
                                .collect(Collectors.toSet());
                        // GlobalIgnoreConstraint means we need to disable all access commodities
                        if (commoditiesOfGroup.contains(ConstraintType.GlobalIgnoreConstraint.name())) {
                            commoditiesOfGroup.remove(ConstraintType.GlobalIgnoreConstraint.name());
                            commoditiesOfGroup.add(ALL_COMMODITIES);
                        }
                        groupMembersOids.forEach(entityId ->
                            entitesToIgnoredCommodities.putAll(entityId, commoditiesOfGroup));
                    } catch (GroupResolutionException e) {
                        logger.warn("Cannot resolve member for group {}", group);
                    }
                });

        return entitesToIgnoredCommodities;
    }

    /**
     * For given Oids, update commodities sold for PMs and find VM customers.
     * @param groupMembersOids Oids to iterate over.
     * @param graph to find entities from.
     * @return set of original Oids with VM customers.
     */
    private Set<Long> updateCommoditiesSoldForHostAndGetVMCustomers(Set<Long> groupMembersOids,
                    TopologyGraph<TopologyEntity> graph) {
        return groupMembersOids.stream()
            .map(oid -> graph.getEntity(oid))
            .filter(Optional::isPresent)
            .map(Optional::get)
            .flatMap(entity -> {
                deactivateCommoditiesSoldForPM(entity);
                return entity.getConsumers().stream();
            })
            .filter(customer -> customer.getEntityType() == EntityType.VIRTUAL_MACHINE_VALUE)
            .map(TopologyEntity::getOid)
            .collect(Collectors.toSet());
    }

    private void deactivateCommoditiesSoldForPM(TopologyEntity entity) {
        if (entity.getEntityType() != EntityType.PHYSICAL_MACHINE_VALUE) {
            return;
        }
        final TopologyEntityDTO.Builder entityBuilder = entity.getTopologyEntityDtoBuilder();
        entityBuilder.getCommoditySoldListBuilderList().stream()
            .filter(commSoldBldr -> IGNORED_COMMODITIES_FOR_PRESSURE_PLAN
                            .contains(commSoldBldr.getCommodityType().getType()))
            .forEach(commSoldBldr -> commSoldBldr.setActive(false));
    }

    @Nonnull
    private void deactivateCommodities(@Nonnull TopologyGraph<TopologyEntity> graph,
            @Nonnull Multimap<Long, String> entitiesToIgnoredCommodities) {
        for (Long entityId : entitiesToIgnoredCommodities.keySet()) {
            final Optional<TopologyEntity> entity = graph.getEntity(entityId);
            if (!entity.isPresent()) {
                continue;
            }
            final TopologyEntityDTO.Builder entityBuilder = entity.get().getTopologyEntityDtoBuilder();
            final List<CommoditiesBoughtFromProvider> commoditiesBoughtFromProvider =
                    entityBuilder.getCommoditiesBoughtFromProvidersList();
            final List<CommoditiesBoughtFromProvider> deactivatedCommodities =
                    getCommoditiesBoughtFromProviderDeactivated(commoditiesBoughtFromProvider,
                            entitiesToIgnoredCommodities.get(entityId));
            entityBuilder.clearCommoditiesBoughtFromProviders()
                    .addAllCommoditiesBoughtFromProviders(deactivatedCommodities);
        }
    }

    @Nonnull
    private List<CommoditiesBoughtFromProvider> getCommoditiesBoughtFromProviderDeactivated(
            @Nonnull List<CommoditiesBoughtFromProvider> commoditiesBoughtFromProvider,
            @Nonnull Collection<String> commoditiesToDeactivate) {
        final ImmutableList.Builder<CommoditiesBoughtFromProvider> deactivatedCommodities
                = ImmutableList.builder();
        for (CommoditiesBoughtFromProvider commodities : commoditiesBoughtFromProvider) {
            final List<CommodityBoughtDTO> deactivatedCommodityBoughtDTOS =
                    deactivateIgnoredCommodities(commodities, commoditiesToDeactivate);
            final CommoditiesBoughtFromProvider deactivatedCommoditesBoughtFromProvider =
                    CommoditiesBoughtFromProvider
                            .newBuilder(commodities)
                            .clearCommodityBought()
                            .addAllCommodityBought(deactivatedCommodityBoughtDTOS)
                            .build();
            deactivatedCommodities.add(deactivatedCommoditesBoughtFromProvider);
        }
        return deactivatedCommodities.build();
    }

    @Nonnull
    private List<CommodityBoughtDTO> deactivateIgnoredCommodities(
            @Nonnull CommoditiesBoughtFromProvider commodities,
            @Nonnull Collection<String> commoditiesToDeactivate) {
        final ImmutableList.Builder<CommodityBoughtDTO> deactivatedCommodities = ImmutableList.builder();
        boolean ignoreAll =
                commoditiesToDeactivate.stream()
                        .anyMatch(commodity -> commodity.equals(ALL_COMMODITIES));
        final Set<Integer> ignoredTypes = commoditiesToDeactivate.stream()
                .map(COMMODITY_NAME_TO_COMMODITY_TYPE::get)
                .collect(Collectors.toSet());
        for (CommodityBoughtDTO commodity : commodities.getCommodityBoughtList()) {
            if ((ignoreAll && commodity.getCommodityType().hasKey()) ||
                    ignoredTypes.contains(commodity.getCommodityType().getType())) {
                deactivatedCommodities.add(CommodityBoughtDTO.newBuilder(commodity)
                        .setActive(false).build());
            } else {
                deactivatedCommodities.add(commodity);
            }
        }
        return deactivatedCommodities.build();
    }

    /**
     * An exception thrown {@link ConstraintsEditor} stage fails.
     */
    public static class ConstraintsEditorException extends Exception {

        /**
         * Create a new {@link ConstraintsEditorException}.
         *
         * @param message The message describing the exception.
         */
        public ConstraintsEditorException(@Nonnull final String message) {
            super(message);
        }
    }

}
