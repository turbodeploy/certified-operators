package com.vmturbo.topology.processor.topology;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;

import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupID;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.PlanChanges.IgnoreConstraint;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.platform.common.dto.CommonDTOREST.CommodityDTO.CommodityType;
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
     */
    public void editConstraints(@Nonnull final TopologyGraph graph,
            @Nonnull final List<ScenarioChange> changes) {
        final Multimap<Long, String> entitiesToIgnoredCommodities = HashMultimap.create();
        changes.forEach(change -> {
            if (change.hasPlanChanges()) {
                final List<IgnoreConstraint> ignoreConstraints = change
                        .getPlanChanges().getIgnoreConstraintsList();
                if (!CollectionUtils.isEmpty(ignoreConstraints)) {
                    entitiesToIgnoredCommodities.putAll(
                            getEntitiesOidsForIgnoredCommodities(ignoreConstraints, graph));
                }
            } else {
                logger.warn("Unimplemented handling for change of type {}", change.getDetailsCase());
            }
        });

        deactivateCommodities(graph, entitiesToIgnoredCommodities);
    }

    @Nonnull
    private Multimap<Long, String> getEntitiesOidsForIgnoredCommodities(
            @Nonnull List<IgnoreConstraint> ignoredCommodities, @Nonnull TopologyGraph graph) {
        Set<Long> groups = ignoredCommodities.stream()
                .map(IgnoreConstraint::getGroupUuid)
                .collect(Collectors.toSet());
        final Multimap<Long, String> entitesToIgnoredCommodities = HashMultimap.create();
        for (long groupId : groups) {
            final GetGroupResponse groupResponse = groupService.getGroup(
                    GroupID.newBuilder().setId(groupId).build());
            try {
                final Set<Long> groupMembersOids = groupResolver.resolve(groupResponse.getGroup(), graph);
                final Set<String> commoditiesOfGroup = ignoredCommodities.stream()
                        .filter(commodity -> commodity.getGroupUuid() == groupId)
                        .map(IgnoreConstraint::getCommodityType)
                        .collect(Collectors.toSet());
                groupMembersOids.forEach(entityId ->
                        entitesToIgnoredCommodities.putAll(entityId, commoditiesOfGroup));
            } catch (GroupResolutionException e) {
                logger.warn("Cannot resolve member for group {}", groupResponse.getGroup());
            }
        }
        return entitesToIgnoredCommodities;
    }

    @Nonnull
    private void deactivateCommodities(@Nonnull TopologyGraph graph,
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
        final Set<Integer> ignoredTypes = commoditiesToDeactivate.stream()
                .map(COMMODITY_NAME_TO_COMMODITY_TYPE::get)
                .collect(Collectors.toSet());
        for (CommodityBoughtDTO commodity : commodities.getCommodityBoughtList()) {
            if (ignoredTypes.contains(commodity.getCommodityType().getType())) {
                deactivatedCommodities.add(CommodityBoughtDTO.newBuilder(commodity)
                        .setActive(false).build());
            } else {
                deactivatedCommodities.add(commodity);
            }
        }
        return deactivatedCommodities.build();
    }

}
