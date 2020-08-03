package com.vmturbo.topology.processor.group.discovery;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.platform.common.builders.SDKConstants;
import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.ConstraintType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.processor.entity.Entity;
import com.vmturbo.topology.processor.entity.EntityStore;

/**
 * This class is used to handle discovered cluster constraints which come from Probe discovery response.
 * The first step is when receive discovery response, it tries to parse {@link CommonDTO.GroupDTO}
 * and store cluster constraint relationship about who are providers and excluded consumers. The second
 * step is when broadcast happens, it will add a cluster constraint to related providers and consumers.
 */
public class DiscoveredClusterConstraintCache {

    private final Logger logger = LogManager.getLogger();

    private final EntityStore entityStore;

    private final Map<Long, List<DiscoveredClusterConstraint>> discoveredClusterConstraintMap;

    public DiscoveredClusterConstraintCache(@Nonnull final EntityStore entityStore) {
        this.entityStore = Objects.requireNonNull(entityStore);
        this.discoveredClusterConstraintMap= new ConcurrentHashMap<>();
    }

    /**
     * Parse a list of {@link CommonDTO.GroupDTO}, and convert to a list of {@link DiscoveredClusterConstraint}
     * which contains cluster constraint relationship about who are providers and excluded consumers.
     * And also store the results in {@link DiscoveredClusterConstraintCache#discoveredClusterConstraintMap}.
     *
     * @param targetId id of target.
     * @param groups a list of {@link CommonDTO.GroupDTO} comes from Probe discovery response.
     */
    public void storeDiscoveredClusterConstraint(final long targetId,
            @Nonnull List<CommonDTO.GroupDTO> groups) {
        if (groups.isEmpty()) {
            // remove cached cluster constraint of targetId, it will happen when removing some targets.
            discoveredClusterConstraintMap.remove(targetId);
            return;
        }
        final Optional<Map<String, Long>> targetEntityIdMap =
                entityStore.getTargetEntityIdMap(targetId);
        if (!targetEntityIdMap.isPresent()) {
            logger.warn("No entity ID map available for target {}", targetId);
            return;
        }
        final Map<String, List<CommonDTO.GroupDTO>> clusterConstraintMap = groups.stream()
                .filter(CommonDTO.GroupDTO::hasConstraintInfo)
                .filter(groupDTO -> groupDTO.getConstraintInfo()
                        .getConstraintType().equals(ConstraintType.CLUSTER))
                .collect(Collectors.groupingBy(groupDTO -> groupDTO
                        .getConstraintInfo().getConstraintId()));
        final List<DiscoveredClusterConstraint> latestDiscoveredClusterConstraints =
                createLatestDiscoveredClusterConstraints(clusterConstraintMap, targetId);
        discoveredClusterConstraintMap.put(targetId, latestDiscoveredClusterConstraints);
    }

    /**
     * Based on current cluster constraint information in {@link DiscoveredClusterConstraintCache#
     * discoveredClusterConstraintMap}, add a cluster commodity to related providers and consumers.
     *
     * @param topologyGraph a graph of topology.
     */
    public void applyClusterCommodity(@Nonnull final TopologyGraph<TopologyEntity> topologyGraph) {
        discoveredClusterConstraintMap.values().stream()
                .flatMap(List::stream)
                .filter(discoveredClusterConstraint ->
                        !discoveredClusterConstraint.getProviderIds().isEmpty())
                // not need to add cluster commodity if entity has already contains cluster commodity.
                .filter(discoveredClusterConstraint -> !hasClusterConstraint(topologyGraph,
                        discoveredClusterConstraint))
                .forEach(discoveredClusterConstraint -> {
                    final Set<Long> providerIds = discoveredClusterConstraint.getProviderIds();
                    final Set<Long> consumerIds = getIncludedConsumers(topologyGraph, discoveredClusterConstraint);
                    final CommodityType clusterCommodity =
                            generateClusterCommodity(discoveredClusterConstraint.getProviderEntityType(),
                                    discoveredClusterConstraint.getConstraintId());
                    applyClusterCommodityForProvider(topologyGraph, providerIds, clusterCommodity);
                    applyClusterCommodityForConsumer(topologyGraph, consumerIds, providerIds,
                            discoveredClusterConstraint.getProviderEntityType(), clusterCommodity);
                });
    }

    /**
     * Get a list of current {@link DiscoveredClusterConstraint} which belongs to input targetId.
     *
     * @param targetId id of target.
     * @return a list of {@link DiscoveredClusterConstraint}
     */
    public Optional<List<DiscoveredClusterConstraint>> getClusterConstraintByTarget(final long targetId) {
        return Optional.ofNullable(discoveredClusterConstraintMap.get(targetId));
    }

    /**
     * Generate a list of {@link DiscoveredClusterConstraint} based on input clusterConstraintMap,
     * for each value of clusterConstraintMap, it tries to create a {@link DiscoveredClusterConstraint}.
     *
     * @param clusterConstraintMap a Map which key is constraint id, value is a list of
     *                             {@link CommonDTO.GroupDTO}.
     * @param targetId id of target.
     * @return a list of {@link DiscoveredClusterConstraint}.
     */
    private List<DiscoveredClusterConstraint> createLatestDiscoveredClusterConstraints(
            @Nonnull final Map<String, List<CommonDTO.GroupDTO>> clusterConstraintMap,
            final long targetId) {
        return clusterConstraintMap.entrySet().stream()
                .map(entry -> convertToDiscoveredClusterConstraint(entry.getValue(), targetId,
                        entry.getKey()))
                .collect(Collectors.toList());
    }

    /**
     * Generate a {@link DiscoveredClusterConstraint} based on input a list of {@link CommonDTO.GroupDTO}.
     *
     * @param groupDTO a list of {@link CommonDTO.GroupDTO}.
     * @param targetId id of target.
     * @param constraintId id of cluster constraint.
     * @return {@link DiscoveredClusterConstraint}.
     */
    private DiscoveredClusterConstraint convertToDiscoveredClusterConstraint(
            @Nonnull final List<CommonDTO.GroupDTO> groupDTO,
            final long targetId,
            @Nonnull final String constraintId) {
        final DiscoveredClusterConstraint discoveredClusterConstraint =
                new DiscoveredClusterConstraint(constraintId);
        // cluster constraint has no more than two groups. And it is possible have only one group.
        if (groupDTO.size() > 2) {
            return discoveredClusterConstraint;
        }
        for (CommonDTO.GroupDTO group : groupDTO) {
            final List<Long> groupMemberIds = parseGroupMembers(group, targetId);
            if (group.getConstraintInfo().getForExcludedConsumers()) {
                discoveredClusterConstraint.addExcludedConsumerIds(groupMemberIds);
            } else if (!group.getConstraintInfo().getIsBuyer()){
                discoveredClusterConstraint.addProviderIds(groupMemberIds);
                discoveredClusterConstraint.setProviderEntityType(group.getEntityType().getNumber());
            } else {
                logger.warn("Invalid cluster constraint groupDTO: " + group);
            }
        }
        return discoveredClusterConstraint;
    }

    /**
     * Convert group member SDK entity Id to our internal entity oid through {@link EntityStore}
     * and input targetId.
     *
     * @param groupDTO a {@link CommonDTO.GroupDTO}.
     * @param targetId id of target.
     * @return a list of group member entity oid.
     */
    private List<Long> parseGroupMembers(@Nonnull final CommonDTO.GroupDTO groupDTO,
                                         final long targetId) {
        // has already checked entityStore contains targetId.
        final Map<String, Long> targetEntityIdMap = entityStore.getTargetEntityIdMap(targetId).get();
        return groupDTO.getMemberList().getMemberList().stream()
                .map(targetEntityIdMap::get)
                .map(Optional::ofNullable)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .map(entityStore::getEntity)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .filter(entity -> entity.getEntityType() == groupDTO.getEntityType())
                .map(Entity::getId)
                .collect(Collectors.toList());
    }

    /**
     * Generate a cluster constraint type based on input provider entity type. If provider entity
     * type is Storage, it will create a Cluster Storage commodity, otherwise it create a Cluster
     * commodity.
     *
     * @param providerEntityType provider entity type.
     * @param constraintId id of cluster constraint.
     * @return {@link CommodityType}.
     */
    private CommodityType generateClusterCommodity(final int providerEntityType,
                                                   @Nonnull final String constraintId) {
        final CommodityType.Builder clusterCommodityBuilder = CommodityType.newBuilder();
        if (providerEntityType == EntityType.STORAGE_VALUE) {
            clusterCommodityBuilder.setType(CommodityDTO.CommodityType.STORAGE_CLUSTER_VALUE);
        } else {
            clusterCommodityBuilder.setType(CommodityDTO.CommodityType.CLUSTER_VALUE);
        }
        clusterCommodityBuilder.setKey(constraintId);
        return clusterCommodityBuilder.build();
    }

    /**
     * Check if input discoveredClusterConstraint consumer and provider both have already contains
     * cluster commodity, if so, return true, otherwise, return false;
     *
     * @param topologyGraph a graph of topology.
     * @param discoveredClusterConstraint {@link DiscoveredClusterConstraint}.
     * @return a boolean.
     */
    private boolean hasClusterConstraint(
            @Nonnull final TopologyGraph<TopologyEntity> topologyGraph,
            @Nonnull final DiscoveredClusterConstraint discoveredClusterConstraint) {
        return hasClusterConstraintForProvider(topologyGraph, discoveredClusterConstraint.providerIds) &&
                hasClusterConstraintForConsumer(topologyGraph, getIncludedConsumers(topologyGraph,
                        discoveredClusterConstraint), discoveredClusterConstraint);
    }

    /**
     * Check if providers of {@link DiscoveredClusterConstraint} has already contains cluster constraint
     * or not.
     *
     * @param topologyGraph a graph of topology.
     * @param providerIds a set of provider ids.
     * @return a boolean.
     */
    private boolean hasClusterConstraintForProvider(
            @Nonnull final TopologyGraph<TopologyEntity> topologyGraph,
            @Nonnull final Set<Long> providerIds) {
        return providerIds.stream()
                .map(topologyGraph::getEntity)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .map(TopologyEntity::getTopologyEntityDtoBuilder)
                .anyMatch(topologyEntity -> (
                    topologyEntity.getEntityType() == EntityType.STORAGE_VALUE ?
                        commoditySoldHasConstraint(topologyEntity.getCommoditySoldListList(),
                                CommodityDTO.CommodityType.STORAGE_CLUSTER_VALUE) :
                        commoditySoldHasConstraint(topologyEntity.getCommoditySoldListList(),
                                CommodityDTO.CommodityType.CLUSTER_VALUE)));
    }

    /**
     * Check if a list of {@link CommoditySoldDTO} contains input constraint or not.
     *
     * @param commoditySoldDTOs a list of {@link CommoditySoldDTO}.
     * @param constraint constraint type.
     * @return a boolean.
     */
    private boolean commoditySoldHasConstraint(@Nonnull final List<CommoditySoldDTO> commoditySoldDTOs,
                                               final int constraint) {
        return commoditySoldDTOs.stream()
                .anyMatch(commoditySold -> commoditySold.getCommodityType().getType() == constraint);
    }

    /**
     * Check if consumers of {@link DiscoveredClusterConstraint} has already contains cluster constraint
     * or not.
     *
     * @param topologyGraph a graph of topology.
     * @param consumerIds a set of consumer ids.
     * @param discoveredClusterConstraint {@link DiscoveredClusterConstraint}.
     * @return a boolean.
     */
    private boolean hasClusterConstraintForConsumer(
            @Nonnull final TopologyGraph<TopologyEntity> topologyGraph,
            @Nonnull final Set<Long> consumerIds,
            @Nonnull final DiscoveredClusterConstraint discoveredClusterConstraint) {
        return consumerIds.stream()
                .map(topologyGraph::getEntity)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .map(TopologyEntity::getTopologyEntityDtoBuilder)
                .anyMatch(topologyEntity -> commodityBoughtGroupHasConstraint(topologyEntity,
                        discoveredClusterConstraint));
    }

    /**
     * Check if {@link TopologyEntityDTO.Builder} has already contains cluster constraints or not.
     *
     * @param topologyEntity {@link TopologyEntityDTO.Builder}.
     * @param discoveredClusterConstraint {@link DiscoveredClusterConstraint}.
     * @return a boolean.
     */
    private boolean commodityBoughtGroupHasConstraint(
            @Nonnull final TopologyEntityDTO.Builder topologyEntity,
            @Nonnull final DiscoveredClusterConstraint discoveredClusterConstraint) {
        final Set<Long> providerIds = discoveredClusterConstraint.getProviderIds();
        return topologyEntity.getCommoditiesBoughtFromProvidersList().stream()
                .filter(commodityBoughtGroup -> providerIds.contains(commodityBoughtGroup.getProviderId()))
                .anyMatch(commodityBoughtGroup ->
                        discoveredClusterConstraint.getProviderEntityType() == EntityType.STORAGE_VALUE ?
                            commodityBoughtHasConstraint(commodityBoughtGroup.getCommodityBoughtList(),
                                    CommodityDTO.CommodityType.STORAGE_CLUSTER_VALUE) :
                                commodityBoughtHasConstraint(commodityBoughtGroup.getCommodityBoughtList(),
                                        CommodityDTO.CommodityType.CLUSTER_VALUE));
    }

    /**
     * Check if a list of {@link CommodityBoughtDTO} contains input constraint or not.
     *
     * @param commodityBoughtDTOs a list of {@link CommodityBoughtDTO}.
     * @param constraint constraint type need to match.
     * @return a boolean.
     */
    private boolean commodityBoughtHasConstraint(
            @Nonnull final List<CommodityBoughtDTO> commodityBoughtDTOs,
            final int constraint) {
        return commodityBoughtDTOs.stream()
                .anyMatch(commodityBought -> commodityBought.getCommodityType().getType() == constraint);
    }

    /**
     * Get included consumers which belong to this cluster constraints.
     *
     * @param topologyGraph a graph of topology.
     * @param discoveredClusterConstraint {@link DiscoveredClusterConstraint}.
     * @return a set of consumer ids.
     */
    private Set<Long> getIncludedConsumers(
            @Nonnull final TopologyGraph<TopologyEntity> topologyGraph,
            @Nonnull final DiscoveredClusterConstraint discoveredClusterConstraint) {
        return getAllConsumerIds(discoveredClusterConstraint, topologyGraph).stream()
                .filter(consumerId ->
                        !discoveredClusterConstraint.getExcludedConsumerIds()
                                .contains(consumerId))
                .collect(Collectors.toSet());
    }

    /**
     * Add cluster constraint commodity to all providers.
     *
     * @param topologyGraph a graph of topology.
     * @param providerIds a set of provider ids.
     * @param clusterCommodity cluster Commodity type.
     */
    private void applyClusterCommodityForProvider(@Nonnull final TopologyGraph<TopologyEntity> topologyGraph,
                                                  @Nonnull final Set<Long> providerIds,
                                                  @Nonnull final CommodityType clusterCommodity) {
        for (Long providerId : providerIds) {
            final Optional<TopologyEntity> providerEntity = topologyGraph.getEntity(providerId);
            if (!providerEntity.isPresent()) {
                logger.warn("Can not find oid {} in topology.", providerId);
            } else {
                providerEntity.get().getTopologyEntityDtoBuilder()
                        .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                                .setCommodityType(clusterCommodity)
                                .setCapacity(SDKConstants.ACCESS_COMMODITY_CAPACITY)
                                .setIsResizeable(false));
            }
        }
    }

    /**
     * Add cluster constraint commodity to all consumers.
     *
     * @param topologyGraph a graph of topology.
     * @param consumerIds a set of consumer ids.
     * @param providerIds a set of provider ids.
     * @param providerEntityType provider entity type.
     * @param clusterCommodity cluster Commodity type.
     */
    private void applyClusterCommodityForConsumer(@Nonnull final TopologyGraph<TopologyEntity> topologyGraph,
                                                  @Nonnull final Set<Long> consumerIds,
                                                  @Nonnull final Set<Long> providerIds,
                                                  final int providerEntityType,
                                                  @Nonnull final CommodityType clusterCommodity) {
        for (Long consumerId : consumerIds) {
            final Optional<TopologyEntity> consumerEntity = topologyGraph.getEntity(consumerId);
            if (!consumerEntity.isPresent()) {
                logger.warn("Can not find oid {} in topology.", consumerId);
            } else {
                consumerEntity.get().getTopologyEntityDtoBuilder()
                        .getCommoditiesBoughtFromProvidersBuilderList().stream()
                        .filter(commodityBoughtGroup ->
                                commodityBoughtGroup.getProviderEntityType() == providerEntityType &&
                                providerIds.contains(commodityBoughtGroup.getProviderId()))
                        .forEach(commodityBoughtBuilder ->
                                commodityBoughtBuilder.addCommodityBought(
                                        CommodityBoughtDTO.newBuilder()
                                                .setCommodityType(clusterCommodity)
                                                .setUsed(1.0d)));
            }
        }
    }

    /**
     * For providers of input {@link DiscoveredClusterConstraint}, find all consumer ids which buying
     * from those providers.
     *
     * @param discoveredClusterConstraint a {@link DiscoveredClusterConstraint}.
     * @param topologyGraph a graph of topology.
     * @return a set of consumer ids.
     */
    private Set<Long> getAllConsumerIds(
            @Nonnull final DiscoveredClusterConstraint discoveredClusterConstraint,
            @Nonnull TopologyGraph<TopologyEntity> topologyGraph) {
        final Set<Long> allConsumerIds = new HashSet<>();
        for (Long providerId : discoveredClusterConstraint.getProviderIds()) {
            final Optional<TopologyEntity> providerEntity = topologyGraph.getEntity(providerId);
            if (!providerEntity.isPresent()) {
                logger.warn("Can not find oid {} in topology.", providerId);
            } else {
                allConsumerIds.addAll(providerEntity.get().getConsumers().stream()
                        .map(TopologyEntity::getOid)
                        .collect(Collectors.toList()));
            }
        }
        return allConsumerIds;
    }

    /**
     * It is wrapper class represent relationship about who are providers and excluded consumers
     * within one cluster constraint.
     */
    public static class DiscoveredClusterConstraint {
        // all provider ids in this cluster constraint.
        private final Set<Long> providerIds;
        // consumers need to be excluded, that means all other consumers of providers will belong to
        // this cluster constraint. The reason why can not directly have included consumers is
        // that when parsing Probe discovery response, it doesn't generate topology graph yet, it
        // can not find consumer provider relationship.
        private final Set<Long> excludedConsumerIds;
        // entity type of providers.
        private int providerEntityType;
        // The Id of cluster constraint.
        private String constraintId;

        public DiscoveredClusterConstraint(@Nonnull final String constraintId) {
            this.providerIds = new HashSet<>();
            this.excludedConsumerIds = new HashSet<>();
            this.constraintId = constraintId;
        }

        public void addProviderIds(@Nonnull final List<Long> providerIds) {
            this.providerIds.addAll(providerIds);
        }

        public void addExcludedConsumerIds(@Nonnull final List<Long> consumerIds) {
            this.excludedConsumerIds.addAll(consumerIds);
        }

        public Set<Long> getProviderIds() {
            return this.providerIds;
        }

        public Set<Long> getExcludedConsumerIds() {
            return this.excludedConsumerIds;
        }

        public void setProviderEntityType(final int providerEntityType) {
            this.providerEntityType = providerEntityType;
        }

        public int getProviderEntityType() {
            return this.providerEntityType;
        }

        public String getConstraintId() {
            return this.constraintId;
        }
    }
}
