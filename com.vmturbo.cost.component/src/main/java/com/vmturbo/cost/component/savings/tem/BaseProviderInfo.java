package com.vmturbo.cost.component.savings.tem;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.cloud.common.topology.CloudTopology;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionSpec;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.action.ActionDTO.ResizeInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.Scale;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;

/**
 * Base implementation for ProviderInfo. It provides logic for setting and comparing provider OID
 * and commodity values. If an entity requires additional attributes to identify the provider,
 * extend this class to save to provide the required functionality.
 */
public abstract class BaseProviderInfo implements ProviderInfo {

    protected final Long providerOid;

    protected final Map<Integer, Double> commodityCapacities;

    protected final int entityType;

    private final Logger logger = LogManager.getLogger();

    /**
     * Constructor. Constructs a ProviderInfo object from a TopologyEntityDTO object.
     *
     * @param cloudTopology cloud topology, for looking up provider of an entity
     * @param discoveredEntity the discovered entity
     * @param getProvider the function to use to get provider given the cloud topology and entity OID
     * @param monitoredCommodities the set of monitored commodities for this entity type
     */
    public BaseProviderInfo(CloudTopology<TopologyEntityDTO> cloudTopology,
            TopologyEntityDTO discoveredEntity,
            BiFunction<CloudTopology<TopologyEntityDTO>, Long, Optional<TopologyEntityDTO>> getProvider,
            Set<Integer> monitoredCommodities) {
        this.providerOid = getDiscoveredProviderId(cloudTopology, discoveredEntity, getProvider).orElse(null);
        this.commodityCapacities = getDiscoveredMonitoredCommodities(discoveredEntity, monitoredCommodities);
        this.entityType = discoveredEntity.getEntityType();
    }

    /**
     * Constructor.
     *
     * @param providerOid provider OID
     * @param commodityCapacities commodity capacities
     * @param entityType entity type of the discovered entity (NOT the type of the provider)
     */
    public BaseProviderInfo(Long providerOid, Map<Integer, Double> commodityCapacities, int entityType) {
        this.providerOid = providerOid;
        this.commodityCapacities = commodityCapacities;
        this.entityType = entityType;
    }

    /**
     * Get the provider ID of a discovered entity.
     *
     * @param cloudTopology cloud topology
     * @param discoveredEntity discovered entity
     * @param getProvider the method to be called on the cloud topology to retrieve the provider DTO
     * @return the provider OID wrapped in Optional
     */
    @Nonnull
    private Optional<Long> getDiscoveredProviderId(CloudTopology<TopologyEntityDTO> cloudTopology,
            TopologyEntityDTO discoveredEntity,
            BiFunction<CloudTopology<TopologyEntityDTO>, Long, Optional<TopologyEntityDTO>> getProvider) {
        TopologyEntityDTO providerEntity = getProvider.apply(cloudTopology,
                discoveredEntity.getOid()).orElse(null);
        if (providerEntity != null) {
            return Optional.of(providerEntity.getOid());
        }
        return Optional.empty();
    }

    @Nonnull
    private Map<Integer, Double> getDiscoveredMonitoredCommodities(TopologyEntityDTO discoveredEntity,
            Set<Integer> monitoredCommodities) {
        // Check for changed commodity usage
        if (!monitoredCommodities.isEmpty()) {
            return getCommodityCapacitiesFromDTO(discoveredEntity, monitoredCommodities);
        }
        return ImmutableMap.of();
    }

    @Nonnull
    private Map<Integer, Double> getCommodityCapacitiesFromDTO(TopologyEntityDTO entityDTO,
            Set<Integer> commodityTypes) {
        Map<Integer, Double> values = new HashMap<>();
        for (CommoditySoldDTO commSold : entityDTO.getCommoditySoldListList()) {
            if (commodityTypes.contains(commSold.getCommodityType().getType())) {
                values.put(commSold.getCommodityType().getType(), commSold.getCapacity());
            }
        }
        return values;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BaseProviderInfo that = (BaseProviderInfo)o;
        return entityType == that.entityType && Objects.equals(providerOid, that.providerOid)
                && commodityCapacities.equals(that.commodityCapacities);
    }

    @Override
    public int hashCode() {
        return Objects.hash(providerOid, commodityCapacities, entityType);
    }

    /**
     * Get the provider OID.
     *
     * @return provider OID
     */
    public Long getProviderOid() {
        return providerOid;
    }

    /**
     * Get the map of commodity capacities (commodity -> capacity).
     *
     * @return commodity capacity map
     */
    public Map<Integer, Double> getCommodityCapacities() {
        return commodityCapacities;
    }

    /**
     * Get the entity type of the entity.
     *
     * @return entity type
     */
    @Override
    public int getEntityType() {
        return entityType;
    }

    private Map<Integer, Double> getCommInfoFromScaleAction(final ActionInfo actionInfo,
                                                            final boolean isDestinationCheck) {
        final Map<Integer, Double> actionCommCapacities = new HashMap<>();
        if (actionInfo.hasScale()) {
            final Scale scale = actionInfo.getScale();
            final List<ResizeInfo> resizeInfoList = scale.getCommodityResizesList();
            resizeInfoList.forEach(resize -> {
                if (isDestinationCheck) {
                    actionCommCapacities.put(resize.getCommodityType().getType(), (double)resize.getNewCapacity());
                } else {
                    actionCommCapacities.put(resize.getCommodityType().getType(), (double)resize.getOldCapacity());
                }
            });
        }
        logger.debug("Action Capacities: {}, Provider Capacities {}", actionCommCapacities, commodityCapacities);
        return actionCommCapacities;
    }

    private boolean checkMatchForComms(final ActionInfo actionInfo, final boolean isDestinationCheck) {
        // Source or Destination commodities match current commodities, including the case of no commodities resize.
        // Some entities like VIRTUAL VOLUME may scale either tier or commodities or both.
        final Map<Integer, Double> actionCommCapacities
                = getCommInfoFromScaleAction(actionInfo, isDestinationCheck);
        return commodityCapacities.entrySet().containsAll(actionCommCapacities.entrySet());
    }

    /**
     * Match a discovered entity's provider information with information in an executed action.
     *
     * @param actionSpec The ActionSpec to compare.
     * @param isDestinationCheck true if we're checking the action destination, false if we're
     *                           checking the action source for match.
     * @return true if there's a match, false otherwise.
     */
    @Override
    public boolean matchesAction(final ActionSpec actionSpec, final boolean isDestinationCheck) {
        final ActionInfo actionInfo =  actionSpec.getRecommendation().getInfo();
        final List<ChangeProvider> changeProviders = ActionDTOUtil.getChangeProviderList(actionInfo);
        // Match tier if either the action doesn't involve a tier change or if it does,
        // then the destination or source match, as the case may be.
        final boolean tierMatches = changeProviders.isEmpty() || (isDestinationCheck
                ? changeProviders.stream().anyMatch(cp -> providerOid == cp.getDestination().getId())
                : changeProviders.stream().anyMatch(cp -> providerOid == cp.getSource().getId()));
        // Check for any commodity resizes.  Entities like Virtual Volume could change
        // both tier and commodity capacities at the same time.
        boolean commsMatch = checkMatchForComms(actionInfo,  isDestinationCheck);
        logger.debug("Action: {} Tier Match: {} Comms Match: {}", actionSpec.getRecommendation().getId(),
                tierMatches, commsMatch);
        return (tierMatches && commsMatch);
    }

    @Override
    public String toString() {
        return "BaseProviderInfo{" + "providerOid=" + providerOid + ", commodityCapacities="
                + commodityCapacities + ", entityType=" + entityType + '}';
    }
}
