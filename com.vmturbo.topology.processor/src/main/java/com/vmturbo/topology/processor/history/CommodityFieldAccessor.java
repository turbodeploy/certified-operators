package com.vmturbo.topology.processor.history;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Objects;
import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommodityBoughtImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommoditySoldImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommoditySoldImpl.ThresholdsImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.HistoricalValuesImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl.DiscoveryOriginView;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.UtilizationDataView;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.EntityCommodityReference;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.processor.history.percentile.PercentileEditor;

/**
 * Implementation for access to the values of commodity fields in a topology by identifiers.
 * - allow faster multiple lookups by field identifier using lazy maps
 * - synchronize updates by commodity builder
 * - different from TopologicalChangelog - only allow historical value modifications but faster
 * - make assumption that the graph itself is not being changed across the lifecycle of this instance
 *   the only changes being made are through this
 */
public class CommodityFieldAccessor implements ICommodityFieldAccessor {
    private static final Logger logger = LogManager.getLogger();

    /**
     * An entity type to its resizable commodity sold types mapping.
     */
    private static final Map<EntityType, Set<CommodityType>> entityTypeToPercentileCommSoldMap = computeResizableCommMap();

    /**
     * Initialize an entity type to its resizable commodity sold types mapping based on the commodity
     * types provided by PercentileEditor.
     *
     * @return an entity type to resizable commodity sold types mapping.
     */
    private static Map<EntityType, Set<CommodityType>> computeResizableCommMap() {
        final Map<EntityType, Set<CommodityType>> resizableCommMap = new HashMap<>();
        PercentileEditor.REQUIRED_SOLD_COMMODITY_TYPES.entrySet().stream().forEach(e -> {
            CommodityType comm = e.getKey();
            for (EntityType entityType : e.getValue()) {
                resizableCommMap.computeIfAbsent(entityType, k -> new HashSet<>()).add(comm);
            }
        });
        return resizableCommMap;
    }

    /**
     * Get the builder for a sold commodity from entity builder by field reference.
     */
    public static final BiFunction<EntityCommodityReference, TopologyEntityImpl, CommoditySoldImpl> SOLD_BUILDER_EXTRACTOR =
            (f, dto) -> dto.getCommoditySoldListImplList().stream()
            .filter(commBuilder -> commBuilder.hasCommodityType() && commBuilder
                .getCommodityType()
                .equals(f.getCommodityType()))
            .findAny().orElse(null);
    /**
     * Get the builder for a bought commodity from entity builder by field reference.
     */
    public static final BiFunction<EntityCommodityReference, TopologyEntityImpl, CommodityBoughtImpl> BOUGHT_BUILDER_EXTRACTOR =
            (f, dto) -> dto.getCommoditiesBoughtFromProvidersImplList().stream()
                .filter(fromProvider -> Objects.equal(f.getProviderOid(), fromProvider.getProviderId()))
                .findAny()
                .map(fromProvider -> fromProvider.getCommodityBoughtImplList().stream())
                .orElseGet(Stream::empty)
                .filter(commBuilder -> commBuilder.hasCommodityType() && commBuilder
                    .getCommodityType()
                    .equals(f.getCommodityType()))
                .findAny().orElse(null);

    private final TopologyGraph<TopologyEntity> graph;
    // speed up the look up of entities' commodities builders in the dtos
    // queries are multi-threaded
    private final Map<EntityCommodityReference, CommoditySoldImpl> soldBuilders = new ConcurrentHashMap<>();
    private final Map<EntityCommodityReference, CommodityBoughtImpl> boughtBuilders = new ConcurrentHashMap<>();
    private final Map<String, Integer> updateStatistics = new ConcurrentHashMap<>();

    /**
     * Construct the commodity fields accessor.
     *
     * @param graph topology graph
     */
    public CommodityFieldAccessor(@Nonnull TopologyGraph<TopologyEntity> graph) {
        this.graph = graph;
    }

    @Override
    @Nullable
    public Double getRealTimeValue(@Nonnull EntityCommodityFieldReference field) {
        if (field.getProviderOid() == null) {
            Optional<CommoditySoldImpl> soldBuilder =
                           getCommodityImpl(soldBuilders, field,
                                               SOLD_BUILDER_EXTRACTOR);
            return soldBuilder
                   .map(b -> field.getField().getSoldValue().apply(soldBuilder.get()))
                   .orElse(null);
        } else {
            Optional<CommodityBoughtImpl> boughtBuilder =
                           getCommodityImpl(boughtBuilders, field,
                                               BOUGHT_BUILDER_EXTRACTOR);
            return boughtBuilder
                   .map(b -> field.getField().getBoughtValue().apply(boughtBuilder.get()))
                   .orElse(null);
        }
    }

    @Override
    @Nullable
    public Double getCapacity(@Nonnull EntityCommodityReference commRef) {
        if (commRef.getProviderOid() == null) {
            return getCommodityImpl(soldBuilders, commRef, SOLD_BUILDER_EXTRACTOR)
                    .map(CommoditySoldImpl::getCapacity).orElse(null);
        } else {
            EntityCommodityReference providerRef = new EntityCommodityReference(commRef
                            .getProviderOid(), commRef.getCommodityType(), null);
            return getCommodityImpl(soldBuilders, providerRef, SOLD_BUILDER_EXTRACTOR)
                    .map(CommoditySoldImpl::getCapacity).orElse(null);
        }
    }

    @Override
    public UtilizationDataView getUtilizationData(@Nonnull EntityCommodityReference commRef) {
        if (commRef.getProviderOid() == null) {
            return getCommodityImpl(soldBuilders, commRef, SOLD_BUILDER_EXTRACTOR)
                    .map(builder -> builder.hasUtilizationData()
                                    ? builder.getOrCreateUtilizationData()
                                    : null)
                    .orElse(null);
        } else {
            return getCommodityImpl(boughtBuilders, commRef, BOUGHT_BUILDER_EXTRACTOR)
                    .map(builder -> builder.hasUtilizationData()
                                    ? builder.getOrCreateUtilizationData()
                                    : null)
                    .orElse(null);
        }
    }

    @Override
    public void clearUtilizationData(@Nonnull EntityCommodityReference commRef) {
        if (commRef.getProviderOid() == null) {
            getCommodityImpl(soldBuilders, commRef, SOLD_BUILDER_EXTRACTOR)
                            .map(CommoditySoldImpl::clearUtilizationData);
        } else {
            getCommodityImpl(boughtBuilders, commRef, BOUGHT_BUILDER_EXTRACTOR)
                            .map(CommodityBoughtImpl::clearUtilizationData);
        }
    }

    @Override
    public void updateHistoryValue(@Nonnull EntityCommodityFieldReference field,
                                   @Nonnull Consumer<HistoricalValuesImpl> setter,
                                   @Nonnull String description) {
        if (field.getProviderOid() == null) {
            Optional<CommoditySoldImpl> soldCommodityImpl =
                                           getCommodityImpl(soldBuilders, field,
                                                               SOLD_BUILDER_EXTRACTOR);
            applyIfPresent(soldCommodityImpl
                               .map(b -> field.getField().getSoldBuilder().apply(soldCommodityImpl.get()))
                               .orElse(null),
                           setter);
        } else {
            Optional<CommodityBoughtImpl> boughtBuilder =
                                           getCommodityImpl(boughtBuilders, field,
                                                               BOUGHT_BUILDER_EXTRACTOR);
            applyIfPresent(boughtBuilder
                               .map(b -> field.getField().getBoughtBuilder().apply(boughtBuilder.get()))
                               .orElse(null),
                           setter);
        }
        updateStatistics.compute(description, (desc, count) -> count == null ? 0 : ++count);
    }

    @Override
    public void updateThresholds(@Nonnull EntityCommodityReference commRef,
                                 @Nonnull Consumer<ThresholdsImpl> setter,
                                 @Nonnull String description) {
        if (commRef.getProviderOid() == null) {
            Optional<CommoditySoldImpl> soldBuilder =
                                           getCommodityImpl(soldBuilders, commRef,
                                                               SOLD_BUILDER_EXTRACTOR);
            applyIfPresent(soldBuilder
                               .map(b -> soldBuilder.get().getOrCreateThresholds())
                               .orElse(null),
                           setter);

            updateStatistics.compute(description, (desc, count) -> count == null ? 0 : ++count);
        } else {
            logger.error("Unable to set thresholds on bought commodity field reference {}", commRef);
        }
    }

    @Override
    public int getUpdateCount(@Nonnull String description) {
        return updateStatistics.getOrDefault(description, 0);
    }

    @Nullable
    @Override
    public Long getLastUpdatedTime(@Nonnull EntityCommodityReference commRef) {
        return graph.getEntity(commRef.getEntityOid())
            .flatMap(TopologyEntity::getDiscoveryOrigin)
            .map(DiscoveryOriginView::getLastUpdatedTime)
            .orElse(null);
    }

    private static <T> void applyIfPresent(@Nullable T arg, @Nonnull Consumer<T> valueSetter) {
        if (arg != null) {
            // adding historical fields may modify the builder's contents
            // avoid extra sync objects creation, synchronize on the builder itself
            // knowing that this is executed in a global pipeline
            synchronized (arg) {
                valueSetter.accept(arg);
            }
        }
    }

    @Nonnull
    private <T>
           Optional<T> getCommodityImpl(@Nonnull Map<EntityCommodityReference, T> buildersMap,
                       @Nonnull EntityCommodityReference field,
                       @Nonnull BiFunction<EntityCommodityReference, TopologyEntityImpl, T> builderGetter) {
        Optional<TopologyEntity> entity = graph.getEntity(field.getEntityOid());
        if (!entity.isPresent()) {
            logger.debug("Attempting to locate a missing entity: " + field);
            return Optional.empty();
        }
        T builder = buildersMap
                        .computeIfAbsent(field, ref -> builderGetter
                                        .apply(field, entity.get().getTopologyEntityImpl()));
        if (builder == null) {
            logger.debug("Attempting to locate a missing commodity: " + field);
            return Optional.empty();
        }
        return Optional.of(builder);
    }

    @Override
    public void applyInsufficientHistoricalDataPolicy(@Nonnull EntityCommodityReference commRef) {
        if (commRef.getProviderOid() == null) {
            // Disable resize on comm sold (disables resize on-prem)
            getCommodityImpl(soldBuilders, commRef, SOLD_BUILDER_EXTRACTOR)
                            .ifPresent(builder -> builder.setIsResizeable(false));
            Optional<TopologyEntity> entity = graph.getEntity(commRef.getEntityOid());
            // Disable scalable on all comm bought groupings (disables scaling actions in cloud)
            // when the expected set of commodities sold are all marked as resizable false.
            // Note that this is currently safe to do because:
            // On all entities we scale today with multiple comm bought groupings, we only actually
            //  generate scale actions on one of them, so disabling scaling on all of them is fine.
            if (!checkEntityScalability(entity)) {
                entity.ifPresent(e -> e.getTopologyEntityImpl()
                        .getCommoditiesBoughtFromProvidersImplList()
                        .forEach(commBoughtGroup -> commBoughtGroup.setScalable(false)));
            }
        }
        //TODO:implementation for BusinessUser use-case.
    }

    /**
     * Check if the given entity should disable scale on all of its CommoditiesBoughtFromProviders.
     * In particular, when the entity is a cloud entity, the scalability of CommoditiesBoughtFromProviders
     * should be strictly relying on a predefined set of commodities sold's resize ability.
     *
     * @param entity the topology entity DTO.
     * @return false if the entity's CommoditiesBoughtFromProviders has to be marked as scalable false.
     */
    private static boolean checkEntityScalability(Optional<TopologyEntity> entity) {
        if (!entity.isPresent()) {
            return true;
        }
        if (entity.get().getEnvironmentType() != EnvironmentType.CLOUD) {
            return false;
        }
        TopologyEntityImpl entityImpl = entity.get().getTopologyEntityImpl();
        // The given entity's CommoditiesBoughtFromProviders should still be scalable if any one of
        // the resizable commodity sold is resizable. For example, when a cloud VM has VCPU resizable
        // but VMEM not resizable, scale actions should still be generated based on VCPU. When a cloud
        // VM has both VCPU and VMEM not resizable, the entire entity should not generate scale actions.
        Set<CommodityType> commTypes = entityTypeToPercentileCommSoldMap.get(EntityType
                .forNumber(entityImpl.getEntityType()));
        final Set<Integer> resizeCommoditySoldSet = commTypes != null ? commTypes.stream()
                .map(ct -> ct.getNumber()).collect(Collectors.toSet()) : entityTypeToPercentileCommSoldMap
                .getOrDefault(EntityType.UNKNOWN, Sets.newHashSet()).stream().map(ct -> ct.getNumber())
                .collect(Collectors.toSet());
        return entityImpl.getCommoditySoldListList().stream().filter(cs
                    -> resizeCommoditySoldSet.contains(cs.getCommodityType().getType()))
                    .anyMatch(cs -> cs.getIsResizeable());
    }
}
