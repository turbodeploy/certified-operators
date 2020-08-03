package com.vmturbo.market.topology.conversions;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.immutables.value.Value;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;

/**
 * An index to keep track of commodities in the {@link TopologyEntityDTO}s received by the market.
 * We often need to look up an original commodity when converting market entities to the
 * projected topology. This index allows such lookups to require no iteration.
 */
public class CommodityIndex {

    /**
     * For commodities bought, we index the scaling factors by {@link CommBoughtKey}.
     * <p>
     * Within a {@link CommBoughtKey} we use a table of (comm type, comm key) -> comm. This
     * is to enable easy lookups of all commodities of the same type.
     * <p>
     * Note - It may be a little bit less space-efficient to use the table, since it has two
     * underlying maps. If so, we can include the full {@link CommodityType} in the key,
     * and disallow lookups by base type only.
     */
    private final Map<CommBoughtKey, CommodityBoughtDTO> commBoughtIndex = new HashMap<>();

    /**
     * For commodities sold, we index the scaling factors by entity ID.
     *
     * For a particular entity ID we use a table of (comm type, comm key) -> comm. This
     * is to enable easy lookups of all commodities of the same type.
     *
     * Note - It may be a little bit less space-efficient to use the table, since it has two
     * underlying maps. If so, we can include the full {@link CommodityType} in the key,
     * and disallow lookups by base type only.
     */
    private final Map<CommSoldKey, CommoditySoldDTO> commSoldIndex = new HashMap<>();

    private static final Logger logger = LogManager.getLogger();

    /**
     * Use {@link CommodityIndex#newFactory()}.
     */
    private CommodityIndex() {}

    /**
     * Add the commodities for an entity to the index.
     *
     * @param topologyEntityDTO The {@link TopologyEntityDTO}.
     */
    public void addEntity(@Nonnull final TopologyEntityDTO topologyEntityDTO) {
        final long entityId = topologyEntityDTO.getOid();
        topologyEntityDTO.getCommoditySoldListList().forEach(commSold -> {
            addCommSold(entityId, commSold);
        });

        topologyEntityDTO.getCommoditiesBoughtFromProvidersList().forEach(commBoughtFromProvider -> {
            commBoughtFromProvider.getCommodityBoughtList().forEach(commBought -> {
                addCommBought(entityId, commBoughtFromProvider.getProviderId(),
                    commBought, commBoughtFromProvider.getVolumeId());
            });
        });
    }

    /**
     * Get a bought commodity.
     *
     * @param entityId The entity doing the buying. The {@link TopologyEntityDTO} for this entity
     *                 must have been added to the index via
     *                 {@link CommodityIndex#addEntity(TopologyEntityDTO)}.
     * @param providerId The provider for the commodity.
     * @param commType The {@link CommodityType} of the commodity being bought.
     * @param volumeId The buying entity's volume Id.
     * @return An optional of the {@link CommodityBoughtDTO}, if present.
     *         Note - the (entityId, providerId, commType) tuple uniquely identifies a
     *                {@link CommodityBoughtDTO}. We enforce that constraint when adding entities
     *                to the index.
     */
    @Nonnull
    public Optional<CommodityBoughtDTO> getCommBought(final long entityId,
                                           final long providerId,
                                           final CommodityType commType,
                                           long volumeId) {
        final CommBoughtKey commBoughtKey = constructCommBoughtKey(entityId, providerId, commType, volumeId);
        return Optional.ofNullable(commBoughtIndex.get(commBoughtKey));
    }

    /**
     * Get a sold commodity.
     *
     * @param entityId The entity doing the selling. The {@link TopologyEntityDTO} for this entity
     *                 must have been added to the index via
     *                 {@link CommodityIndex#addEntity(TopologyEntityDTO)}.
     * @param commType The {@link CommodityType} of the commodity being sold.
     * @return An optional of the {@link CommoditySoldDTO}, if present.
     *         Note - the (entityId, commType) tuple uniquely identifies a
     *                {@link CommoditySoldDTO}. We enforce that constraint when adding entities
     *                to the index.
     */
    @Nonnull
    public Optional<CommoditySoldDTO> getCommSold(final long entityId,
                                                  @Nonnull final CommodityType commType) {
        final CommSoldKey commSoldKey = ImmutableCommSoldKey.builder()
            .entityId(entityId)
            .commodityType(commType)
            .build();
        return Optional.ofNullable(commSoldIndex.get(commSoldKey));
    }

    private void addCommBought(final long entityId,
                               final long providerId,
                               @Nonnull final CommodityBoughtDTO commBought,
                               final long volumeId) {
        final CommBoughtKey commBoughtKey = constructCommBoughtKey(entityId, providerId, commBought.getCommodityType(), volumeId);
        final CommodityBoughtDTO oldCommBought = commBoughtIndex.put(commBoughtKey, commBought);
        if (oldCommBought != null) {
            final CommodityDTO.CommodityType sdkCommType = CommodityDTO.CommodityType
                    .forNumber(commBought.getCommodityType().getType());
            // This means we have an entity buying multiple commodities of the same type
            // (type + key) from a single provider. As Dana White says, that's *** illegal!
            logger.error("Entity " + entityId +
                " buying the same commodity from provider " + providerId + " more than once: " +
                sdkCommType + ". Keeping most recent one.");
        }
    }

    /**
     * Construct commBoughtKey
     *
     * @param entityId The entity ID
     * @param providerId The provider ID
     * @param commType The commodity bought type
     * @param volumeId The volume ID
     * @return @link{CommBoughtKey}
     */
    private CommBoughtKey constructCommBoughtKey(long entityId, long providerId,
                                                 CommodityType commType, long volumeId) {
        ImmutableCommBoughtKey.Builder commBoughtKeyBuilder = ImmutableCommBoughtKey.builder()
                .entityId(entityId)
                .providerId(providerId)
                .commodityType(commType);
        if (volumeId != 0) {
            // set volumeId only if it really exists
            commBoughtKeyBuilder.volumeId(volumeId);
        }
        return commBoughtKeyBuilder.build();
    }

    private void addCommSold(final long entityId,
                             final CommoditySoldDTO commSold) {
        final CommSoldKey commSoldKey = ImmutableCommSoldKey.builder()
            .entityId(entityId)
            .commodityType(commSold.getCommodityType())
            .build();
        final CommoditySoldDTO oldCommSold = commSoldIndex.put(commSoldKey, commSold);
        if (oldCommSold != null) {
            final CommodityDTO.CommodityType sdkCommType = CommodityDTO.CommodityType
                    .forNumber(commSold.getCommodityType().getType());
            // This means we have an entity selling multiple commodities of the same type
            // (type + key). As Dana White says, that's *** illegal!
            logger.error("Entity " + entityId +
                " selling same commodity more than once: " +
                sdkCommType + ". Keeping most recent one.");
        }
    }

    /**
     * Key for bought commodities.
     */
    @Value.Immutable
    interface CommBoughtKey {
        /**
         * The ID of the entity doing the buying.
         */
        long entityId();

        /**
         * The ID of the seller entity providing the commodity
         */
        long providerId();

        /**
         * The volume ID of the buying entity
         */
        Optional<Long> volumeId();

        /**
         * The type and key of the commodity being sold.
         */
        CommodityType commodityType();
    }

    @Value.Immutable
    interface CommSoldKey {
        long entityId();

        /**
         * The type and key of the commodity being sold.
         */
        CommodityType commodityType();
    }

    /**
     * A factory class for creating {@link CommodityIndex} objects.
     *
     * Useful for unit testing when we want to inject a mock/fake/pre-populated index
     * into a class that uses it internally.
     */
    @FunctionalInterface
    public interface CommodityIndexFactory {

        CommodityIndex newIndex();

    }

    /**
     * Get a new instance of {@link CommodityIndexFactory} that returns a real
     * {@link CommodityIndex}.
     */
    @Nonnull
    public static CommodityIndexFactory newFactory() {
        return CommodityIndex::new;
    }

}
