package com.vmturbo.topology.processor.reservation;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableSet;

import org.apache.commons.lang3.mutable.MutableInt;

import gnu.trove.map.TIntIntMap;
import gnu.trove.map.hash.TIntIntHashMap;

import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.commons.analysis.AnalysisUtil;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.stitching.TopologyEntity.Builder;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.graph.TopologyGraphCreator;

/**
 * Trims a {@link TopologyGraph} to include only entities required for market analysis.
 *
 * <p/>Note: This may make it impossible to do certain scoping and policy application, so it's advisable to
 * do that first.
 */
public class ReservationTrimmer {

    /**
     * We will only keep these providers after trimming.
     */
    private static final Set<Integer> DESIRED_PROVIDER_TYPES =
        ImmutableSet.of(EntityType.PHYSICAL_MACHINE_VALUE, EntityType.STORAGE_VALUE);

    /**
     * Trim the input graph, and return a summary containing a new graph.
     *
     * @param inputGraph The input graph. Note - because {@link TopologyGraph}s are immutable, the graph
     *                   won't be modified, but the entity builders inside it could be. Once this method returns
     *                   the input graph should be considered invalid.
     * @return A {@link TrimmingSummary} containing a new graph.
     */
    @Nonnull
    public TrimmingSummary trimTopologyGraph(@Nonnull final TopologyGraph<TopologyEntity> inputGraph) {
        final TopologyGraphCreator<Builder, TopologyEntity> creator = new TopologyGraphCreator<>();
        final Set<CommodityType> requiredComms = new HashSet<>();
        final List<TopologyEntityDTO.Builder> providers = new ArrayList<>();

        final TIntIntMap entitiesCleared = new TIntIntHashMap();
        inputGraph.entities()
            .map(TopologyEntity::getTopologyEntityDtoBuilder)
            .forEach(e -> {
                if (e.getOrigin().hasReservationOrigin()) {
                    e.getCommoditiesBoughtFromProvidersList().forEach(commBoughtFromProvider -> {
                        commBoughtFromProvider.getCommodityBoughtList().forEach(commBought -> {
                            requiredComms.add(commBought.getCommodityType());
                        });
                    });
                    creator.addEntity(TopologyEntity.newBuilder(e));
                } else if (DESIRED_PROVIDER_TYPES.contains(e.getEntityType())) {
                    // We will want to retain commodities the providers sell to each other.
                    e.getCommoditiesBoughtFromProvidersList().forEach(commBoughtFromProvider -> {
                        if (DESIRED_PROVIDER_TYPES.contains(commBoughtFromProvider.getProviderEntityType())) {
                            commBoughtFromProvider.getCommodityBoughtList().forEach(commBought -> {
                                requiredComms.add(commBought.getCommodityType());
                            });
                        }
                    });
                    // We collect the relevant providers here. We will later to do additional
                    // trimming of the provider entities.
                    providers.add(e);
                } else {
                    entitiesCleared.adjustOrPutValue(e.getEntityType(), 1, 1);
                }
            });

        final TIntIntMap commoditiesCleared = new TIntIntHashMap();
        Predicate<CommodityType> commodityPredicate = commType -> {
            final boolean retain = requiredComms.contains(commType) ||
                // We need these for BiCliques even if the reservation entity isn't selling them.
                AnalysisUtil.DSPM_OR_DATASTORE.contains(commType.getType());
            if (!retain) {
                commoditiesCleared.adjustOrPutValue(commType.getType(), 1, 1);
            }
            return retain;
        };
        providers.forEach(provider -> {
            // Trim down the provider, eliminating the stuff we don't need for reservation.

            final List<CommoditiesBoughtFromProvider> newCommBought =
                new ArrayList<>(provider.getCommoditiesBoughtFromProvidersCount());
            // Eliminate bought commodities that don't match the commodity predicate.
            provider.getCommoditiesBoughtFromProvidersList().forEach(commFromProvider -> {
                final List<CommodityBoughtDTO> commsBought = commFromProvider.getCommodityBoughtList().stream()
                    .filter(commBought -> commodityPredicate.test(commBought.getCommodityType()))
                    .collect(Collectors.toList());
                if (!commsBought.isEmpty()) {
                    newCommBought.add(CommoditiesBoughtFromProvider.newBuilder()
                        .setProviderId(commFromProvider.getProviderId())
                        .setProviderEntityType(commFromProvider.getProviderEntityType())
                        .addAllCommodityBought(commsBought)
                        .build());
                }
            });
            provider.clearCommoditiesBoughtFromProviders();
            provider.addAllCommoditiesBoughtFromProviders(newCommBought);

            provider.clearTypeSpecificInfo();
            provider.clearTags();
            provider.clearEntityPropertyMap();

            // Eliminate sold commodities that don't match the commodity predicate.
            final List<CommoditySoldDTO> newCommSold = provider.getCommoditySoldListList().stream()
                .filter(commSold -> commodityPredicate.test(commSold.getCommodityType()))
                .collect(Collectors.toList());

            provider.clearCommoditySoldList();
            provider.addAllCommoditySoldList(newCommSold);
            creator.addEntity(TopologyEntity.newBuilder(provider));
        });

        return new TrimmingSummary(creator.build(), entitiesCleared, commoditiesCleared);
    }

    /**
     * Summary of the trimming, containing the new graph and information about modifications made.
     */
    public static class TrimmingSummary {
        private final TopologyGraph<TopologyEntity> newGraph;
        private final TIntIntMap entitiesCleared;
        private final TIntIntMap commoditiesCleared;

        TrimmingSummary(@Nonnull final TopologyGraph<TopologyEntity> newGraph,
                        @Nonnull final TIntIntMap entitiesCleared,
                        @Nonnull final TIntIntMap commoditiesCleared) {
            this.newGraph = newGraph;
            this.entitiesCleared = entitiesCleared;
            this.commoditiesCleared = commoditiesCleared;
        }

        @Nonnull
        public TopologyGraph<TopologyEntity> getNewGraph() {
            return newGraph;
        }

        @Override
        public String toString() {
            final StringBuilder msg = new StringBuilder();
            if (!entitiesCleared.isEmpty()) {
                MutableInt numCleared = new MutableInt(0);
                entitiesCleared.forEachEntry((type, amount) -> {
                    numCleared.add(amount);
                    msg.append("    ").append(ApiEntityType.fromType(type)).append(" : ").append(amount).append("\n");
                    return true;
                });
                msg.insert(0, "Removed " + numCleared + " providers. Type breakdown:\n");
            }

            if (!commoditiesCleared.isEmpty()) {
                final int offset = msg.length();
                MutableInt numCleared = new MutableInt(0);
                commoditiesCleared.forEachEntry((type, amount) -> {
                    numCleared.add(amount);
                    msg.append("    ").append(CommodityDTO.CommodityType.forNumber(type)).append(" : ").append(amount).append("\n");
                    return true;
                });
                msg.insert(offset, "Removed " + numCleared + " commodities. Type breakdown:\n");
            }
            return msg.toString();
        }
    }
}
