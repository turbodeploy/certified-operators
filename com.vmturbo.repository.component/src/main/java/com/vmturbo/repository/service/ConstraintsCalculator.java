package com.vmturbo.repository.service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.repository.EntityConstraints.CurrentPlacement;
import com.vmturbo.common.protobuf.repository.EntityConstraints.EntityConstraint;
import com.vmturbo.common.protobuf.repository.EntityConstraints.EntityConstraintsResponse;
import com.vmturbo.common.protobuf.repository.EntityConstraints.PotentialPlacements;
import com.vmturbo.common.protobuf.repository.EntityConstraints.RelationType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.repository.listener.realtime.RepoGraphEntity;
import com.vmturbo.topology.graph.TopologyGraph;

/**
 * This class houses the business logic for constraints calculation.
 */
public class ConstraintsCalculator {
    private final Logger logger = LogManager.getLogger();

    /**
     * Calculate the constraints.
     * @param entity entity for which constraints are to be calculated.
     * @param entityGraph the real time entity graph
     * @return the constraints.
     */
    public Map<Integer, List<ConstraintGrouping>> calculateConstraints(
        @Nonnull RepoGraphEntity entity,
        @Nonnull TopologyGraph<RepoGraphEntity> entityGraph) {
        Map<Integer, List<ConstraintGrouping>> constraintGroupings = buildEmptyConstraintGroupings(entity);

        // Pass through all the entities and if its an entity type of one of the provider types,
        // then check if it sells a commodity which the consumer buys. And if it does,
        // add it to the set of sellers for this commType
        entityGraph.entities().forEach(e -> {
            if (constraintGroupings.containsKey(e.getEntityType())) {
                List<ConstraintGrouping> constraintGroupingsForEntityType = constraintGroupings.get(e.getEntityType());
                for (CommoditySoldDTO commSold : e.getTopologyEntity().getCommoditySoldListList()) {
                    for (ConstraintGrouping constraintGrouping : constraintGroupingsForEntityType) {
                        if (constraintGrouping.sellersByCommodityType.containsKey(commSold.getCommodityType())) {
                            logger.trace("Adding potential seller {} for Consumer = {}, Provider Entity type = {}, Comm type bought = {}",
                                e.getDisplayName(), entity.getDisplayName(), e.getEntityType(),
                                getPrintableConstraint(commSold.getCommodityType()));
                            constraintGrouping.sellersByCommodityType.get(commSold.getCommodityType()).add(e.getOid());
                        }
                    }
                }
            }
        });
        return constraintGroupings;
    }

    /**
     * Calculate the potential placements for the given set of constraints and set of potential
     * entity types.
     *
     * @param potentialEntityTypes potential entity types. This will be sent in from the UI as the
     *                             current placement's entity type. But this is a set because for
     *                             ex., containers can be hosted on VMs and on PMs. In that case,
     *                             we can send in multiple potential entity types.
     * @param constraints the constraints to be considered for potential placement calculation
     * @param entityGraph real time entity graph
     * @return List of potential placements
     */
    public List<TopologyEntityDTO> calculatePotentialPlacements(
        @Nonnull final Set<Integer> potentialEntityTypes,
        @Nonnull final Set<CommodityType> constraints,
        @Nonnull final TopologyGraph<RepoGraphEntity> entityGraph) {
        String printableConstraints = getPrintableConstraints(constraints);
        // We use a List because using a hash set for a TopologyEntityDTO will mean we need to
        // hash the contents of TopologyEntityDTO, which can be expensive. And we don't expect duplicates.
        List<TopologyEntityDTO> potentialPlacements = Lists.newArrayList();
        entityGraph.entities().forEach(repoEntity -> {
            if (potentialEntityTypes.contains(repoEntity.getEntityType())) {
                TopologyEntityDTO topoEntity = repoEntity.getTopologyEntity();
                Set<CommodityType> commTypesSold = topoEntity.getCommoditySoldListList().stream()
                    .map(c -> c.getCommodityType()).collect(Collectors.toSet());
                if (commTypesSold.containsAll(constraints)) {
                    logger.trace("Adding potential seller {} of type {} for constraints = {}",
                        repoEntity.getDisplayName(), repoEntity.getEntityType(), printableConstraints);
                    potentialPlacements.add(topoEntity);
                }
            }
        });
        return potentialPlacements;
    }

    /**
     * We build the empty constraint groupings here.
     * For ex., consider a VM which has one PM leg, and 2 storage legs.
     * We will return a Map which has
     * PM Entity type number -> List of {@link ConstraintGrouping}. PM will have 1 constraint grouping.
     * Storage Entity type number -> List of {@link ConstraintGrouping}. Storage will have 2 constraint groupings.
     *
     * @param entity the entity for which constraints are sought
     * @return the empty constraint groupings
     */
    private Map<Integer, List<ConstraintGrouping>> buildEmptyConstraintGroupings(RepoGraphEntity entity) {
        Map<Integer, List<ConstraintGrouping>> constraintGroupings = Maps.newHashMap();
        for (CommoditiesBoughtFromProvider commBoughtGrouping : entity.getTopologyEntity().getCommoditiesBoughtFromProvidersList()) {
            // We are only interested in the commodities which have keys, because they
            // are the constraints. Commodities without keys are not considered constraints
            Set<CommodityType> commBoughtTypesWithKey = commBoughtGrouping.getCommodityBoughtList()
                .stream()
                .map(CommodityBoughtDTO::getCommodityType)
                .filter(CommodityType::hasKey)
                .collect(Collectors.toSet());
            if (!commBoughtTypesWithKey.isEmpty()) {
                List<ConstraintGrouping> constraintGroupingsForEntityType = constraintGroupings.computeIfAbsent(
                    commBoughtGrouping.getProviderEntityType(), entityType -> new ArrayList<>());
                ConstraintGrouping constraintGrouping = new ConstraintGrouping();
                if (commBoughtGrouping.hasProviderId()) {
                    constraintGrouping.currentPlacement = Optional.of(commBoughtGrouping.getProviderId());
                }
                commBoughtTypesWithKey.forEach(cTypeWithKey ->
                    constraintGrouping.sellersByCommodityType.computeIfAbsent(cTypeWithKey, c -> Sets.newHashSet()));
                constraintGroupingsForEntityType.add(constraintGrouping);
            }
        }
        return constraintGroupings;
    }

    /**
     * Converts the constraintGroupings to EntityConstraintsResponse.
     *
     * @param constraintGroupings constraintGroupings for the requested entity
     * @param entityGraph entity graph
     * @return EntityConstraintsResponse
     */
    private EntityConstraintsResponse buildEntityConstraintsResponse(
        @Nonnull final Map<Integer, List<ConstraintGrouping>> constraintGroupings,
        @Nonnull final TopologyGraph<RepoGraphEntity> entityGraph) {
        // Convert the constraintGroupings map to protobuf structures
        final EntityConstraintsResponse.Builder response = EntityConstraintsResponse.newBuilder();
        constraintGroupings.forEach((providerEntityType, constraintGroupingsForProviderEntityType) -> {
            constraintGroupingsForProviderEntityType.forEach(constraintGrouping -> {
                // We create an EntityConstraint for each constraintGrouping
                final EntityConstraint.Builder entityConstraint = EntityConstraint.newBuilder();
                entityConstraint.setRelationType(RelationType.BOUGHT);
                // set the current placement
                constraintGrouping.currentPlacement.ifPresent(currPlacement -> {
                    entityGraph.getEntity(currPlacement).ifPresent(curPlacementEntity -> {
                        entityConstraint.setCurrentPlacement(CurrentPlacement.newBuilder()
                            .setOid(curPlacementEntity.getOid())
                            .setDisplayName(curPlacementEntity.getDisplayName())
                            .build());
                        entityConstraint.setEntityType(curPlacementEntity.getEntityType());
                    });
                });
                // Overall potential placements should be the intersection of the
                // potential placements for all of the commodity types
                final Set<Long> overallPotentialPlacements = Sets.newHashSet();
                if (!constraintGrouping.sellersByCommodityType.isEmpty()) {
                    // Initialize the overallPotentialPlacements to the first set of sellers
                    overallPotentialPlacements.addAll(
                        constraintGrouping.sellersByCommodityType.values().iterator().next());
                }
                constraintGrouping.sellersByCommodityType.forEach((commType, sellers) -> {
                    entityConstraint.addPotentialPlacements(
                        PotentialPlacements.newBuilder()
                            .setCommodityType(commType)
                            .setNumPotentialPlacements(sellers.size())
                            .setScopeDisplayName(commType.getKey()));
                    overallPotentialPlacements.retainAll(sellers);
                });
                entityConstraint.setNumPotentialPlacements(overallPotentialPlacements.size());
                response.addEntityConstraint(entityConstraint);
            });
        });
        return response.build();
    }

    /**
     * Get the constraint in String format.
     *
     * @param constraint the constraint to print
     * @return string version of constraint
     */
    private String getPrintableConstraint(CommodityType constraint) {
        return getPrintableConstraints(ImmutableSet.of(constraint));
    }

    /**
     * Get the constraints in String format.
     *
     * @param constraints the constraints to print
     * @return string version of constraints
     */
    public String getPrintableConstraints(Set<CommodityType> constraints) {
        StringBuilder printableConstraints = new StringBuilder();
        constraints.forEach(constraint -> {
            printableConstraints
                .append(CommodityDTO.CommodityType.forNumber(constraint.getType()).name())
                .append("|")
                .append(constraint.getKey())
                .append(", ");
        });
        // Remove the trailing comma
        printableConstraints.setLength(Math.max(printableConstraints.length() - 2, 0));
        return printableConstraints.toString();
    }

    /**
     * Constraint grouping is a data structure which has the CommType bought and the providers
     * which sell it. We have one ConstraintGrouping for each {@link CommoditiesBoughtFromProvider}
     * of the {@link TopologyEntityDTO}.
     */
    public static class ConstraintGrouping {
        // A map of the commodity type bought to the set of providers which sell this commodity type.
        private final Map<CommodityType, Set<Long>> sellersByCommodityType = Maps.newHashMap();

        // Current placement of this
        private Optional<Long> currentPlacement = Optional.empty();

        public Optional<Long> getCurrentPlacement() {
            return currentPlacement;
        }

        public Map<CommodityType, Set<Long>> getSellersByCommodityType() {
            return Collections.unmodifiableMap(sellersByCommodityType);
        }
    }
}
