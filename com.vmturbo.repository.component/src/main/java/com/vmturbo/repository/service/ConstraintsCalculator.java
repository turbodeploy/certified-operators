package com.vmturbo.repository.service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.ImmutablePaginatedResults;
import com.vmturbo.common.protobuf.PaginationProtoUtil.PaginatedResults;
import com.vmturbo.common.protobuf.action.ActionDTO.Severity;
import com.vmturbo.common.protobuf.action.EntitySeverityDTO.EntitySeveritiesResponse;
import com.vmturbo.common.protobuf.action.EntitySeverityDTO.EntitySeverity;
import com.vmturbo.common.protobuf.action.EntitySeverityDTO.MultiEntityRequest;
import com.vmturbo.common.protobuf.action.EntitySeverityServiceGrpc.EntitySeverityServiceBlockingStub;
import com.vmturbo.common.protobuf.common.Pagination.PaginationParameters;
import com.vmturbo.common.protobuf.common.Pagination.PaginationResponse;
import com.vmturbo.common.protobuf.repository.EntityConstraints.CurrentPlacement;
import com.vmturbo.common.protobuf.repository.EntityConstraints.EntityConstraint;
import com.vmturbo.common.protobuf.repository.EntityConstraints.EntityConstraintsResponse;
import com.vmturbo.common.protobuf.repository.EntityConstraints.PotentialPlacements;
import com.vmturbo.common.protobuf.repository.EntityConstraints.PotentialPlacementsResponse.MatchedEntity;
import com.vmturbo.common.protobuf.repository.EntityConstraints.RelationType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PerTargetEntityInformation;
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

    private final EntitySeverityServiceBlockingStub severityRpcService;

    private final int defaultPaginationLimit;

    private final int maxPaginationLimit;

    /**
     * The constructor.
     *
     * @param severityRpcService {@link EntityConstraintsRpcService}
     * @param defaultPaginationLimit default pagination limit
     * @param maxPaginationLimit max pagination limit
     */
    public ConstraintsCalculator(
            @Nonnull final EntitySeverityServiceBlockingStub severityRpcService,
            final int defaultPaginationLimit, final int maxPaginationLimit) {
        this.severityRpcService = severityRpcService;
        this.defaultPaginationLimit = defaultPaginationLimit;
        this.maxPaginationLimit = maxPaginationLimit;
    }

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
                        if (constraintGrouping.potentialSellersByCommodityType.containsKey(commSold.getCommodityType())) {
                            logger.trace("Adding potential seller {} for Consumer = {}, Provider Entity type = {}, Comm type bought = {}",
                                e.getDisplayName(), entity.getDisplayName(), e.getEntityType(),
                                getPrintableConstraint(commSold.getCommodityType()));
                            PotentialSellers potentialSellers = constraintGrouping.potentialSellersByCommodityType.get(commSold.getCommodityType());
                            potentialSellers.addSeller(e.getOid());
                            if (StringUtils.isEmpty((potentialSellers.scopeDisplayName))) {
                                if (commSold.hasAccesses()) {
                                    Optional<RepoGraphEntity> accessesBy = entityGraph.getEntity(commSold.getAccesses());
                                    accessesBy.ifPresent(a -> potentialSellers.scopeDisplayName = a.getDisplayName());
                                }
                            }
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
     * @param paginationParameters the pagination parameters
     * @param topologyContextId topology context id
     * @return List of potential placements
     */
    PaginatedResults<MatchedEntity> calculatePotentialPlacements(
            @Nonnull final Set<Integer> potentialEntityTypes,
            @Nonnull final Set<CommodityType> constraints,
            @Nonnull final TopologyGraph<RepoGraphEntity> entityGraph,
            @Nonnull final PaginationParameters paginationParameters,
            final long topologyContextId) {
        final long skipCount = (!paginationParameters.hasCursor() || StringUtils.isEmpty(paginationParameters.getCursor())) ?
            0 : Long.parseLong(paginationParameters.getCursor());

        final long limit;
        if (paginationParameters.hasLimit()) {
            if (paginationParameters.getLimit() > maxPaginationLimit) {
                logger.warn("Client-requested limit {} exceeds maximum!" +
                    " Lowering the limit to {}!", paginationParameters.getLimit(), maxPaginationLimit);
                limit = maxPaginationLimit;
            } else {
                limit = paginationParameters.getLimit();
            }
        } else {
            limit = defaultPaginationLimit;
        }

        final String printableConstraints = getPrintableConstraints(constraints);

        // Perform the pagination
        final Map<Long, RepoGraphEntity> potentialPlacements = entityGraph.entities()
            // Filter by entity type
            .filter(entity -> potentialEntityTypes.contains(entity.getEntityType()))
            // Filter by commodity sold type
            .filter(entity -> entity.getTopologyEntity().getCommoditySoldListList().stream()
                .map(CommoditySoldDTO::getCommodityType).collect(Collectors.toSet()).containsAll(constraints))
            // Count every record, pre-pagination to get the total record count
            .peek(entity -> {
                logger.trace("Adding potential seller {} of type {} for constraints = {}",
                    entity.getDisplayName(), entity.getEntityType(), printableConstraints);
            })
            .collect(Collectors.toMap(RepoGraphEntity::getOid, Function.identity()));

        final Iterable<EntitySeveritiesResponse> entitySeveritiesResponses =
            () -> severityRpcService.getEntitySeverities(MultiEntityRequest.newBuilder()
                .setTopologyContextId(topologyContextId)
                .addAllEntityIds(potentialPlacements.keySet())
                .build());

        // Sort by severity, then display name
        final Comparator<EntitySeverity> comparator = Comparator
            .comparing((EntitySeverity severity) ->
                severity.hasSeverity() ? severity.getSeverity() : Severity.NORMAL)
            .reversed()
            .thenComparing((EntitySeverity severity) ->
                potentialPlacements.get(severity.getEntityId()).getDisplayName());

        final List<MatchedEntity> matchedEntities =
            StreamSupport.stream(entitySeveritiesResponses.spliterator(), false)
                .flatMap(chunk -> chunk.getEntitySeverity().getEntitySeverityList().stream())
                .sorted(comparator)
                .skip(skipCount)
                // Add 1 so we know if there are more results or not.
                .limit(limit + 1)
                .map(severity -> createMatchedEntity(severity,
                    potentialPlacements.get(severity.getEntityId())))
                .collect(Collectors.toList());

        final PaginationResponse.Builder paginationResponse = PaginationResponse.newBuilder();
        paginationResponse.setTotalRecordCount(potentialPlacements.size());
        if (matchedEntities.size() > limit) {
            final String nextCursor = Long.toString(skipCount + limit);
            // Remove the last element to conform to limit boundaries.
            matchedEntities.remove(matchedEntities.size() - 1);
            paginationResponse.setNextCursor(nextCursor);
        }

        return ImmutablePaginatedResults.<MatchedEntity>builder()
            .nextPageEntities(matchedEntities)
            .paginationResponse(paginationResponse.build())
            .build();
    }

    private MatchedEntity createMatchedEntity(final EntitySeverity severity, final RepoGraphEntity repoGraphEntity) {
        final MatchedEntity.Builder builder = MatchedEntity.newBuilder().setOid(repoGraphEntity.getOid())
            .setDisplayName(repoGraphEntity.getDisplayName())
            .setEntityType(repoGraphEntity.getEntityType())
            .setEntityState(repoGraphEntity.getEntityState())
            .setSeverity(severity.hasSeverity() ? severity.getSeverity() : Severity.NORMAL);

        repoGraphEntity.getDiscoveringTargetIds().forEach(id -> {
            String vendorId = repoGraphEntity.getVendorId(id);
            PerTargetEntityInformation.Builder info = PerTargetEntityInformation.newBuilder();
            if (vendorId != null) {
                info.setVendorId(vendorId);
            }
            builder.putDiscoveredTargetData(id, info.build());
        });

        return builder.build();
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
                    constraintGrouping.potentialSellersByCommodityType.computeIfAbsent(cTypeWithKey, c -> new PotentialSellers()));
                constraintGroupingsForEntityType.add(constraintGrouping);
            }
        }
        return constraintGroupings;
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
        private final Map<CommodityType, PotentialSellers> potentialSellersByCommodityType = Maps.newHashMap();

        // Current placement of this
        private Optional<Long> currentPlacement = Optional.empty();

        public Optional<Long> getCurrentPlacement() {
            return currentPlacement;
        }

        public Map<CommodityType, PotentialSellers> getPotentialSellersByCommodityType() {
            return Collections.unmodifiableMap(potentialSellersByCommodityType);
        }
    }

    /**
     * Potential sellers along with the scope display name.
     */
    public static class PotentialSellers {
        // oids of potential placements
        private Set<Long> potentialPlacements;
        // Scope display name
        private String scopeDisplayName;

        private PotentialSellers() {
            potentialPlacements = Sets.newHashSet();
            scopeDisplayName = "";
        }

        private void addSeller(Long sellerOid) {
            potentialPlacements.add(sellerOid);
        }

        public Set<Long> getSellers() {
            return Collections.unmodifiableSet(potentialPlacements);
        }

        public String getScopeDisplayName() {
            return scopeDisplayName;
        }
    }
}
