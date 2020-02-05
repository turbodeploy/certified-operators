package com.vmturbo.api.component.external.api.util.stats;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.external.api.mapper.StatsMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.api.component.external.api.service.StatsService;
import com.vmturbo.api.component.external.api.util.GroupExpander;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.statistic.EntityStatsApiDTO;
import com.vmturbo.api.dto.statistic.StatApiInputDTO;
import com.vmturbo.api.dto.statistic.StatPeriodApiInputDTO;
import com.vmturbo.api.dto.statistic.StatScopesApiInputDTO;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.api.pagination.EntityStatsPaginationRequest;
import com.vmturbo.api.pagination.EntityStatsPaginationRequest.EntityStatsPaginationResponse;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.common.protobuf.PaginationProtoUtil;
import com.vmturbo.common.protobuf.RepositoryDTOUtil;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.common.Pagination;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode;
import com.vmturbo.common.protobuf.stats.Stats.EntityStats;
import com.vmturbo.common.protobuf.stats.Stats.EntityStatsScope;
import com.vmturbo.common.protobuf.stats.Stats.EntityStatsScope.EntityGroup;
import com.vmturbo.common.protobuf.stats.Stats.EntityStatsScope.EntityGroupList;
import com.vmturbo.common.protobuf.stats.Stats.EntityStatsScope.EntityList;
import com.vmturbo.common.protobuf.stats.Stats.GetEntityStatsResponse;
import com.vmturbo.common.protobuf.stats.Stats.ProjectedEntityStatsResponse;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.common.protobuf.topology.UIEntityType;

/**
 * Supports paginated requests for stats calls.
 *
 * <p>Provides historical and projection stats data for list of entiites</p>
 */
public class PaginatedStatsExecutor {

    private final StatsMapper statsMapper;
    private final UuidMapper uuidMapper;
    private final Clock clock;
    private final RepositoryApi repositoryApi;
    private final StatsHistoryServiceBlockingStub historyRpcService;
    private final SupplyChainFetcherFactory supplyChainFetcherFactory;
    private final UserSessionContext userSessionContext;
    private static Logger logger = LogManager.getLogger(StatsService.class);
    private final GroupExpander groupExpander;

    public PaginatedStatsExecutor(@Nonnull final StatsMapper statsMapper,
            @Nonnull final UuidMapper uuidMapper,
            @Nonnull final Clock clock,
            @Nonnull final RepositoryApi repositoryApi,
            @Nonnull final StatsHistoryServiceBlockingStub historyRpcService,
            @Nonnull final SupplyChainFetcherFactory supplyChainFetcherFactory,
            @Nonnull final UserSessionContext userSessionContext,
            @Nonnull final GroupExpander groupExpander) {
        this.statsMapper = Objects.requireNonNull(statsMapper);
        this.uuidMapper = Objects.requireNonNull(uuidMapper);
        this.clock = Objects.requireNonNull(clock);
        this.repositoryApi = Objects.requireNonNull(repositoryApi);
        this.historyRpcService = Objects.requireNonNull(historyRpcService);
        this.supplyChainFetcherFactory = Objects.requireNonNull(supplyChainFetcherFactory);
        this.userSessionContext = Objects.requireNonNull(userSessionContext);
        this.groupExpander = Objects.requireNonNull(groupExpander);

    }

    public EntityStatsPaginationResponse getLiveEntityStats(@Nonnull final StatScopesApiInputDTO inputDto,
            @Nonnull final EntityStatsPaginationRequest paginationRequest)
            throws OperationFailedException {
        PaginatedStatsGather statsGatherer = new PaginatedStatsGather(inputDto, paginationRequest);
        statsGatherer.processRequest();
        return statsGatherer.getEntityStatsPaginationResponse();
    }

    @VisibleForTesting
    class PaginatedStatsGather {

        /**
         * Contains the query arguments.
         *
         * <p>like scopes, commodities, relatedType, etc</p>
         */
        private final StatScopesApiInputDTO inputDto;

        private final EntityStatsPaginationRequest paginationRequest;

        private final long clockTimeNow;

        private final Optional<StatPeriodApiInputDTO> period;

        /**
         * This variable will be set to the pagination response.
         */
        private EntityStatsPaginationResponse entityStatsPaginationResponse;

        /**
         * Map Entity types to be expanded to the RelatedEntityType to retrieve. For example,
         * replace requests for stats for a DATACENTER entity with the PHYSICAL_MACHINEs
         * in that DATACENTER.
         */
        private final Map<UIEntityType, UIEntityType> ENTITY_TYPES_TO_EXPAND = ImmutableMap.of(
                UIEntityType.DATACENTER, UIEntityType.PHYSICAL_MACHINE
        );

        /**
         * Constructs PaginatedStatsGather object.
         *
         * @param inputDto contains the query arguments; the 'scopes' property indicates a list of
         *          items to query - might be Group, Cluster, or ServiceEntity.
         * @param paginationRequest the pagination request object
         */
        PaginatedStatsGather(@Nonnull final StatScopesApiInputDTO inputDto,
                @Nonnull final EntityStatsPaginationRequest paginationRequest) {
            this.inputDto = inputDto;
            this.paginationRequest = paginationRequest;
            this.clockTimeNow = clock.millis();
            this.period = Optional.ofNullable(this.inputDto.getPeriod());
        }

        /**
         * Final processing of stats data and setting of paginationResponse.
         *
         * <p>Adds additional display info for all entites in nextStatsPage
         * and finally sets the {@link EntityStatsPaginationResponse} object</p>
         *
         * @param nextStatsPage This variable will be set to the next page of per-entity stats
         * @param paginationResponseOpt This variable will be set to the pagination response.
         */
        void getAdditionalDisplayInfoForAllEntities(List<EntityStats> nextStatsPage,
                Optional<Pagination.PaginationResponse> paginationResponseOpt) {
            final Set<Long> entitiesWithStats =
                    nextStatsPage.stream().map(EntityStats::getOid).collect(Collectors.toSet());

            final Map<Long, MinimalEntity> entities =
                    repositoryApi.entitiesRequest(entitiesWithStats).getMinimalEntities().collect(Collectors.toMap(MinimalEntity::getOid, Function
                            .identity()));

            //Combines the nextStatsPages results with minimalEntityData
            final List<EntityStatsApiDTO> dto = nextStatsPage.stream().map(entityStats -> {
                final Optional<MinimalEntity> apiDto = Optional.ofNullable(entities.get(entityStats.getOid()));
                return apiDto.map(
                        seApiDto -> constructEntityStatsDto(seApiDto, isProjectedStatsRequest(),
                                entityStats, inputDto));
            }).filter(Optional::isPresent).map(Optional::get).collect(Collectors.toList());

            entityStatsPaginationResponse = paginationResponseOpt.map(paginationResponse ->
                            PaginationProtoUtil.getNextCursor(paginationResponse)
                                    .map(nextCursor -> paginationRequest.nextPageResponse(dto, nextCursor, paginationResponse.getTotalRecordCount()))
                                    .orElseGet(() -> paginationRequest.finalPageResponse(dto, paginationResponse.getTotalRecordCount())))
                            .orElseGet(() -> paginationRequest.allResultsResponse(dto));
        }

        /**
         * Kicks off reading and processing historical or projected stats request.
         *
         * @throws OperationFailedException
         */
        void processRequest() throws OperationFailedException {
            if (isHistoricalStatsRequest()) {
                runHistoricalStatsRequest();
            } else if (isProjectedStatsRequest()) {
                runProjectedStatsRequest();
            } else {
                throw new OperationFailedException("Invalid start and end date combination.");
            }
        }

        /**
         * Determines if statsRequest is historical.
         *
         * @return true if startDate in the past or not provided
         */
        boolean isHistoricalStatsRequest() {
            return period.map(StatPeriodApiInputDTO::getStartDate)
                    .map(startDate -> DateTimeUtil.parseTime(startDate) < clockTimeNow)
                    .orElse(true);
        }

        /**
         * Determines if statsRequest for projected data.
         *
         * @return true only if endDate set and in the future
         */
        boolean isProjectedStatsRequest() {
            return period.map(StatPeriodApiInputDTO::getEndDate)
                    .map(endDate -> DateTimeUtil.parseTime(endDate) > clockTimeNow)
                    .orElse(false);
        }

        /**
         * Makes request for historical stat data.
         *
         * @throws OperationFailedException exception thrown if the scope can not be recognized
         */
        void runHistoricalStatsRequest() throws OperationFailedException {
            // For historical requests, we use the EntityStatsScope object
            // which has some optimizations to deal with global requests.
            final EntityStatsScope entityStatsScope;
            if (this.getContainsEntityGroupScope()) {
                // create stats scope which will be used to get aggregated stats for each
                // seedEntity (based on derived members)
                entityStatsScope = createEntityGroupsStatsScope();
            } else {
                entityStatsScope = createEntityStatsScope();
            }
            // fetch the historical stats for the given entities using the given search spec
            final GetEntityStatsResponse statsResponse = historyRpcService.getEntityStats(
                    statsMapper.toEntityStatsRequest(entityStatsScope, inputDto.getPeriod(),
                            paginationRequest));

            List<EntityStats> nextStatsPage = statsResponse.getEntityStatsList();
            Optional<Pagination.PaginationResponse> paginationResponseOpt =
                    statsResponse.hasPaginationResponse() ?
                            Optional.of(statsResponse.getPaginationResponse()) : Optional.empty();

            getAdditionalDisplayInfoForAllEntities(nextStatsPage, paginationResponseOpt);
        }

        /**
         * Makes request for projected stat data.
         *
         * @throws OperationFailedException
         */
        void runProjectedStatsRequest() throws OperationFailedException {
            // The projected stats service doesn't support the global entity type optimization,
            // because at the time of this writing there are no requests for projected
            // per-entity stats on the global scope, and it would require modifications
            // to the way we store projected stats.
            final EntityStatsScope entityStatsScope;
            if (this.getContainsEntityGroupScope()) {
                entityStatsScope = createEntityGroupsStatsScope();
            } else {
                entityStatsScope = EntityStatsScope.newBuilder()
                        .setEntityList(EntityList.newBuilder()
                                .addAllEntities(this.getExpandedScope(inputDto)))
                        .build();
            }
            final ProjectedEntityStatsResponse projectedStatsResponse =
                    historyRpcService.getProjectedEntityStats(statsMapper.toProjectedEntityStatsRequest(
                            entityStatsScope, inputDto.getPeriod(), paginationRequest));

            List<EntityStats> nextStatsPage = projectedStatsResponse.getEntityStatsList();
            Optional<Pagination.PaginationResponse> paginationResponseOpt = projectedStatsResponse.hasPaginationResponse() ?
                    Optional.of(projectedStatsResponse.getPaginationResponse()) : Optional.empty();

            getAdditionalDisplayInfoForAllEntities(nextStatsPage, paginationResponseOpt);
        }

        /**
         * Check if the given scope uuids contains any scope whose stats should be aggregated on a
         * group of entities, meaning derived entities from the scope, like from DC to PMs.
         *
         * @return true of the inputDto contains any scope which should be treated as aggregated
         * scope
         * @throws OperationFailedException exception thrown if the scope can not be recognized
         */
        boolean getContainsEntityGroupScope()
                throws OperationFailedException {

            // if scope is empty, then it doesn't contain aggregated scope, returning false so
            // it will create normal EntityStatsScope in the request
            final List<String> scopes = inputDto.getScopes();
            if (CollectionUtils.isEmpty(scopes)) {
                return false;
            }

            // if relatedType is a type (like DataCenter) whose stats should be aggregated on its
            // derived entities (like PMs), then it's aggregated scope, otherwise not
            final String relatedType = inputDto.getRelatedType();
            if (relatedType != null) {
                return statsMapper.shouldNormalize(relatedType);
            }

            // if relatedType is not set, it should return one aggregated stats for each scope,
            // if any of the given scope is a type of scope whose stats should be aggregated on
            // derived entities, then we should use getAggregatedEntityStats for all; if not, then
            // we should use getEntityStats for better performance
            for (String scope : scopes) {
                final ApiId apiId = uuidMapper.fromUuid(scope);
                // return one aggregated stats for the group, since relatedType is not set
                if (apiId.isGroup()) {
                    return true;
                }
                // this is not a group, but an entity (or market, which doesn't have entity type).
                // if the entity is a grouping entity, it should be handled by getAggregatedEntityStats
                Optional<Set<UIEntityType>> scopeTypes = apiId.getScopeTypes();

                if (scopeTypes.isPresent() &&
                        scopeTypes.get()
                                .stream()
                                .map(UIEntityType::apiStr)
                                .anyMatch(statsMapper::shouldNormalize)) {
                    return true;
                }
            }
            return false;
        }

        /**
         * Create a {@link EntityStatsApiDTO} that can be returned to the client, given all the
         * necessary information retrieved from history/repository.
         *
         * @param serviceEntity         The {@link ServiceEntityApiDTO} from which to get the
         *                              entity-based fields.
         * @param projectedStatsRequest Whether or not the request was for projected stats. Note -
         *                              we pass this in instead of re-determining it from the
         *                              inputDto because time will have passed from the start of the
         *                              API call to the point at which we construct {@link
         *                              EntityStatsApiDTO}s.
         * @param entityStats           The {@link EntityStats} from which to get stats values.
         * @param inputDto              The {@link StatScopesApiInputDTO} for the stats query.
         * @return A fully configured {@link EntityStatsApiDTO}.
         */
        @Nonnull
        private EntityStatsApiDTO constructEntityStatsDto(MinimalEntity serviceEntity, final boolean projectedStatsRequest,
                final EntityStats entityStats, @Nonnull final StatScopesApiInputDTO inputDto) {
            final EntityStatsApiDTO entityStatsApiDTO = new EntityStatsApiDTO();
            entityStatsApiDTO.setUuid(Long.toString(serviceEntity.getOid()));
            entityStatsApiDTO.setClassName(UIEntityType.fromType(serviceEntity.getEntityType()).apiStr());
            entityStatsApiDTO.setDisplayName(serviceEntity.getDisplayName());
            entityStatsApiDTO.setStats(new ArrayList<>());
            if (projectedStatsRequest && entityStats.getStatSnapshotsCount() > 0) {
                // we expect either zero or one snapshot for each entity
                if (entityStats.getStatSnapshotsCount() > 1) {
                    // this indicates a bug in History Component
                    logger.error("Too many entity stats ({}) for: {} -> {}; taking the first.",
                            entityStats.getStatSnapshotsCount(), entityStats.getOid(), entityStats.getStatSnapshotsList());
                }
                entityStats.getStatSnapshotsList()
                        .stream()
                        .findFirst()
                        .map(statsMapper::toStatSnapshotApiDTO)
                        .map(statApiDto -> {
                            // set the time of the snapshot to "future" using the "endDate" of the request
                            // in the
                            Optional.ofNullable(inputDto.getPeriod())
                                    .map(StatPeriodApiInputDTO::getEndDate)
                                    .ifPresent(statApiDto::setDate);
                            return statApiDto;
                        })
                        .ifPresent(statApiDto -> entityStatsApiDTO.getStats().add(statApiDto));
            } else {
                entityStatsApiDTO.getStats().addAll(statsMapper.toStatsSnapshotApiDtoList(entityStats));
            }
            return entityStatsApiDTO;
        }

        /**
         * Create list of EntityGroups and wrap it in a EntityStatsScope, which will be used in the
         * request to history component. It first tries to expand given scopes to entities of
         * related types, and then for each related entity, expands to derived entities if
         * necessary.
         *
         * @return list of aggregated entities
         * @throws OperationFailedException exception thrown if expanding scope failed
         */
        EntityStatsScope createEntityGroupsStatsScope()
                throws OperationFailedException {
            // first try to expand to related types if defined, these are the entities which
            // will be returned one by one in stats response
            final Set<Long> seedEntities = this.getExpandedScope(inputDto);

            // for each seedEntity, find derived entities which will be used to aggregate stats on
            final List<EntityGroup> entityGroups = new ArrayList<>();
            final String relatedType = inputDto.getRelatedType();
            for (Long seedEntity : seedEntities) {
                final Set<Long> derivedEntities;
                if (relatedType != null && statsMapper.shouldNormalize(relatedType)) {
                    // expand seedEntity to 'member' entities, like: from DC to PMs
                    derivedEntities = supplyChainFetcherFactory.expandScope(Sets.newHashSet(seedEntity),
                            Lists.newArrayList(statsMapper.normalizeRelatedType(relatedType)));
                } else {
                    // try to expand grouping entity, since seedEntity can be DataCenter
                    derivedEntities = supplyChainFetcherFactory.expandAggregatedEntities(Collections.singleton(seedEntity));
                }
                entityGroups.add(EntityGroup.newBuilder()
                        .setSeedEntity(seedEntity)
                        .addAllEntities(derivedEntities)
                        .build());
            }

            return EntityStatsScope.newBuilder()
                    .setEntityGroupList(EntityGroupList.newBuilder().addAllGroups(entityGroups))
                    .build();
        }

        /**
         * Get the {@link EntityStatsScope} scope that a {@link StatScopesApiInputDTO} is trying to
         * select. This includes everything implied by 'this.getExpandedScope(StatScopesApiInputDTO)'
         * as well as optimizations to handle requesting stats for all entities from the global
         * market or a global temp group.
         *
         * @return A {@link EntityStatsScope} object to use for the query.
         * @throws OperationFailedException If any part of the operation failed.
         */
        @Nonnull
        @VisibleForTesting
        protected EntityStatsScope createEntityStatsScope()
                throws OperationFailedException {
            final EntityStatsScope.Builder entityStatsScope = EntityStatsScope.newBuilder();
            final Optional<String> relatedType = Optional.ofNullable(inputDto.getRelatedType())
                    // Treat unknown type as non-existent.
                    .filter(type -> !type.equals(UIEntityType.UNKNOWN.apiStr()));
            // Market stats request must be the only uuid in the scopes list
            if (StatsService.isMarketScoped(this.inputDto)) {
                // 'relatedType' is required for full market entity stats
                if (!relatedType.isPresent()) {
                    throw new IllegalArgumentException("Cannot request individual stats for full " +
                            "Market without specifying 'relatedType'");
                }

                // if the user is scoped, set this request to the set of in-scope entities of the
                // requested 'relatedType'
                if (userSessionContext.isUserScoped()) {
                    entityStatsScope.setEntityList(EntityList.newBuilder()
                            .addAllEntities(userSessionContext.getUserAccessScope().getAccessibleOidsByEntityType(relatedType.get()))
                            .build());
                } else {
                    // Otherwise, just set the entityType field on the stats scope.
                    entityStatsScope.setEntityType(
                            UIEntityType.fromString(relatedType.get()).typeNumber());
                }
            } else if (inputDto.getScopes().size() == 1) {
                // Check if we can do the global entity type optimization.
                final Optional<Integer> globalEntityType =
                        getGlobalTempGroupEntityType(groupExpander.getGroup(inputDto.getScopes().get(0)));
                final Optional<Integer> relatedTypeInt = relatedType.map(UIEntityType::fromString).map(UIEntityType::typeNumber);
                if (globalEntityType.isPresent()
                        // We can only do the global entity type optimization if the related type
                        // is unset, or is the same as the global entity type. Why? Because the
                        // topology graph may not be connected. For example, suppose global entity type
                        // is PM and related type is VM. We can't just set the entity stats scope to
                        // VM, because there may be VMs not connected to PMs (e.g. in the cloud).
                        && (!relatedTypeInt.isPresent() || relatedTypeInt.equals(globalEntityType))) {
                    // if the user is scoped, set this request to the set of in-scope entities of the
                    // requested 'relatedType'. Note that this is different than classic, which applies
                    // the regular scope check to the global temp group, resulting in "no access".
                    // TODO: Perhaps we should do the same, but it feels like a bug.
                    if (userSessionContext.isUserScoped()) {
                        entityStatsScope.setEntityList(EntityList.newBuilder()
                                .addAllEntities(userSessionContext.getUserAccessScope()
                                        .getAccessibleOidsByEntityType(
                                                UIEntityType.fromType(globalEntityType.get()).apiStr()))
                                .build());
                    } else {
                        entityStatsScope.setEntityType(globalEntityType.get());
                    }
                } else {
                    entityStatsScope.setEntityList(
                            EntityList.newBuilder().addAllEntities(this.getExpandedScope(this.inputDto)));
                }
            } else {
                entityStatsScope.setEntityList(
                        EntityList.newBuilder().addAllEntities(this.getExpandedScope(this.inputDto)));
            }
            Preconditions.checkArgument(
                    entityStatsScope.hasEntityList() || entityStatsScope.hasEntityType());
            return entityStatsScope.build();
        }

        /**
         * Will return pagination response.
         *
         * <p>Field is set once processRequest() is run and successfully finishes</p>
         *
         * @return EntityStatsPaginationResponse
         */
        public EntityStatsPaginationResponse getEntityStatsPaginationResponse() {
            return entityStatsPaginationResponse;
        }

        /**
         * Check if group is a temporary group with global scope. And if temp group entity need to expand,
         * it should use expanded entity type instead of group entity type. If it is a temporary group
         * with global scope, we can speed up query using pre-aggregate market stats table.
         *
         * @param groupOptional a optional of group need to check.
         * @return return a optional of entity type, if input group is a temporary global scope group,
         *         otherwise return empty option.
         */
        @VisibleForTesting
        protected Optional<Integer> getGlobalTempGroupEntityType(@Nonnull final Optional<Grouping> groupOptional) {
            if (!groupOptional.isPresent() || !groupOptional.get().getDefinition().getIsTemporary()
                    || !groupOptional.get().getDefinition().hasStaticGroupMembers()
                    || groupOptional.get().getDefinition()
                    .getStaticGroupMembers().getMembersByTypeCount() != 1) {
                return Optional.empty();
            }

            final GroupDefinition tempGroup = groupOptional.get().getDefinition();
            final boolean isGlobalTempGroup = tempGroup.hasOptimizationMetadata()
                    && tempGroup.getOptimizationMetadata().getIsGlobalScope()
                    // the global scope optimization.
                    && (!tempGroup.getOptimizationMetadata().hasEnvironmentType()
                    || tempGroup.getOptimizationMetadata().getEnvironmentType()
                    == EnvironmentType.HYBRID);

            int entityType = tempGroup.getStaticGroupMembers()
                    .getMembersByType(0)
                    .getType()
                    .getEntity();

            // if it is global temp group and need to expand, should return target expand entity type.
            if (isGlobalTempGroup && ENTITY_TYPES_TO_EXPAND.containsKey(
                    UIEntityType.fromType(entityType))) {
                return Optional.of(ENTITY_TYPES_TO_EXPAND.get(
                        UIEntityType.fromType(entityType)).typeNumber());
            } else if (isGlobalTempGroup) {
                // if it is global temp group and not need to expand.
                return Optional.of(entityType);
            } else {
                return Optional.empty();
            }
        }

        /**
         * Get the scope that a {@link StatScopesApiInputDTO} is trying to select. This includes,
         * for example, expanding groups, or getting the IDs of related entities. Calling this method
         * may result in one or more remote calls to other components.
         * m
         * @param inputDto The {@link StatScopesApiInputDTO}.
         * @return A set of entity OIDs of entities in the scope.
         * @throws OperationFailedException If any part of the operation failed.
         */
        @Nonnull
        public Set<Long> getExpandedScope(@Nonnull final StatScopesApiInputDTO inputDto)
                throws OperationFailedException {
            final Set<Long> expandedUuids;
            final Optional<String> relatedType = Optional.ofNullable(inputDto.getRelatedType())
                    // Treat unknown type as non-existent.
                    .filter(type -> !type.equals(UIEntityType.UNKNOWN.apiStr()));
            // Market stats request must be the only uuid in the scopes list
            if (StatsService.isMarketScoped(inputDto)) {
                // 'relatedType' is required for full market entity stats
                if (!relatedType.isPresent()) {
                    throw new IllegalArgumentException("Cannot request individual stats for full " +
                            "Market without specifying 'relatedType'");
                }

                // if the user is scoped, we will request the subset of entity oids in the user scope
                // that match the related type.
                if (userSessionContext.isUserScoped()) {
                    expandedUuids = userSessionContext.getUserAccessScope()
                            .getAccessibleOidsByEntityType(relatedType.get())
                            .toSet();
                    logger.debug("getExpandedScope() using cached oids for scoped user. " +
                                    "found {} oids for related entity type {}",
                            expandedUuids.size(), relatedType.get());
                } else {
                    // otherwise, use a supply chain query to get them
                    final Map<String, SupplyChainNode> result = supplyChainFetcherFactory.newNodeFetcher()
                            .entityTypes(Collections.singletonList(relatedType.get()))
                            .fetch();
                    final SupplyChainNode relatedTypeNode = result.get(relatedType.get());
                    expandedUuids = relatedTypeNode == null ?
                            Collections.emptySet() : RepositoryDTOUtil.getAllMemberOids(relatedTypeNode);
                }
            } else {
                // Expand scopes list to determine the list of entity OIDs to query for this operation
                final Set<String> seedUuids = Sets.newHashSet(inputDto.getScopes());

                // This shouldn't happen, because we handle the full market case earlier on.
                Preconditions.checkArgument(UuidMapper.hasLimitedScope(seedUuids));

                // Collect related entity types: if there are any we must perform supply chain traversal
                final List<String> relatedEntityTypes = new ArrayList<>();
                relatedType.ifPresent(relatedEntityTypes::add);
                if (inputDto.getPeriod() != null && inputDto.getPeriod().getStatistics() != null) {
                    relatedEntityTypes.addAll(
                            inputDto.getPeriod().getStatistics().stream()
                                    .map(StatApiInputDTO::getRelatedEntityType)
                                    .filter(Objects::nonNull)
                                    .collect(Collectors.toList()));
                }

                // Expand groups and perform supply chain traversal with the resulting seed
                // Note that supply chain traversal will not happen if the list of related entity
                // types is empty

                Set<Long> uuids = groupExpander.expandUuids(seedUuids);
                if (uuids.isEmpty()) {
                    return Collections.emptySet();
                }
                expandedUuids =
                        performSupplyChainTraversal(uuids, relatedEntityTypes);

                // if the user is scoped, we will filter the entities according to their scope
                if (userSessionContext.isUserScoped()) {
                    return userSessionContext.getUserAccessScope().filter(expandedUuids);
                }
            }
            return expandedUuids;
        }

        /**
         * When a stats query contains related entity types and a non-empty scope, then
         * a supply chain traversal should bring all entities of the related entity types
         * that are transitively connected to the scope.  This is the role of this method.
         * If the list of related entity types is empty, then the method will return
         * without traversing the supply chain.
         *
         * @param entityIds scope of the stats query.
         * @param relatedEntityTypes a list of related entity types.
         * @return the set of all entity ids after the supply chain traversal
         * @throws OperationFailedException supply chain traversal failed
         */
        @Nonnull
        private Set<Long> performSupplyChainTraversal(
                @Nonnull Set<Long> entityIds,
                @Nonnull List<String> relatedEntityTypes
        ) throws OperationFailedException {
            return relatedEntityTypes.isEmpty() ? entityIds
                    : supplyChainFetcherFactory.expandScope(entityIds, relatedEntityTypes);
        }

    }
}
