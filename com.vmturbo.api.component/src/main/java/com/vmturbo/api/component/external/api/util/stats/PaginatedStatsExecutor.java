package com.vmturbo.api.component.external.api.util.stats;

import static java.util.stream.Collectors.groupingBy;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.external.api.mapper.PaginationMapper;
import com.vmturbo.api.component.external.api.mapper.StatsMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.api.component.external.api.service.StatsService;
import com.vmturbo.api.component.external.api.util.GroupExpander;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory;
import com.vmturbo.api.component.external.api.util.stats.query.impl.CloudCostsStatsSubQuery;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.statistic.EntityStatsApiDTO;
import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.api.dto.statistic.StatApiInputDTO;
import com.vmturbo.api.dto.statistic.StatPeriodApiInputDTO;
import com.vmturbo.api.dto.statistic.StatScopesApiInputDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.enums.Epoch;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.api.pagination.EntityStatsPaginationRequest;
import com.vmturbo.api.pagination.EntityStatsPaginationRequest.EntityStatsPaginationResponse;
import com.vmturbo.api.pagination.PaginationRequest;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.common.protobuf.PaginationProtoUtil;
import com.vmturbo.common.protobuf.RepositoryDTOUtil;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.common.Pagination;
import com.vmturbo.common.protobuf.common.Pagination.PaginationParameters;
import com.vmturbo.common.protobuf.common.Pagination.PaginationResponse;
import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatRecord;
import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatRecord.StatRecord;
import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatsQuery;
import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatsQuery.CostSourceFilter;
import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatsQuery.GroupBy;
import com.vmturbo.common.protobuf.cost.Cost.EntityTypeFilter;
import com.vmturbo.common.protobuf.cost.Cost.GetCloudCostStatsRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetCloudCostStatsResponse;
import com.vmturbo.common.protobuf.cost.CostServiceGrpc.CostServiceBlockingStub;
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
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.commons.forecasting.TimeInMillisConstants;
import com.vmturbo.components.common.pagination.EntityStatsPaginationParams;
import com.vmturbo.components.common.pagination.EntityStatsPaginationParamsFactory;
import com.vmturbo.components.common.pagination.EntityStatsPaginator;
import com.vmturbo.components.common.pagination.EntityStatsPaginator.PaginatedStats;
import com.vmturbo.components.common.pagination.EntityStatsPaginator.SortCommodityValueGetter;

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

    /**
     * To do in-memory pagination of entities.
     */
    private final EntityStatsPaginator entityStatsPaginator;

    /**
     * A factory for creating {@link EntityStatsPaginationParams}.
     */
    private final EntityStatsPaginationParamsFactory paginationParamsFactory;

    /**
     * Responsible for mapping API {@link PaginationRequest}s to {@link PaginationParameters}.
     */
    private final PaginationMapper paginationMapper;

    /**
     * Service which will provide cost information by entities.
     */
    private final CostServiceBlockingStub costServiceRpc;


    public PaginatedStatsExecutor(@Nonnull final StatsMapper statsMapper,
            @Nonnull final UuidMapper uuidMapper,
            @Nonnull final Clock clock,
            @Nonnull final RepositoryApi repositoryApi,
            @Nonnull final StatsHistoryServiceBlockingStub historyRpcService,
            @Nonnull final SupplyChainFetcherFactory supplyChainFetcherFactory,
            @Nonnull final UserSessionContext userSessionContext,
            @Nonnull final GroupExpander groupExpander,
            @Nonnull final EntityStatsPaginator entityStatsPaginator,
            @Nonnull final EntityStatsPaginationParamsFactory paginationParamsFactory,
            @Nonnull final PaginationMapper paginationMapper,
            @Nonnull final CostServiceBlockingStub costServiceRpc) {
        this.statsMapper = Objects.requireNonNull(statsMapper);
        this.uuidMapper = Objects.requireNonNull(uuidMapper);
        this.clock = Objects.requireNonNull(clock);
        this.repositoryApi = Objects.requireNonNull(repositoryApi);
        this.historyRpcService = Objects.requireNonNull(historyRpcService);
        this.supplyChainFetcherFactory = Objects.requireNonNull(supplyChainFetcherFactory);
        this.userSessionContext = Objects.requireNonNull(userSessionContext);
        this.groupExpander = Objects.requireNonNull(groupExpander);
        this.entityStatsPaginator = Objects.requireNonNull(entityStatsPaginator);
        this.paginationParamsFactory = Objects.requireNonNull(paginationParamsFactory);
        this.paginationMapper = Objects.requireNonNull(paginationMapper);
        this.costServiceRpc = Objects.requireNonNull(costServiceRpc);
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

        private final StatPeriodApiInputDTO period;

        /**
         * This variable will be set to the pagination response.
         */
        private EntityStatsPaginationResponse entityStatsPaginationResponse;

        /**
         * Map Entity types to be expanded to the RelatedEntityType to retrieve. For example,
         * replace requests for stats for a DATACENTER entity with the PHYSICAL_MACHINEs
         * in that DATACENTER.
         */
        private final Map<ApiEntityType, ApiEntityType> ENTITY_TYPES_TO_EXPAND = ImmutableMap.of(
                ApiEntityType.DATACENTER, ApiEntityType.PHYSICAL_MACHINE
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
            this.inputDto = Objects.requireNonNull(inputDto);
            this.paginationRequest = Objects.requireNonNull(paginationRequest);
            this.clockTimeNow = clock.millis();
            this.period = this.inputDto.getPeriod();
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
            final Set<Long> entitiesWithStats = nextStatsPage.stream()
                    .map(EntityStats::getOid)
                    .collect(Collectors.toSet());

            final Map<Long, MinimalEntity> entities = getMinimalEntitiesForEntityList(new HashSet<>(entitiesWithStats));

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
         */
        void processRequest() throws OperationFailedException {
            sanitizeStartDateOrEndDate();
            if (isHistoricalStatsRequest()) {
                runHistoricalStatsRequest();
            } else if (isProjectedStatsRequest()) {
                runProjectedStatsRequest();
            } else {
                throw new IllegalArgumentException("Invalid start and end date combination.");
            }
        }

        /**
         * If ONLY one of startDate/endDate is provided, adjust their values so that are
         * consistent in what they represent. (If both of them are provided or none of them is,
         * there is no need to modify anything so the function does nothing.)
         * Adjustment goes as follows:
         * - If only endDate is provided:
         * --- if end date is in the past, throw an exception since no assumption can be made for
         *      the start date; it is considered invalid input.
         * --- if end date is now* , set it to null. (When both dates are null, the most recent
         *      timestamp is returned.)
         * --- if end date is in the future, set the start date to current time
         * - If only startDate is provided:
         * --- if start date is in the past, set the end date to current time.
         * --- if start date is now* , set it to null. (When both dates are null, the most recent
         *      timestamp is returned.)
         * --- if start date is in the future, set the end date equal to start date.
         *
         * <p>*'now' is considered a small time frame of current time up to a minute ago
         */
        protected void sanitizeStartDateOrEndDate() {
            if (period == null || (period.getStartDate() == null && period.getEndDate() == null)) {
                return;
            }
            // Use a small tolerance time window: [clockTimeNowTolerance, clockTimeNow]
            // Timestamps up to 1 min before 'now' are considered 'now'
            long clockTimeNowTolerance =
                    clockTimeNow - TimeInMillisConstants.MINUTE_LENGTH_IN_MILLIS;
            if (period.getStartDate() == null) {        // in this case we have just endDate
                long inputEndTime = DateTimeUtil.parseTime(period.getEndDate());
                if (inputEndTime < clockTimeNowTolerance) {
                    // end date in the past
                    throw new IllegalArgumentException(
                            "Incorrect combination of start and end date: Start date missing & end date in the past.");
                } else if (inputEndTime <= clockTimeNow) {
                    // end date now (belongs to [clockTimeNowTolerance, clockTimeNow])
                    period.setEndDate(null);
                } else {
                    // end date in the future
                    period.setStartDate(Long.toString(clockTimeNow));
                }
            } else if (period.getEndDate() == null) {   // in this case we have just startDate
                long inputStartTime = DateTimeUtil.parseTime(period.getStartDate());
                if (inputStartTime < clockTimeNowTolerance) {
                    // start date in the past
                    period.setEndDate(Long.toString(clockTimeNow));
                } else if (inputStartTime <= clockTimeNow) {
                    // start date now (belongs to [clockTimeNowTolerance, clockTimeNow])
                    period.setStartDate(null);
                } else {
                    // start date in the future
                    period.setEndDate(Long.toString(inputStartTime));
                }
            }
            // if none of startDate, endDate is null, there is no need to modify anything
        }

        /**
         * Determines if statsRequest is historical.
         *
         * @return true if startDate in the past or not provided
         */
        boolean isHistoricalStatsRequest() {
            if (period == null || period.getStartDate() == null) {
                return true;
            }

            return DateTimeUtil.parseTime(period.getStartDate()) < clockTimeNow;
        }

        /**
         * Determines if statsRequest for projected data.
         *
         * @return true only if endDate set and in the future
         */
        boolean isProjectedStatsRequest() {
            if (period == null || period.getEndDate() == null) {
                return false;
            }
            return DateTimeUtil.parseTime(period.getEndDate()) > clockTimeNow;
        }

        /**
         * Makes call to history component get to get request stats.
         *
         * @param entityStatsScope The scope for an entity stats query.
         * @param period Describes the request for Statistics by a Time range
         * @param paginationRequest Request for data the needs paginating
         * @return response from history component
         */
        GetEntityStatsResponse requestHistoricalStats(EntityStatsScope entityStatsScope,
                StatPeriodApiInputDTO period, EntityStatsPaginationRequest paginationRequest) {
            return historyRpcService.getEntityStats(
                    statsMapper.toEntityStatsRequest(entityStatsScope, period, paginationRequest));

        }

        /**
         * Processes api request via historical component first followed by cost component.
         *
         * @throws OperationFailedException exception thrown from unexpected behavior
         */
        void runRequestThroughHistoricalComponent() throws OperationFailedException {
            // For historical requests, we use the EntityStatsScope object
            // which has some optimizations to deal with global requests.
            final EntityStatsScope entityStatsScope;
            if (this.containsEntityGroupScope()) {
                // create stats scope which will be used to get aggregated stats for each
                // seedEntity (based on derived members)
                entityStatsScope = createEntityGroupsStatsScope();
            } else {
                entityStatsScope = createEntityStatsScope();
            }

            //Process
            //  1. Call History component
            //      a. If no history results there will be no cost results
            //  2. Call Cost component use entity list response from history results
            //  3. Get Additional Entity Data list from 3.
            //  4. Map all gathered results to apiDtos
            //  5. Create the pagination response
            //  6. Set the response for access outside of class

            //1. History Stats call
            final GetEntityStatsResponse historyEntityStatResponse =
                    requestHistoricalStats(entityStatsScope, this.period, paginationRequest);

            //2. Cost Stats -  uses entities from historyStatsResponse to scope query for cost
            final List<Long> sortedList =  getSortedListFromHistoryResponse(historyEntityStatResponse);
            final Set<Long> entitiesSet = new HashSet<>(sortedList);
            final GetCloudCostStatsResponse costStatsResponse =
                    getCloudCostStatsForEntities(entitiesSet);

            //3. Entity Info call
            final Map<Long, MinimalEntity> minimalEntityMap =
                    getMinimalEntitiesForEntityList(entitiesSet);

            //4. Combine Stats
            final List<EntityStatsApiDTO> combinedResultsInOrder =
                    constructEntityStatsApiDTOFromResults(sortedList, minimalEntityMap,
                            costStatsResponse, historyEntityStatResponse);

            //5. Construct paginated response
            final EntityStatsPaginationResponse entityStatsPaginationResponse =
                    constructPaginatedResponse(historyEntityStatResponse.getPaginationResponse(),
                            combinedResultsInOrder);

            //6. Set results
            setEntityStatsPaginationResponse(entityStatsPaginationResponse);
        }

        /**
         * Iterates history stat response and returns entityUuids in expected order.
         *
         * @param historyEntityStatResponse history stats response sorted that comes sorted from
         *                                  history component
         * @return List with expected order of entities
         */
        List<Long> getSortedListFromHistoryResponse(@Nonnull GetEntityStatsResponse historyEntityStatResponse) {
            return historyEntityStatResponse.getEntityStatsList()
                    .stream()
                    .map(EntityStats::getOid)
                    .collect(Collectors.toList());
        }

        /**
         * Gets cloud cost stats from list of entityUuids.
         *
         * @param scope list of entity uuids to query
         * @return cloud cost stats response
         */
        GetCloudCostStatsResponse getCloudCostStatsForEntities(Set<Long> scope) {
            if (scope.isEmpty()) {
                return GetCloudCostStatsResponse.getDefaultInstance();
            }

            return getCloudCostStats(scope, this.inputDto);
        }

        /**
         * Makes request for projected stat data.
         *
         * @throws OperationFailedException exception thrown if expanding scope failed
         */
        void runProjectedStatsRequest() throws OperationFailedException {
            // The projected stats service doesn't support the global entity type optimization,
            // because at the time of this writing there are no requests for projected
            // per-entity stats on the global scope, and it would require modifications
            // to the way we store projected stats.
            final EntityStatsScope entityStatsScope;
            if (this.containsEntityGroupScope()) {
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
        boolean containsEntityGroupScope()
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
                Optional<Set<ApiEntityType>> scopeTypes = apiId.getScopeTypes();

                if (scopeTypes.isPresent() &&
                        scopeTypes.get()
                                .stream()
                                .map(ApiEntityType::apiStr)
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
            entityStatsApiDTO.setClassName(ApiEntityType.fromType(serviceEntity.getEntityType()).apiStr());
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
         * Gets history stats for entityUuids.
         *
         * @param entityUuids entities to query stats from history component
         * @return the {@link GetEntityStatsResponse} history stats reponse for entityUuids
         */
        GetEntityStatsResponse getHistoryComponentStatsFromCostStatsResults(final List<Long> entityUuids) {
            if (!requestContainsMoreThanCostStats()) {
                return GetEntityStatsResponse.getDefaultInstance();
            }

            return requestHistoryStatsFromCostStatsResponse(entityUuids);
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
                    .filter(type -> !type.equals(ApiEntityType.UNKNOWN.apiStr()));
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
                            ApiEntityType.fromString(relatedType.get()).typeNumber());
                }
            } else if (inputDto.getScopes().size() == 1) {
                // Check if we can do the global entity type optimization.
                final Optional<Integer> globalEntityType =
                        getGlobalTempGroupEntityType(groupExpander.getGroup(inputDto.getScopes().get(0)));
                final Optional<Integer> relatedTypeInt = relatedType.map(ApiEntityType::fromString).map(ApiEntityType::typeNumber);
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
                                                ApiEntityType.fromType(globalEntityType.get()).apiStr()))
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
         * Sets {@link EntityStatsPaginationResponse} containing class response.
         *
         * @param response the {@link EntityStatsPaginationResponse} to set class response
         */
        public void setEntityStatsPaginationResponse(EntityStatsPaginationResponse response) {
            this.entityStatsPaginationResponse = response;
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
                    ApiEntityType.fromType(entityType))) {
                return Optional.of(ENTITY_TYPES_TO_EXPAND.get(
                        ApiEntityType.fromType(entityType)).typeNumber());
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
         *
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
                    .filter(type -> !type.equals(ApiEntityType.UNKNOWN.apiStr()));
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

                // Note that supply chain traversal will not happen if the list of related entity
                // types is empty

                if (seedUuids.isEmpty()) {
                    return Collections.emptySet();
                }
                expandedUuids = performSupplyChainTraversal(
                        seedUuids.stream().map(Long::valueOf).collect(Collectors.toSet()),
                        relatedEntityTypes);

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

        /**
         * Returns true if pagination request to be sorted by cloud cost stat.
         *
         * @return true if pagination request to be sorted by cloud cost stat else false
         */
        boolean requestIsSortByCloudCostStats() {
            return paginationRequest.getOrderByStat()
                    .map(CloudCostsStatsSubQuery.COST_STATS_SET::contains)
                    .orElse(false);
        }

        /**
         * Kicks of processing historical stats query.
         *
         * @throws OperationFailedException exception thrown from unexpected behavior
         */
        void runHistoricalStatsRequest() throws OperationFailedException {
            if (requestIsSortByCloudCostStats()) {
                runRequestThroughCostComponent();
            } else {
                runRequestThroughHistoricalComponent();
            }
        }

        /**
         * Paginates cost stats data according to request sortBy.
         *
         * @param cloudCostStatsResponse the cloud results to paginate
         * @param expandedScope the scope for an entity stats query.
         * @return PaginatedStats paginated cloud cost results
         */
        PaginatedStats paginateAndSortCostResponse(
                final GetCloudCostStatsResponse cloudCostStatsResponse,
                final Set<Long> expandedScope) {
            final List<CloudCostStatRecord> cloudCostStatRecords =
                    cloudCostStatsResponse.getCloudStatRecordList();
            final int cloudCostStatRecordSize = cloudCostStatRecords.size();

            //CloudCostStatRecords Empty - create a default instance
            //CloudCostStatRecords Not Empty - take last array entry,
            //                                 results based on latest snapshot
            final CloudCostStatRecord cloudCostStatRecordToBasePaginationOn =
                    cloudCostStatRecordSize == 0 ? CloudCostStatRecord.getDefaultInstance() :
                            cloudCostStatsResponse.getCloudStatRecordList()
                                    .get(cloudCostStatRecordSize - 1);

            final Map<Long, List<StatRecord>> costStats =
                    cloudCostStatRecordToBasePaginationOn.getStatRecordsList()
                            .stream()
                            .collect(groupingBy(StatRecord::getAssociatedEntityId));


            final String orderByStat = this.paginationRequest.getOrderByStat().orElse(null);
            final SortCommodityValueGetter sortCommodityValueGetter = (entityId) -> {
                if (!costStats.containsKey(entityId)) {
                    //SortCommodityValueGetter applies a secondary sort on entity uuid if
                    //optional empty is returned.
                    return Optional.empty();
                }

                List<StatRecord> statRecordList = costStats.get(entityId);
                return statRecordList.stream()
                        .filter(statRecord -> statRecord.getName().equals(orderByStat))
                        .findFirst() // Would only ever be 1, cloud cost stats aggregated to entity level
                        .map(statRecord -> statRecord.getValues().getTotal());
            };

            final EntityStatsPaginationParams paginationParams =
                    paginationParamsFactory.newPaginationParams(
                            paginationMapper.toProtoParams(this.paginationRequest));

            return entityStatsPaginator.paginate(expandedScope, sortCommodityValueGetter,
                    paginationParams);
        }

        /**
         * Requests additional stats from history component that will append cost stats response.
         *
         * @param entityUuids entities to query in history component
         * @return            GetEntityStatsResponse, stats response from history component
         */
        GetEntityStatsResponse requestHistoryStatsFromCostStatsResponse(
                List<Long> entityUuids) {

            //History component stats can't be sorted on by costStats, create null paginationRequest
            final EntityStatsPaginationRequest paginationRequestRemovedOrderBy =
                    new EntityStatsPaginationRequest(null, null, true, null);
            EntityStatsScope historyEntityStatsScope = EntityStatsScope.newBuilder()
                    .setEntityList(EntityList.newBuilder().addAllEntities(entityUuids))
                    .build();
            return requestHistoricalStats(historyEntityStatsScope, period,
                    paginationRequestRemovedOrderBy);
        }

        /**
         * Executes request for cloud cost stats using request passed params.
         *
         * @param cloudEntityOids Entities to query
         * @param requestInputDto Multiple scope stat request.  Scoping information not
         *                        derived from this object, use cloudEntityOids for scoping query.
         * @return                {@link GetCloudCostStatsResponse} cloud cost stats response
         */
        private GetCloudCostStatsResponse getCloudCostStats(Set<Long> cloudEntityOids,
                StatScopesApiInputDTO requestInputDto) {

            final CloudCostStatsQuery.Builder costStatsBuilder = CloudCostStatsQuery.newBuilder();

            //Set the scope
             if (!cloudEntityOids.isEmpty()) {
                 costStatsBuilder.getEntityFilterBuilder().addAllEntityId(cloudEntityOids);
             }

            StatPeriodApiInputDTO statPeriod = requestInputDto.getPeriod();

            if (requestInputDto.getRelatedType() != null) {
                EntityTypeFilter entityTypeFilter = EntityTypeFilter.newBuilder()
                        .addEntityTypeId(
                                ApiEntityType.fromString(
                                        requestInputDto.getRelatedType())
                                        .typeNumber())
                        .build();
                costStatsBuilder.setEntityTypeFilter(entityTypeFilter);
            }

            //startDate and endDate
            if (statPeriod.getStartDate() != null) {
                costStatsBuilder.setStartDate(DateTimeUtil.parseTime(statPeriod.getStartDate()));
            }

            if (statPeriod.getEndDate() != null) {
                costStatsBuilder.setEndDate(DateTimeUtil.parseTime(statPeriod.getEndDate()));
            }

            //Get cost sources aggregated
            CostSourceFilter costSourceFilter = CostSourceFilter.newBuilder()
                    .setExclusionFilter(true)
                    .build();
            costStatsBuilder.setCostSourceFilter(costSourceFilter);

            //Get cost stats on entity level
            costStatsBuilder.addGroupBy(GroupBy.ENTITY);

            final GetCloudCostStatsRequest getCloudCostStatsRequest =
                    GetCloudCostStatsRequest.newBuilder()
                            .addCloudCostStatsQuery(costStatsBuilder)
                            .build();

            //Make Request
            return costServiceRpc.getCloudCostStats(getCloudCostStatsRequest);
        }

        /**
         * Queries cost component then further building additional stat requirements from history.
         *
         * <p>Will make request for cost stats first followed by a
         * call to the history component to fullfil any remaining stats requested</p>
         *
         * @throws OperationFailedException Unsupported operation when response from cost or history
         *                                  component contains stats for multiple timestamps
         */
        void runRequestThroughCostComponent() throws OperationFailedException {
            final Set<Long> expandedScope = getExpandedScope(this.inputDto);

            //Process
            //  1. Call cost component
            //  2. Paginate, sort cost results to narrow collection of entities we
            //     will use to query for additional history stats
            //      a. Cost results may not have data for all entities requested, i.e. onprem vms or
            //         cloud vms with no cloud cost stats
            //      b. We pass original expandedScope for pagination, if cost results do not have
            //         all or enough results for paginated limit page (see 2a.) we add remaining
            //         entities from original scope
            //  3. Get the paginated page entity list, this is now sorted and what we should return
            //  4. Get History stats with list from 3.
            //  5. Get Additional Entity Data list from 3.
            //  6. Map all gathered results to apiDtos
            //  7. Create the pagination response
            //  8. Set the response for access outside of class

            //1. Cost Call
            final GetCloudCostStatsResponse
                    getCloudCostStatsResponse = getCloudCostStats(expandedScope, this.inputDto);

            //2.3 Paginate and sort cost Results
            final PaginatedStats paginatedCostStats =
                    paginateAndSortCostResponse(getCloudCostStatsResponse, expandedScope);
            final List<Long> sortedNextPageEntityIds = paginatedCostStats.getNextPageIds();

            //4. History Stats call
            final GetEntityStatsResponse historyEntityStatsResponse =
                    getHistoryComponentStatsFromCostStatsResults(sortedNextPageEntityIds);

            //5. Entity Info call
            final Map<Long, MinimalEntity> minimalEntityMap =
                    getMinimalEntitiesForEntityList(new HashSet<>(sortedNextPageEntityIds));

            //6. Combine Stats
            final List<EntityStatsApiDTO> combinedResultsInOrder =
                    constructEntityStatsApiDTOFromResults(sortedNextPageEntityIds,
                            minimalEntityMap, getCloudCostStatsResponse,
                            historyEntityStatsResponse);

            //7. Construct paginated response
            final EntityStatsPaginationResponse entityStatsPaginationResponse =
                    constructPaginatedResponse(paginatedCostStats.getPaginationResponse(),
                            combinedResultsInOrder);

            //8.
            setEntityStatsPaginationResponse(entityStatsPaginationResponse);
        }


        /**
         * Constructs {@link EntityStatsPaginationResponse} with sorted results.
         *
         * @param paginationResponse paginationResponse providing nextCursor and totalRecordCount
         *                           information
         * @param sortedResults Results of paginated request
         * @return {@link EntityStatsPaginationResponse}
         */
        EntityStatsPaginationResponse constructPaginatedResponse(
                @Nullable PaginationResponse paginationResponse,
                @Nonnull List<EntityStatsApiDTO> sortedResults) {
                if (paginationResponse == null) {
                    paginationRequest.allResultsResponse(sortedResults);
                }

                return PaginationProtoUtil.getNextCursor(paginationResponse)
                        .map(nextCursor ->
                                paginationRequest.nextPageResponse(sortedResults,
                                        nextCursor, paginationResponse.getTotalRecordCount()))
                        .orElseGet(() ->
                                paginationRequest.finalPageResponse(sortedResults,
                                        paginationResponse.getTotalRecordCount()));
        }

        /**
         * Map {@link GetCloudCostStatsResponse} to entityUuuid, snapShotTime and records.
         *
         *<p>Stats with same entityUuid and snapShot will be collected into single list</p>
         *
         * @param cloudCostStatsResponse    the cloud stats to map
         * @param pagedEntities             set of entities that will be in paged response
         * @return mapped response
         */
        Map<Long, Map<Long, List<StatApiDTO>>> mapCostEntityResults(
                @Nonnull GetCloudCostStatsResponse cloudCostStatsResponse,
                @Nonnull Set<Long> pagedEntities) {
            final Map<Long, Map<Long, List<StatApiDTO>>> entityUuidMap = new HashMap<>();

            for (CloudCostStatRecord cloudCostStatRecord: cloudCostStatsResponse.getCloudStatRecordList()) {
                long snapShotDate = cloudCostStatRecord.getSnapshotDate();

                for (StatRecord statRecord: cloudCostStatRecord.getStatRecordsList()) {
                    final long entityUuid = statRecord.getAssociatedEntityId();
                    if (!pagedEntities.contains(entityUuid)) {
                        //Ignore entities that are not part of paged response
                        continue;
                    }

                    //Unroll the maps
                    final Map<Long, List<StatApiDTO>> snapShotToRecords =
                            entityUuidMap.getOrDefault(entityUuid, new HashMap<>());

                    List<StatApiDTO> statApiDTOS = snapShotToRecords.getOrDefault(snapShotDate,
                            new LinkedList<>());

                    //Add Stat to list
                    statApiDTOS.add(CloudCostsStatsSubQuery.mapStatRecordToStatApiDTO(statRecord));

                    //Roll the maps back up
                    snapShotToRecords.put(snapShotDate, statApiDTOS);
                    entityUuidMap.put(entityUuid, snapShotToRecords);
                }
            }

            return entityUuidMap;
        }

        /**
         * Maps cloud and history stat results.
         *
         * <p>Cloud and history stats will be combined into
         * a single collection according to entityUuid and snapShopDate</p>
         *
         * @param cloudCostStatsResponse    the cloud cost stats
         * @param historyStatsResponse      the history stats
         * @param pagedEntities             set of entities that will be in paged response
         * @return mapped results by entityUuid, snapShotDate with list of stat
         */
        Map<Long, Map<Long, List<StatApiDTO>>> mapAndCombineCloudAndHistoryStats(
                @Nonnull GetCloudCostStatsResponse cloudCostStatsResponse,
                @Nonnull GetEntityStatsResponse historyStatsResponse,
                @Nonnull Set<Long> pagedEntities) {
            final Map<Long, Map<Long, List<StatApiDTO>>> costEntitiesMap =
                    mapCostEntityResults(cloudCostStatsResponse, pagedEntities);

            for (EntityStats entityStats : historyStatsResponse.getEntityStatsList()) {
                final long entityUuid = entityStats.getOid();
                final Map<Long, List<StatApiDTO>> snapShotMap =
                        costEntitiesMap.getOrDefault(entityUuid, new HashMap<>());

                for (StatSnapshot statSnapshot: entityStats.getStatSnapshotsList()) {
                    final long snapShotDate = statSnapshot.getSnapshotDate();
                    final List<StatApiDTO> statApiDTOS = snapShotMap.getOrDefault(snapShotDate, new LinkedList<>());

                    final List<StatApiDTO> historyStats = statSnapshot.getStatRecordsList().stream()
                            .map(statsMapper::toStatApiDto)
                            .collect(Collectors.toList());
                    statApiDTOS.addAll(historyStats);

                    snapShotMap.put(snapShotDate, statApiDTOS);
                }

                costEntitiesMap.put(entityUuid, snapShotMap);
            }

            return costEntitiesMap;
        }

        /**
         * Create {@link EntityStatsApiDTO} from stats results.
         *
         * <p>All gathered information will be combined to create a list of
         * {@link EntityStatsApiDTO}.  This dto is a grouping of stats by entity and snapshotdate.
         * The list will be follow order of entities in sortedNextPageEntityIds
         * </p>
         *
         * @param sortedNextPageEntityIds list of entities in expected sort order
         * @param minimalEntityMap entityUuid to {@link MinimalEntity}
         * @param cloudCostStatsResponse  cloud cost stas response {@link GetCloudCostStatsResponse}
         * @param historyStatsResponse history stats response {@link GetEntityStatsResponse}
         * @return entityUuid to {@link EntityStatsApiDTO}
         *
         */
        List<EntityStatsApiDTO> constructEntityStatsApiDTOFromResults(
                @Nonnull final List<Long> sortedNextPageEntityIds,
                @Nonnull final Map<Long, MinimalEntity> minimalEntityMap,
                @Nonnull final GetCloudCostStatsResponse cloudCostStatsResponse,
                @Nonnull final GetEntityStatsResponse historyStatsResponse) {

            List<EntityStatsApiDTO> entityStatsApiDTOS =
                    Lists.newArrayListWithCapacity(sortedNextPageEntityIds.size());

            Map<Long, Map<Long, List<StatApiDTO>>> combinedCloudAndHistoryStats =
                    mapAndCombineCloudAndHistoryStats(cloudCostStatsResponse, historyStatsResponse, new HashSet<>(sortedNextPageEntityIds));

            for (long entityUuid: sortedNextPageEntityIds) {
                final EntityStatsApiDTO entityStatsApiDTO =
                        constructEntityStatsApiDTOFromMinimalEntity(entityUuid, minimalEntityMap);

                Map<Long, List<StatApiDTO>> snapShotToStats =
                        combinedCloudAndHistoryStats.getOrDefault(entityUuid, Collections.emptyMap());

                List<StatSnapshotApiDTO> statSnapshotApiDTOS = snapShotToStats.keySet()
                        .stream()
                        .map(snapShotDate -> {
                            final StatSnapshotApiDTO statSnapshotApiDTO =
                                    constructEntityStatsApiDtO(snapShotDate);
                            statSnapshotApiDTO.setStatistics(snapShotToStats.get(snapShotDate));
                            return statSnapshotApiDTO;
                        }).collect(Collectors.toList());

                entityStatsApiDTO.setStats(Lists.newArrayList(statSnapshotApiDTOS));

                entityStatsApiDTOS.add(entityStatsApiDTO);
            }

            return entityStatsApiDTOS;
        }

        /**
         * Construcst {@link StatSnapshotApiDTO} with date and derived EPOCH.
         *
         * @param date the date to set dto date value to
         * @return StatSnapshotApiDTO configured with date
         */
        StatSnapshotApiDTO constructEntityStatsApiDtO(long date) {
            final StatSnapshotApiDTO statSnapshotApiDTO = new StatSnapshotApiDTO();
            statSnapshotApiDTO.setDate(DateTimeUtil.toString(date));
            statSnapshotApiDTO.setEpoch( date <= clockTimeNow ? Epoch.HISTORICAL : Epoch.PROJECTED);
            return statSnapshotApiDTO;
        }

        /**
         * Constructs {@link EntityStatsApiDTO} from {@link MinimalEntity}.
         *
         * @param entityUuid The entityUuid of interest
         * @param minimalEntityMap Map of entityUuids to their {@link MinimalEntity}
         * @return {@link EntityStatsApiDTO}
         */
        EntityStatsApiDTO constructEntityStatsApiDTOFromMinimalEntity(Long entityUuid,
                @Nonnull final Map<Long, MinimalEntity> minimalEntityMap) {
            final EntityStatsApiDTO entityStatsApiDTO = new EntityStatsApiDTO();
            MinimalEntity minimalEntity = minimalEntityMap.get(entityUuid);
            entityStatsApiDTO.setUuid(Long.toString(minimalEntity.getOid()));
            entityStatsApiDTO.setClassName(ApiEntityType.fromType(minimalEntity.getEntityType()).apiStr());
            entityStatsApiDTO.setDisplayName(minimalEntity.getDisplayName());
            return entityStatsApiDTO;
        }

        /**
         * Returns map of entity.uuid to {@link MinimalEntity}.
         *
         * @param entityList List of entites to gather {@link MinimalEntity} and map results
         * @return Map entityUuid to {@link MinimalEntity}
         */
        Map<Long, MinimalEntity> getMinimalEntitiesForEntityList(@Nonnull Set<Long> entityList) {
            if (entityList.isEmpty()) {
                return Collections.emptyMap();
            }

            return repositoryApi.entitiesRequest(entityList)
                    .getMinimalEntities()
                    .collect(Collectors.toMap(MinimalEntity::getOid, Function.identity()));
        }

        /**
         * Checks if query request for stats other than cloud cost.
         *
         * @return returns true if stats other than cloud cost stats requested
         */
        private boolean requestContainsMoreThanCostStats() {
            if (period == null) {
                return false;
            }

            Set<String> costStatsSet = CloudCostsStatsSubQuery.COST_STATS_SET;

            return period.getStatistics().stream()
                    .anyMatch(stat -> !costStatsSet.contains(stat.getName()));
        }
    }
}
