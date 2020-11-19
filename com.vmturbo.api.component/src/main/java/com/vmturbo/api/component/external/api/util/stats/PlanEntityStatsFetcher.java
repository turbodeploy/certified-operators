package com.vmturbo.api.component.external.api.util.stats;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.concurrent.atomic.AtomicReference;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.annotation.Nonnull;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.FormattedMessage;

import com.vmturbo.api.component.external.api.mapper.ServiceEntityMapper;
import com.vmturbo.api.component.external.api.mapper.StatsMapper;
import com.vmturbo.api.component.external.api.service.StatsService;
import com.vmturbo.api.component.external.api.util.stats.query.impl.CloudCostsStatsSubQuery;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.statistic.EntityStatsApiDTO;
import com.vmturbo.api.dto.statistic.StatApiInputDTO;
import com.vmturbo.api.dto.statistic.StatFilterApiDTO;
import com.vmturbo.api.dto.statistic.StatPeriodApiInputDTO;
import com.vmturbo.api.dto.statistic.StatScopesApiInputDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.pagination.EntityStatsPaginationRequest;
import com.vmturbo.api.pagination.EntityStatsPaginationRequest.EntityStatsPaginationResponse;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.common.protobuf.PaginationProtoUtil;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.common.Pagination.PaginationResponse;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.PlanCombinedStatsRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.PlanCombinedStatsResponse;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.PlanTopologyStatsRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.PlanTopologyStatsResponse;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.PlanTopologyStatsResponse.TypeCase;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.TopologyType;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc.RepositoryServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ApiPartialEntity;
import com.vmturbo.common.protobuf.utils.StringConstants;

/**
 * Retrieve plan entity stats from the repository.
 *
 * <p>This is a utility class shared by both the MarketsService and the StatsService.</p>
 */
public class PlanEntityStatsFetcher {

    private final Logger logger = LogManager.getLogger();

    /**
     * Some of the work to convert between Repository DTOs and API DTOs is performed directly in
     * this class, while other conversion work is offloaded to the {@link StatsMapper}.
     */
    private final StatsMapper statsMapper;

    /**
     * Used to convert plan topology entities to EntityApiDTOs.
     */
    private final ServiceEntityMapper serviceEntityMapper;

    /**
     * Used to make remote calls to the Repository, in order to fetch plan entities and stats.
     */
    private final RepositoryServiceBlockingStub repositoryRpcService;

    /**
     * Create a PlanEntityStatsFetcher, used to retrieve plan entity stats from the repository.
     *
     * @param statsMapper performs conversion between Repository DTOs and API DTOs
     * @param serviceEntityMapper used to convert plan topology entities to EntityApiDTOs
     * @param repositoryRpcService used to make remote calls to the Repository
     */
    public PlanEntityStatsFetcher(@Nonnull final StatsMapper statsMapper,
                                @Nonnull final ServiceEntityMapper serviceEntityMapper,
                                @Nonnull final RepositoryServiceBlockingStub repositoryRpcService) {

        this.statsMapper = Objects.requireNonNull(statsMapper);
        this.serviceEntityMapper = Objects.requireNonNull(serviceEntityMapper);
        this.repositoryRpcService = Objects.requireNonNull(repositoryRpcService);
    }

    /**
     * Return per-entity stats from a plan topology.
     * This is a helper method for
     * {@link StatsService#getStatsByUuidsQuery(StatScopesApiInputDTO, EntityStatsPaginationRequest)}.
     *
     * <p>If the 'inputDto.period.startDate is before "now", then the stats returned will include stats
     * for the Plan Source Topology. If the 'inputDto.period.startDate is after "now", then  the
     * stats returned will include stats for the Plan Projected Topology.</p>
     *
     * @param planInstance the plan for which to retrieve stats
     * @param inputDto a description of what stats to request from this plan, including time range,
     *                 stats types, etc
     * @param paginationRequest controls the pagination of the response
     * @return per-entity stats from a single plan topology (source or projected) or combined stats
     */
    @Nonnull
    public EntityStatsPaginationResponse getPlanEntityStats(
                @Nonnull final PlanInstance planInstance,
                @Nonnull final StatScopesApiInputDTO inputDto,
                @Nonnull final EntityStatsPaginationRequest paginationRequest) {
        final long planId = planInstance.getPlanId();
        final StatPeriodApiInputDTO period = inputDto.getPeriod();
        if (period == null) {
            final String errorMessage = "Plan stats request for plan " + planId
                + " failed to include a stat period.";
            logger.warn(errorMessage);
            throw new IllegalArgumentException(errorMessage);
        }

        // Determine whether to retrieve stats for a single topology or combined stats representing
        // both source and projected topologies.
        final long planStartTime = planInstance.getStartTime();
        final Long requestedStartDate =
            period.getStartDate() == null ? null : DateTimeUtil.parseTime(period.getStartDate());
        final Long requestedEndDate =
            period.getEndDate() == null ? null : DateTimeUtil.parseTime(period.getEndDate());
        // If the requested startDate is equal to (or earlier than) the plan start time, include
        // source data in the response.
        final boolean includeSourceData =
            requestedStartDate != null && requestedStartDate <= (planStartTime);
        // If the requested endDate is greater than the plan start time, include projected data
        // in the response.
        final boolean includeProjectedData =
            requestedEndDate != null && requestedEndDate > (planStartTime);
        final boolean includeCombinedData = includeSourceData && includeProjectedData;
        if (includeCombinedData) {
            logger.debug("Fetching plan combined stats for plan ID {}.", planId);
            return getPlanEntityCombinedStats(planId, inputDto, paginationRequest);
        } else if (includeSourceData) {
            final long sourceTopologyId = planInstance.getSourceTopologyId();
            logger.debug("Fetching plan source stats for plan ID {}, using topology {}",
                planId, sourceTopologyId);
            return getPlanEntityStats(sourceTopologyId, inputDto, paginationRequest);
        } else if (includeProjectedData) {
            final long projectedTopologyId = planInstance.getProjectedTopologyId();
            logger.debug("Fetching plan projected stats for plan ID {}, using topology {}",
                planId, projectedTopologyId);
            return getPlanEntityStats(projectedTopologyId, inputDto, paginationRequest);
        } else {
            final String errorMessage = new FormattedMessage(
                "Plan stats request included invalid time range. Plan ID: {}, "
                + "Requested start time: {}, requested end time: {}, plan start time: {}. ",
                planId, requestedStartDate, requestedEndDate, planStartTime).getFormattedMessage();
            logger.warn(errorMessage);
            throw new IllegalArgumentException(errorMessage);
        }
    }

    /**
     * Return per-entity stats from a single plan topology (source or projected).
     *
     * @param topologyId the id of a single (source or projected) plan topology for which to
     *                   retrieve stats
     * @param inputDto a description of what stats to request from this plan, including time range,
     *                 stats types, etc
     * @param paginationRequest controls the pagination of the response
     * @return per-entity stats from a single plan topology (source or projected)
     */
    @Nonnull
    private EntityStatsPaginationResponse getPlanEntityStats(final long topologyId,
                                                            // TODO: Add the appropriate epoch here
                @Nonnull final StatScopesApiInputDTO inputDto,
                @Nonnull final EntityStatsPaginationRequest paginationRequest) {

        // fetch the plan stats from the Repository client.
        final PlanTopologyStatsRequest planStatsRequest = statsMapper.toPlanTopologyStatsRequest(
            topologyId, inputDto, paginationRequest);
        final Iterable<PlanTopologyStatsResponse> response = () ->
            repositoryRpcService.getPlanTopologyStats(planStatsRequest);

        // It's important to respect the order of entities in the returned stats, because
        // they're arranged according to the pagination request.
        final List<EntityStatsApiDTO> entityStatsList = new ArrayList<>();
        final AtomicReference<Optional<PaginationResponse>> paginationResponseReference =
            new AtomicReference<>(Optional.ofNullable(null));

        // Reconstruct the streamed response, representing a single page
        StreamSupport.stream(response.spliterator(), false)
            .forEach(chunk -> {
                if (chunk.getTypeCase() == TypeCase.PAGINATION_RESPONSE) {
                    paginationResponseReference.set(Optional.of(chunk.getPaginationResponse()));
                } else {
                    chunk.getEntityStatsWrapper().getEntityStatsList().stream()
                        .forEach(entityStats -> {
                            final EntityStatsApiDTO entityStatsApiDTO = new EntityStatsApiDTO();
                            final ApiPartialEntity planEntity = entityStats.getPlanEntity().getApi();
                            final ServiceEntityApiDTO serviceEntityApiDTO =
                                serviceEntityMapper.toServiceEntityApiDTO(planEntity);
                            StatsMapper.populateEntityDataEntityStatsApiDTO(
                                    planEntity, entityStatsApiDTO);
                            entityStatsApiDTO.setRealtimeMarketReference(serviceEntityApiDTO);
                            final List<StatSnapshotApiDTO> statSnapshotsList = entityStats.getPlanEntityStats()
                                .getStatSnapshotsList()
                                .stream()
                                .map(statsMapper::toStatSnapshotApiDTO)
                                .collect(Collectors.toList());
                            entityStatsApiDTO.setStats(statSnapshotsList);
                            entityStatsList.add( entityStatsApiDTO);
                        });
                }
            });
        if (paginationResponseReference.get().isPresent()) {
            final PaginationResponse paginationResponse = paginationResponseReference.get().get();
            final int totalRecordCount = paginationResponse.getTotalRecordCount();
            return PaginationProtoUtil.getNextCursor(paginationResponse)
                .map(nextCursor -> paginationRequest.nextPageResponse(entityStatsList, nextCursor, totalRecordCount))
                .orElseGet(() -> paginationRequest.finalPageResponse(entityStatsList, totalRecordCount));
        } else {
            return paginationRequest.allResultsResponse(entityStatsList);
        }
    }

    /**
     * Get cost stats for an entity in a given topology.
     *
     * @param topologyContextId The topology for which entity cost stats should be retrieved
     * @param inputDto The {@link StatScopesApiInputDTO} specifying the scopes and period of the request
     * @param planEntityOid The entity OID for which the query is made
     * @return A collection of {@link StatSnapshotApiDTO} representing entity x topology cost stats
     */
    private List<StatSnapshotApiDTO> getCostStats(
            final long topologyContextId,
            @Nonnull final StatScopesApiInputDTO inputDto,
            final long planEntityOid) {
        final List<StatApiInputDTO> requestedStats = Objects.nonNull(inputDto.getPeriod())
                ? inputDto.getPeriod().getStatistics()
                : null;
        if (CollectionUtils.isEmpty(requestedStats)) {
            return Collections.emptyList();
        }
        final Optional<StatApiInputDTO> costStatOptional = requestedStats.stream()
                .filter(stat -> StringConstants.COST_PRICE.equals(stat.getName()))
                .findFirst();
        if (!costStatOptional.isPresent()) {
            return Collections.emptyList();
        }
        final StatApiInputDTO costStat = costStatOptional.get();
        final List<StatFilterApiDTO> filters = costStat.getFilters();
        final List<String> groupings = costStat.getGroupBy();
        return serviceEntityMapper.getCloudCostStatRecords(
                Collections.singletonList(planEntityOid),
                topologyContextId,
                CollectionUtils.isNotEmpty(filters) ? filters : Collections.emptyList(),
                CollectionUtils.isNotEmpty(groupings) ? groupings : Collections.emptyList()).stream()
                    .map(CloudCostsStatsSubQuery::toCloudStatSnapshotApiDTO)
                    .collect(Collectors.toList());
    }

    /**
     * Return per-entity combined stats representing both the source and projected plan topologies.
     *
     * <p>Specifically, each returned plan entity will contain list of {@link StatSnapshotApiDTO}s.
     * If an entity exists in both source and projected topologies, then its list of stats will be
     * of size two; otherwise the stats will be of size one. The epoch field will mark each
     * {@link StatSnapshotApiDTO} as being derived from either the plan source or plan projected
     * topology.</p>
     *
     * @param topologyContextId the context id (or plan id) for which to retrieve combined stats
     * @param inputDto a description of what stats to request from this plan, including time range,
     *                 stats types, etc
     * @param paginationRequest controls the pagination of the response
     * @return per-entity combined stats representing both the source and projected plan topologies
     */
    @Nonnull
    private EntityStatsPaginationResponse getPlanEntityCombinedStats(
            final long topologyContextId,
            @Nonnull final StatScopesApiInputDTO inputDto,
            @Nonnull final EntityStatsPaginationRequest paginationRequest) {
        // For now, we'll always sort on projected topology
        // TODO: OM-52803 Allow the API user to specify which topoloy type or epoch to sort on
        TopologyType topologyToSortOn = TopologyType.PROJECTED;
        // Build the request to Repository
        final PlanCombinedStatsRequest planCombinedStatsRequest = statsMapper
            .toPlanCombinedStatsRequest(topologyContextId, topologyToSortOn, inputDto, paginationRequest);
        // fetch the plan stats from the Repository client.
        final Iterable<PlanCombinedStatsResponse> response = () ->
            repositoryRpcService.getPlanCombinedStats(planCombinedStatsRequest);

        // It's important to respect the order of entities in the returned stats, because
        // they're arranged according to the pagination request.
        final List<EntityStatsApiDTO> entityStatsList = new ArrayList<>();
        final AtomicReference<Optional<PaginationResponse>> paginationResponseReference =
            new AtomicReference<>(Optional.ofNullable(null));

        final Set<Integer> cloudEnvTypes = new HashSet<Integer>() {{
            add(EnvironmentType.CLOUD.getNumber());
            add(EnvironmentType.HYBRID.getNumber());
        }};
        // Reconstruct the streamed response, representing a single page
        StreamSupport.stream(response.spliterator(), false)
            .forEach(chunk -> {
                if (chunk.getTypeCase() == PlanCombinedStatsResponse.TypeCase.PAGINATION_RESPONSE) {
                    paginationResponseReference.set(Optional.of(chunk.getPaginationResponse()));
                } else {
                    chunk.getEntityCombinedStatsWrapper().getEntityAndCombinedStatsList().stream()
                        .forEach(entityAndCombinedStats -> {
                            final EntityStatsApiDTO entityStatsApiDTO = new EntityStatsApiDTO();
                            // For now, we'll always include the projected entity, if present
                            // Stats for both source and projected topologies will be included though
                            // TODO: Consider to allow requesting the source entity or both
                            final ApiPartialEntity planEntity =
                                entityAndCombinedStats.hasPlanProjectedEntity()
                                ? entityAndCombinedStats.getPlanProjectedEntity().getApi()
                                : entityAndCombinedStats.getPlanSourceEntity().getApi();
                            final long planEntityOid = planEntity.getOid();
                            final ServiceEntityApiDTO serviceEntityApiDTO =
                                serviceEntityMapper.toServiceEntityApiDTO(planEntity);
                            StatsMapper.populateEntityDataEntityStatsApiDTO(
                                    planEntity, entityStatsApiDTO);
                            entityStatsApiDTO.setRealtimeMarketReference(serviceEntityApiDTO);
                            final List<StatSnapshotApiDTO> combinedStatSnapshots = entityAndCombinedStats
                                .getPlanCombinedStats()
                                .getStatSnapshotsList()
                                .stream()
                                .map(statsMapper::toStatSnapshotApiDTO)
                                .collect(Collectors.toList());
                            final EnvironmentType environmentType = planEntity.getEnvironmentType();
                            if (Objects.nonNull(environmentType)
                                    && cloudEnvTypes.contains(environmentType.getNumber())) {
                                final List<StatSnapshotApiDTO> costStats = getCostStats(
                                        topologyContextId, inputDto, planEntityOid);
                                combinedStatSnapshots.addAll(costStats);
                                combinedStatSnapshots.stream()
                                        .sorted(Comparator.comparing(StatSnapshotApiDTO::getDate))
                                        .collect(Collectors.toList());
                            }
                            entityStatsApiDTO.setStats(combinedStatSnapshots);

                            entityStatsList.add(entityStatsApiDTO);
                        });
                }
            });
        if (paginationResponseReference.get().isPresent()) {
            final PaginationResponse paginationResponse = paginationResponseReference.get().get();
            final int totalRecordCount = paginationResponse.getTotalRecordCount();
            return PaginationProtoUtil.getNextCursor(paginationResponse)
                .map(nextCursor -> paginationRequest.nextPageResponse(entityStatsList, nextCursor, totalRecordCount))
                .orElseGet(() -> paginationRequest.finalPageResponse(entityStatsList, totalRecordCount));
        } else {
            return paginationRequest.allResultsResponse(entityStatsList);
        }
    }

}
