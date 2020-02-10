package com.vmturbo.api.component.external.api.util.stats.query.impl;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;

import org.apache.commons.collections4.CollectionUtils;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.api.component.external.api.util.BuyRiScopeHandler;
import com.vmturbo.api.component.external.api.util.StatsUtils;
import com.vmturbo.api.component.external.api.util.stats.StatsQueryContextFactory.StatsQueryContext;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.statistic.StatApiInputDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.common.protobuf.cost.Cost.AccountFilter;
import com.vmturbo.common.protobuf.cost.Cost.AvailabilityZoneFilter;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceBoughtCountByTemplateResponse;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceBoughtCountRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceCostStatsRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceCostStatsRequest.GroupBy;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceCostStatsResponse;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceCoverageStatsRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceUtilizationStatsRequest;
import com.vmturbo.common.protobuf.cost.Cost.RegionFilter;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceCostStat;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceStatsRecord;
import com.vmturbo.common.protobuf.cost.Cost.StatsRequestTimeWindow;
import com.vmturbo.common.protobuf.cost.ReservedInstanceBoughtServiceGrpc.ReservedInstanceBoughtServiceBlockingStub;
import com.vmturbo.common.protobuf.cost.ReservedInstanceCostServiceGrpc.ReservedInstanceCostServiceBlockingStub;
import com.vmturbo.common.protobuf.cost.ReservedInstanceUtilizationCoverageServiceGrpc.ReservedInstanceUtilizationCoverageServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.components.common.utils.StringConstants;

/**
 * Sub-query responsible for getting reserved instance stats from the cost component.
 */
public class RIStatsSubQuery extends AbstractRIStatsSubQuery {
    private final ReservedInstanceUtilizationCoverageServiceBlockingStub riUtilizationCoverageService;
    private final ReservedInstanceBoughtServiceBlockingStub riBoughtService;

    private final RIStatsMapper riStatsMapper;
    private final ReservedInstanceCostServiceBlockingStub reservedInstanceCostService;
    private final UserSessionContext userSessionContext;
    private final Set<UIEntityType> validEntityTypesForRIStats = new HashSet<>();

    public RIStatsSubQuery(@Nonnull final ReservedInstanceUtilizationCoverageServiceBlockingStub riUtilizationCoverageService,
                           @Nonnull final ReservedInstanceBoughtServiceBlockingStub riBoughtService,
                           @Nonnull final RepositoryApi repositoryApi,
                           @Nonnull final ReservedInstanceCostServiceBlockingStub reservedInstanceCostService,
                           @Nonnull BuyRiScopeHandler buyRiScopeHandler,
                           @Nonnull UserSessionContext userSessionContext) {
        this(riUtilizationCoverageService, riBoughtService, new RIStatsMapper(buyRiScopeHandler),
                repositoryApi, reservedInstanceCostService, buyRiScopeHandler, userSessionContext);
    }

    RIStatsSubQuery(@Nonnull final ReservedInstanceUtilizationCoverageServiceBlockingStub riUtilizationCoverageService,
                    @Nonnull final ReservedInstanceBoughtServiceBlockingStub riBoughtService,
                    @Nonnull final RIStatsMapper riStatsMapper,
                    @Nonnull final RepositoryApi repositoryApi,
                    @Nonnull final ReservedInstanceCostServiceBlockingStub reservedInstanceCostService,
                    @Nonnull BuyRiScopeHandler buyRiScopeHandler,
                    @Nonnull UserSessionContext userSessionContext) {
        super(repositoryApi, buyRiScopeHandler);
        this.riUtilizationCoverageService = riUtilizationCoverageService;
        this.riBoughtService = riBoughtService;
        this.riStatsMapper = riStatsMapper;
        this.reservedInstanceCostService = reservedInstanceCostService;
        this.userSessionContext = userSessionContext;
        // related entity types that we support RI stats for
        validEntityTypesForRIStats.add(UIEntityType.REGION);
        validEntityTypesForRIStats.add(UIEntityType.AVAILABILITY_ZONE);
        validEntityTypesForRIStats.add(UIEntityType.BUSINESS_ACCOUNT);
        validEntityTypesForRIStats.add(UIEntityType.VIRTUAL_MACHINE);
    }

    @Override
    public boolean applicableInContext(@Nonnull final StatsQueryContext context) {
        // plans should use separate sub query
        if (context.getInputScope().isPlan()) {
            return false;
        }
        UIEntityType relatedEntityType;
        if (context.getQueryScope().getGlobalScope().isPresent() &&  !context.getQueryScope().getGlobalScope().get().entityTypes().isEmpty()) {
            relatedEntityType = context.getQueryScope().getGlobalScope().get().entityTypes().iterator().next();
            return validEntityTypesForRIStats.contains(relatedEntityType);
        } else {
            return true;
        }
    }

    @Nonnull
    @Override
    public List<StatSnapshotApiDTO> getAggregateStats(@Nonnull final Set<StatApiInputDTO> stats,
                                                      @Nonnull final StatsQueryContext context)
            throws OperationFailedException {
        final List<StatSnapshotApiDTO> snapshots = new ArrayList<>();

        if (StatsUtils.isValidScopeForRIBoughtQuery(context.getInputScope()) &&
                containsStat(StringConstants.NUM_RI, stats)) {
            final GetReservedInstanceBoughtCountRequest countRequest = riStatsMapper
                    .createRIBoughtCountRequest(context);

            final Map<String, Long> riBoughtCountByTierName = getRIBoughtCountByTierName(countRequest);
            snapshots.addAll(riStatsMapper
                            .convertNumRIStatsRecordsToStatSnapshotApiDTO(riBoughtCountByTierName));
        }

        if (StatsUtils.isValidScopeForRIBoughtQuery(context.getInputScope()) &&
                containsStat(StringConstants.RI_COST, stats)) {
            final GetReservedInstanceCostStatsRequest riCostRequest = riStatsMapper
                    .createRICostRequest(context);
            final GetReservedInstanceCostStatsResponse response = reservedInstanceCostService
                    .getReservedInstanceCostStats(riCostRequest);
            snapshots.addAll(riStatsMapper.createRICostStatApiDTO(response.getStatsList()));
        }

        if (isValidScopeForCoverageRequest(context, userSessionContext) &&
                containsStat(StringConstants.RI_COUPON_COVERAGE, stats)) {
            final GetReservedInstanceCoverageStatsRequest coverageRequest =
                    riStatsMapper.createCoverageRequest(context);
            snapshots.addAll(riStatsMapper.convertRIStatsRecordsToStatSnapshotApiDTO(
                    riUtilizationCoverageService.getReservedInstanceCoverageStats(coverageRequest)
                            .getReservedInstanceStatsRecordsList(), true));
        }

        if (StatsUtils.isValidScopeForRIBoughtQuery(context.getInputScope()) &&
                containsStat(StringConstants.RI_COUPON_UTILIZATION, stats)) {
            final GetReservedInstanceUtilizationStatsRequest utilizationRequest =
                    riStatsMapper.createUtilizationRequest(context);
            snapshots.addAll(riStatsMapper.convertRIStatsRecordsToStatSnapshotApiDTO(
                    riUtilizationCoverageService.getReservedInstanceUtilizationStats(utilizationRequest)
                            .getReservedInstanceStatsRecordsList(), false));
        }

        return mergeStatsByDate(snapshots);
    }

    private Map<String, Long> getRIBoughtCountByTierName(GetReservedInstanceBoughtCountRequest countRequest) {
        final GetReservedInstanceBoughtCountByTemplateResponse response =
                        riBoughtService.getReservedInstanceBoughtCountByTemplateType(countRequest).toBuilder().build();
        final Map<Long, Long> riBoughtCountsByTierId =
                        response.getReservedInstanceCountMapMap();
        final Map<Long, ServiceEntityApiDTO> tierApiDTOByTierId =
                        getRepositoryApi().entitiesRequest(riBoughtCountsByTierId.keySet())
                                        .getSEMap();
        return riBoughtCountsByTierId
                        .entrySet().stream()
                        .filter(e -> tierApiDTOByTierId.containsKey(e.getKey()))
                        .collect(Collectors
                                        .toMap(e -> tierApiDTOByTierId.get(e.getKey()).getDisplayName(),
                                                        Entry::getValue, Long::sum));
    }

    /**
     * Check if valid scope for RI Coverage request.
     *
     * @param context            - query context
     * @param userSessionContext to check if user is an observer.
     * @return boolean
     */
    private static boolean isValidScopeForCoverageRequest(@Nonnull final StatsQueryContext context,
                                                          @Nonnull final UserSessionContext userSessionContext) {
        //only allow non-observer users.
        if (userSessionContext.isUserObserver()) {
            return false;
        }
        if (context.getInputScope().getScopeTypes().isPresent()
                && !context.getInputScope().getScopeTypes().get().isEmpty()) {
            return context.getInputScope().getScopeTypes().get().size() == 1;
        } else {
            return context.isGlobalScope();
        }
    }

    @VisibleForTesting
    static class RIStatsMapper {
        private BuyRiScopeHandler buyRiScopeHandler;

        public RIStatsMapper(BuyRiScopeHandler buyRiScopeHandler) {
            this.buyRiScopeHandler = buyRiScopeHandler;
        }

        @Nonnull
        GetReservedInstanceBoughtCountRequest createRIBoughtCountRequest(
                @Nonnull final StatsQueryContext context) throws OperationFailedException {
            final GetReservedInstanceBoughtCountRequest.Builder reqBuilder =
                    GetReservedInstanceBoughtCountRequest.newBuilder();
            if (context.getInputScope().getScopeTypes().isPresent()) {
                final Set<Long> scopeEntities = new HashSet<>();
                if (context.getInputScope().isGroup()) {
                    if (context.getInputScope().getCachedGroupInfo().isPresent()) {
                        scopeEntities.addAll(context.getInputScope().getCachedGroupInfo().get().getEntityIds());
                    }
                } else {
                    scopeEntities.add(context.getInputScope().oid());
                }
                final Set<UIEntityType> uiEntityTypes = context.getInputScope().getScopeTypes().get();

                if (CollectionUtils.isEmpty(uiEntityTypes)) {
                    throw new OperationFailedException("Entity type not present");
                }
                final UIEntityType type = uiEntityTypes.stream().findFirst().get();

                switch (type) {
                    case REGION:
                        reqBuilder.setRegionFilter(RegionFilter.newBuilder()
                                .addAllRegionId(scopeEntities));
                        break;
                    case AVAILABILITY_ZONE:
                        reqBuilder.setAvailabilityZoneFilter(AvailabilityZoneFilter.newBuilder()
                                .addAllAvailabilityZoneId(scopeEntities));
                        break;
                    case BUSINESS_ACCOUNT:
                        reqBuilder.setAccountFilter(AccountFilter.newBuilder()
                                .addAllAccountId(scopeEntities));
                        break;
                    default:
                        throw new OperationFailedException("Invalid scope for query: " + type.apiStr());
                }
            } else if (!context.isGlobalScope()) {
                throw new OperationFailedException("Invalid scope for query." +
                        " Must be global or have an entity type.");
            }
            return reqBuilder.build();
        }

        @Nonnull
        GetReservedInstanceUtilizationStatsRequest createUtilizationRequest(@Nonnull final StatsQueryContext context)
                throws OperationFailedException {

            final ApiId inputScope = context.getInputScope();
            final GetReservedInstanceUtilizationStatsRequest.Builder reqBuilder =
                    GetReservedInstanceUtilizationStatsRequest.newBuilder()
                            .setIncludeBuyRiUtilization(
                                    buyRiScopeHandler.shouldIncludeBuyRiDiscount(inputScope));
            return internalCreateUtilizationRequest(context, reqBuilder, inputScope);
        }

        @Nonnull
        GetReservedInstanceCoverageStatsRequest createCoverageRequest(
                @Nonnull final StatsQueryContext context) throws OperationFailedException {

            final ApiId inputScope = context.getInputScope();
            final GetReservedInstanceCoverageStatsRequest.Builder reqBuilder =
                    GetReservedInstanceCoverageStatsRequest.newBuilder()
                            .setIncludeBuyRiCoverage(
                                    buyRiScopeHandler.shouldIncludeBuyRiDiscount(inputScope));

            return internalCreateCoverageRequest(context, reqBuilder, inputScope);
        }

        @Nonnull
        GetReservedInstanceCostStatsRequest createRICostRequest(
                @Nonnull final StatsQueryContext context) throws OperationFailedException {
            final GetReservedInstanceCostStatsRequest.Builder reqBuilder
                    = GetReservedInstanceCostStatsRequest.newBuilder();

            if (context.getInputScope().getScopeTypes().isPresent()
                    && !context.getInputScope().getScopeTypes().get().isEmpty()) {

                final Set<Long> scopeEntities = new HashSet<>();
                if (context.getInputScope().isGroup()) {
                    if (context.getInputScope().getCachedGroupInfo().isPresent()) {
                        scopeEntities.addAll(context.getInputScope().getCachedGroupInfo().get().getEntityIds());
                    }
                } else {
                    scopeEntities.add(context.getInputScope().oid());
                }

                if (context.getInputScope().getScopeTypes().get().size() != 1) {
                    //TODO (mahdi) Change the logic to support scopes with more than one type
                    throw new IllegalStateException("Scopes with more than one type is not supported.");
                }

                final Set<UIEntityType> uiEntityTypes = context.getInputScope().getScopeTypes().get();
                if (CollectionUtils.isEmpty(uiEntityTypes)) {
                    throw new OperationFailedException("Entity type not present");
                }
                final UIEntityType type = uiEntityTypes.stream().findFirst().get();
                switch (type) {
                    case REGION:
                        reqBuilder.setRegionFilter(RegionFilter.newBuilder()
                                .addAllRegionId(scopeEntities));
                        break;
                    case AVAILABILITY_ZONE:
                        reqBuilder.setAvailabilityZoneFilter(AvailabilityZoneFilter.newBuilder()
                                .addAllAvailabilityZoneId(scopeEntities));
                        break;
                    case BUSINESS_ACCOUNT:
                        reqBuilder.setAccountFilter(AccountFilter.newBuilder()
                                .addAllAccountId(scopeEntities));
                        break;
                    default:
                        throw new OperationFailedException("Invalid scope for query: " + type.apiStr());
                }
            } else if (!context.isGlobalScope()) {
                throw new OperationFailedException("Invalid context - must be global or have entity type");
            }

            final boolean shouldIncludeBuyRI = buyRiScopeHandler
                    .shouldIncludeBuyRiDiscount(context.getInputScope());

            reqBuilder.setTimeWindow(StatsRequestTimeWindow.newBuilder().setQueryLatest(true).build());

            return reqBuilder.setIncludeProjected(true)
                    .setGroupBy(GroupBy.SNAPSHOT_TIME)
                    .setIncludeBuyRi(shouldIncludeBuyRI)
                    .setTopologyContextId(context.getInputScope().getTopologyContextId())
                    .build();
        }

        /**
         * Convert numRI records to StatSnapshotApiDTO
         *
         * @param records - map containing template types and counts from users RI inventory
         * @return a list {@link StatSnapshotApiDTO}
         */
        public List<StatSnapshotApiDTO> convertNumRIStatsRecordsToStatSnapshotApiDTO(
                @Nonnull final Map<String, Long> records) {
            return convertNumRIStatsMapToStatSnapshotApiDTO(records);
        }

        /**
         * Convert a list of {@link ReservedInstanceStatsRecord} to a list of {@link StatSnapshotApiDTO}.
         *
         * @param records      a list of {@link ReservedInstanceStatsRecord}.
         * @param isRICoverage a boolean which true means it's a reserved instance coverage stats request,
         *                     false means it's a reserved instance utilization stats request.
         * @return a list {@link ReservedInstanceStatsRecord}.
         */
        public List<StatSnapshotApiDTO> convertRIStatsRecordsToStatSnapshotApiDTO(
                @Nonnull final List<ReservedInstanceStatsRecord> records,
                final boolean isRICoverage) {
            return internalConvertRIStatsRecordsToStatSnapshotApiDTO(records, isRICoverage);
        }

        private List<StatSnapshotApiDTO> createRICostStatApiDTO(List<ReservedInstanceCostStat> rICostStats) {
            return convertRICostStatsToSnapshots(rICostStats);
        }
    }
}
