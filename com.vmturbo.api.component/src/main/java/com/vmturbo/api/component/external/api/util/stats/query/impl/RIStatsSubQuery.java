package com.vmturbo.api.component.external.api.util.stats.query.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.Sets;

import org.apache.commons.collections4.CollectionUtils;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.api.component.external.api.util.BuyRiScopeHandler;
import com.vmturbo.api.component.external.api.util.StatsUtils;
import com.vmturbo.api.component.external.api.util.stats.StatsQueryContextFactory.StatsQueryContext;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.statistic.StatApiInputDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.exceptions.ConversionException;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.common.protobuf.cost.Cost.AccountFilter;
import com.vmturbo.common.protobuf.cost.Cost.AvailabilityZoneFilter;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceBoughtCountRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceCostStatsRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceCostStatsRequest.GroupBy;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceCoverageStatsRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceUtilizationStatsRequest;
import com.vmturbo.common.protobuf.cost.Cost.RegionFilter;
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

    private static final Set<UIEntityType> VALID_ENTITY_TYPES_FOR_RI_STATS =
            Sets.immutableEnumSet(UIEntityType.AVAILABILITY_ZONE, UIEntityType.BUSINESS_ACCOUNT,
                    UIEntityType.REGION, UIEntityType.SERVICE_PROVIDER,
                    UIEntityType.VIRTUAL_MACHINE);

    private final ReservedInstanceUtilizationCoverageServiceBlockingStub
            riUtilizationCoverageService;
    private final ReservedInstanceBoughtServiceBlockingStub riBoughtService;

    private final ReservedInstanceCostServiceBlockingStub reservedInstanceCostService;
    private final UserSessionContext userSessionContext;

    /**
     * Constructor.
     *
     * @param riUtilizationCoverageService the {@link ReservedInstanceUtilizationCoverageServiceBlockingStub}
     * @param riBoughtService the {@link ReservedInstanceBoughtServiceBlockingStub}
     * @param repositoryApi the {@link RepositoryApi}
     * @param reservedInstanceCostService the {@link ReservedInstanceCostServiceBlockingStub}
     * @param buyRiScopeHandler the {@link BuyRiScopeHandler}
     * @param userSessionContext the {@link UserSessionContext}
     */
    public RIStatsSubQuery(
            @Nonnull final ReservedInstanceUtilizationCoverageServiceBlockingStub riUtilizationCoverageService,
            @Nonnull final ReservedInstanceBoughtServiceBlockingStub riBoughtService,
            @Nonnull final RepositoryApi repositoryApi,
            @Nonnull final ReservedInstanceCostServiceBlockingStub reservedInstanceCostService,
            @Nonnull final BuyRiScopeHandler buyRiScopeHandler,
            @Nonnull final UserSessionContext userSessionContext) {
        super(repositoryApi, buyRiScopeHandler);
        this.riUtilizationCoverageService = riUtilizationCoverageService;
        this.riBoughtService = riBoughtService;
        this.reservedInstanceCostService = reservedInstanceCostService;
        this.userSessionContext = userSessionContext;
    }

    @Override
    public boolean applicableInContext(@Nonnull final StatsQueryContext context) {
        // plans should use separate sub query
        if (context.getInputScope().isPlan()) {
            return false;
        }
        return context.getQueryScope()
                .getGlobalScope()
                .map(globalScope -> globalScope.entityTypes().isEmpty() ||
                        VALID_ENTITY_TYPES_FOR_RI_STATS.contains(
                                globalScope.entityTypes().iterator().next()))
                .orElse(true);
    }

    @Nonnull
    @Override
    public List<StatSnapshotApiDTO> getAggregateStats(@Nonnull final Set<StatApiInputDTO> stats,
            @Nonnull final StatsQueryContext context)
            throws OperationFailedException, InterruptedException {
        final List<StatSnapshotApiDTO> snapshots = new ArrayList<>();

        if (StatsUtils.isValidScopeForRIBoughtQuery(context.getInputScope())) {
            if (containsStat(StringConstants.NUM_RI, stats)) {
                try {
                    snapshots.addAll(convertNumRIStatsMapToStatSnapshotApiDTO(
                            getRIBoughtCountByTierName(createRIBoughtCountRequest(context))));
                } catch (ConversionException e) {
                    throw new OperationFailedException(e.getMessage(), e);
                }
            }

            if (containsStat(StringConstants.RI_COST, stats)) {
                snapshots.addAll(convertRICostStatsToSnapshots(
                        reservedInstanceCostService.getReservedInstanceCostStats(
                                createRICostRequest(context)).getStatsList()));
            }

            if (containsStat(StringConstants.RI_COUPON_UTILIZATION, stats)) {
                snapshots.addAll(internalConvertRIStatsRecordsToStatSnapshotApiDTO(
                        riUtilizationCoverageService.getReservedInstanceUtilizationStats(
                                createUtilizationRequest(context))
                                .getReservedInstanceStatsRecordsList(), false));
            }
        }

        if (isValidScopeForCoverageRequest(context) &&
                containsStat(StringConstants.RI_COUPON_COVERAGE, stats)) {
            snapshots.addAll(internalConvertRIStatsRecordsToStatSnapshotApiDTO(
                    riUtilizationCoverageService.getReservedInstanceCoverageStats(
                            createCoverageRequest(context)).getReservedInstanceStatsRecordsList(),
                    true));
        }

        return mergeStatsByDate(snapshots);
    }

    @Nonnull
    GetReservedInstanceUtilizationStatsRequest createUtilizationRequest(
            @Nonnull final StatsQueryContext context) throws OperationFailedException {
        return internalCreateUtilizationRequest(context,
                GetReservedInstanceUtilizationStatsRequest.newBuilder()
                        .setIncludeBuyRiUtilization(
                                getBuyRiScopeHandler().shouldIncludeBuyRiDiscount(
                                        context.getInputScope())));
    }

    @Nonnull
    GetReservedInstanceCoverageStatsRequest createCoverageRequest(
            @Nonnull final StatsQueryContext context) throws OperationFailedException {
        return internalCreateCoverageRequest(context,
                GetReservedInstanceCoverageStatsRequest.newBuilder()
                        .setIncludeBuyRiCoverage(getBuyRiScopeHandler().shouldIncludeBuyRiDiscount(
                                context.getInputScope())));
    }

    @Nonnull
    GetReservedInstanceBoughtCountRequest createRIBoughtCountRequest(
            @Nonnull final StatsQueryContext context) throws OperationFailedException {
        final GetReservedInstanceBoughtCountRequest.Builder reqBuilder =
                GetReservedInstanceBoughtCountRequest.newBuilder();
        final ApiId inputScope = context.getInputScope();
        if (inputScope.getScopeTypes().isPresent()) {
            final Set<UIEntityType> uiEntityTypes = inputScope.getScopeTypes().get();
            if (CollectionUtils.isEmpty(uiEntityTypes)) {
                throw new OperationFailedException("Entity type not present");
            }
            final UIEntityType type = uiEntityTypes.iterator().next();
            switch (type) {
                case REGION:
                    reqBuilder.setRegionFilter(
                            RegionFilter.newBuilder().addAllRegionId(getScopeEntities(context)));
                    break;
                case AVAILABILITY_ZONE:
                    reqBuilder.setAvailabilityZoneFilter(AvailabilityZoneFilter.newBuilder()
                            .addAllAvailabilityZoneId(getScopeEntities(context)));
                    break;
                case BUSINESS_ACCOUNT:
                    reqBuilder.setAccountFilter(
                            AccountFilter.newBuilder().addAllAccountId(getScopeEntities(context)));
                    break;
                case SERVICE_PROVIDER:
                    reqBuilder.setRegionFilter(RegionFilter.newBuilder()
                            .addAllRegionId(
                                    translateServiceProvidersToRegions(getScopeEntities(context))));
                    break;
                default:
                    throw new OperationFailedException(
                            String.format("Invalid scope for query: %s", type.apiStr()));
            }
        } else if (!context.isGlobalScope()) {
            throw new OperationFailedException(
                    "Invalid scope for query. Must be global or have an entity type.");
        }
        return reqBuilder.build();
    }

    @Nonnull
    GetReservedInstanceCostStatsRequest createRICostRequest(
            @Nonnull final StatsQueryContext context) throws OperationFailedException {
        final GetReservedInstanceCostStatsRequest.Builder reqBuilder =
                GetReservedInstanceCostStatsRequest.newBuilder();

        final ApiId inputScope = context.getInputScope();
        if (inputScope.getScopeTypes().isPresent() && !inputScope.getScopeTypes().get().isEmpty()) {
            final Set<UIEntityType> uiEntityTypes = inputScope.getScopeTypes().get();
            if (uiEntityTypes.size() != 1) {
                //TODO (mahdi) Change the logic to support scopes with more than one type
                throw new IllegalStateException("Scopes with more than one type is not supported.");
            }
            final UIEntityType type = uiEntityTypes.iterator().next();
            switch (type) {
                case REGION:
                    reqBuilder.setRegionFilter(
                            RegionFilter.newBuilder().addAllRegionId(getScopeEntities(context)));
                    break;
                case AVAILABILITY_ZONE:
                    reqBuilder.setAvailabilityZoneFilter(AvailabilityZoneFilter.newBuilder()
                            .addAllAvailabilityZoneId(getScopeEntities(context)));
                    break;
                case BUSINESS_ACCOUNT:
                    reqBuilder.setAccountFilter(
                            AccountFilter.newBuilder().addAllAccountId(getScopeEntities(context)));
                    break;
                case SERVICE_PROVIDER:
                    reqBuilder.setRegionFilter(RegionFilter.newBuilder()
                            .addAllRegionId(
                                    translateServiceProvidersToRegions(getScopeEntities(context))));
                    break;
                default:
                    throw new OperationFailedException(
                            String.format("Invalid scope for query: %s", type.apiStr()));
            }
        } else if (!context.isGlobalScope()) {
            throw new OperationFailedException(
                    "Invalid scope for query. Must be global or have an entity type.");
        }

        return reqBuilder.setIncludeProjected(true)
                .setGroupBy(GroupBy.SNAPSHOT_TIME)
                .setTopologyContextId(inputScope.getTopologyContextId())
                //todo: setTimeWindow if we have a context.getTimeWindow().
                .setTimeWindow(StatsRequestTimeWindow.newBuilder().setQueryLatest(true))
                .setIncludeBuyRi(getBuyRiScopeHandler().shouldIncludeBuyRiDiscount(inputScope))
                .build();
    }

    private Map<String, Long> getRIBoughtCountByTierName(
            final GetReservedInstanceBoughtCountRequest countRequest)
            throws ConversionException, InterruptedException {
        final Map<Long, Long> riBoughtCountsByTierId =
                riBoughtService.getReservedInstanceBoughtCountByTemplateType(countRequest)
                        .getReservedInstanceCountMapMap();
        final Map<Long, ServiceEntityApiDTO> tierApiDTOByTierId =
                getRepositoryApi().entitiesRequest(riBoughtCountsByTierId.keySet()).getSEMap();
        return riBoughtCountsByTierId.entrySet()
                .stream()
                .filter(e -> tierApiDTOByTierId.containsKey(e.getKey()))
                .collect(Collectors.toMap(e -> tierApiDTOByTierId.get(e.getKey()).getDisplayName(),
                        Entry::getValue, Long::sum));
    }

    /**
     * Check if valid scope for RI Coverage request.
     *
     * @param context the {@link StatsQueryContext}.
     * @return {@code true} if scope is valid for coverage request
     */
    private boolean isValidScopeForCoverageRequest(@Nonnull final StatsQueryContext context) {
        // Only allow non-scoped-observer users.
        if (userSessionContext.isUserObserver() && userSessionContext.isUserScoped()) {
            return false;
        }
        return context.getInputScope()
                .getScopeTypes()
                .map(scopeTypes -> scopeTypes.size() == 1)
                .orElse(context.isGlobalScope());
    }
}
