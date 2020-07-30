package com.vmturbo.cost.component.reserved.instance;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;


import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.exception.DataAccessException;

import com.vmturbo.common.protobuf.cost.BuyReservedInstanceServiceGrpc.BuyReservedInstanceServiceBlockingStub;
import com.vmturbo.common.protobuf.cost.Cost;
import com.vmturbo.common.protobuf.cost.Cost.GetBuyReservedInstancesByFilterRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetBuyReservedInstancesByFilterResponse;
import com.vmturbo.common.protobuf.cost.Cost.GetPlanReservedInstanceBoughtRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceBoughtByFilterRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceBoughtByFilterResponse;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceBoughtByIdRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceBoughtByIdResponse;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceBoughtCountByTemplateResponse;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceBoughtCountRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceBoughtCountResponse;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceBoughtForAnalysisRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceBoughtForAnalysisResponse;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceBoughtForScopeRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceBoughtForScopeResponse;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpecInfo;
import com.vmturbo.common.protobuf.cost.PlanReservedInstanceServiceGrpc.PlanReservedInstanceServiceBlockingStub;
import com.vmturbo.common.protobuf.cost.Pricing;
import com.vmturbo.common.protobuf.cost.Pricing.OnDemandPriceTable;
import com.vmturbo.common.protobuf.cost.ReservedInstanceBoughtServiceGrpc.ReservedInstanceBoughtServiceImplBase;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc.SupplyChainServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.AnalysisType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.cost.component.pricing.PriceTableStore;
import com.vmturbo.cost.component.reserved.instance.AccountRIMappingStore.AccountRIMappingItem;
import com.vmturbo.cost.component.reserved.instance.filter.EntityReservedInstanceMappingFilter;
import com.vmturbo.cost.component.reserved.instance.filter.ReservedInstanceBoughtFilter;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.CurrencyAmount;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.PricingDTO.ComputeTierPriceList;
import com.vmturbo.platform.sdk.common.PricingDTO.Price;
import com.vmturbo.repository.api.RepositoryClient;

public class ReservedInstanceBoughtRpcService extends ReservedInstanceBoughtServiceImplBase {

    private final Logger logger = LogManager.getLogger();

    private final ReservedInstanceBoughtStore reservedInstanceBoughtStore;

    private final EntityReservedInstanceMappingStore entityReservedInstanceMappingStore;

    private final RepositoryClient repositoryClient;

    private final SupplyChainServiceBlockingStub supplyChainServiceBlockingStub;

    private final PlanReservedInstanceServiceBlockingStub planReservedInstanceService;

    private final Long realtimeTopologyContextId;

    private final PriceTableStore priceTableStore;

    private final ReservedInstanceSpecStore reservedInstanceSpecStore;

    private final BuyReservedInstanceServiceBlockingStub buyRIServiceClient;

    private final AccountRIMappingStore accountRIMappingStore;

    public ReservedInstanceBoughtRpcService(
            @Nonnull final ReservedInstanceBoughtStore reservedInstanceBoughtStore,
            @Nonnull final EntityReservedInstanceMappingStore entityReservedInstanceMappingStore,
            @Nonnull final RepositoryClient repositoryClient,
            @Nonnull final SupplyChainServiceBlockingStub supplyChainServiceBlockingStub,
            @Nonnull final PlanReservedInstanceServiceBlockingStub planReservedInstanceService,
            final long realTimeTopologyContextId,
            @Nonnull final PriceTableStore priceTableStore,
            @Nonnull final ReservedInstanceSpecStore reservedInstanceSpecStore,
            @Nonnull final BuyReservedInstanceServiceBlockingStub buyRIServiceClient,
            @Nonnull final AccountRIMappingStore accountRIMappingStore) {
        this.reservedInstanceBoughtStore =
                Objects.requireNonNull(reservedInstanceBoughtStore);
        this.entityReservedInstanceMappingStore =
                Objects.requireNonNull(entityReservedInstanceMappingStore);
        this.repositoryClient = repositoryClient;
        this.supplyChainServiceBlockingStub = supplyChainServiceBlockingStub;
        this.planReservedInstanceService = planReservedInstanceService;
        this.realtimeTopologyContextId = realTimeTopologyContextId;
        this.priceTableStore = Objects.requireNonNull(priceTableStore);
        this.reservedInstanceSpecStore = Objects.requireNonNull(reservedInstanceSpecStore);
        this.buyRIServiceClient = Objects.requireNonNull(buyRIServiceClient);
        this.accountRIMappingStore = Objects.requireNonNull(accountRIMappingStore);
    }


    @Override
    public void getReservedInstanceBoughtForAnalysis(
            GetReservedInstanceBoughtForAnalysisRequest request,
            final StreamObserver<GetReservedInstanceBoughtForAnalysisResponse> responseObserver) {

        final TopologyInfo topoInfo = request.getTopologyInfo();

        List<ReservedInstanceBought> unstitchedReservedInstances;
        if (topoInfo.hasTopologyContextId() && topoInfo.getTopologyContextId() != realtimeTopologyContextId) {
            final GetPlanReservedInstanceBoughtRequest planSavedRiRequest =
                    GetPlanReservedInstanceBoughtRequest
                            .newBuilder()
                            .setPlanId(topoInfo.getTopologyContextId())
                            .build();
            unstitchedReservedInstances = planReservedInstanceService
                    .getPlanReservedInstanceBought(planSavedRiRequest)
                    .getReservedInstanceBoughtsList();
        } else {
            unstitchedReservedInstances = reservedInstanceBoughtStore.getReservedInstanceBoughtByFilter(
                    ReservedInstanceBoughtFilter.SELECT_ALL_FILTER);
        }

        // if we are doing RI analysis in Market - add recommended RIs to already bought
        final boolean isBoughtRiInAnalysis = topoInfo.hasPlanInfo() && topoInfo.getPlanInfo()
                .getPlanType()
                .equals(StringConstants.OPTIMIZE_CLOUD_PLAN) &&
                !topoInfo.getAnalysisTypeList().contains(AnalysisType.BUY_RI_IMPACT_ANALYSIS);

        if (isBoughtRiInAnalysis) {
            List<ReservedInstanceBought> buyRIs = getBuyRIs(topoInfo);
            unstitchedReservedInstances = Collections.unmodifiableList(
                    Stream.concat(buyRIs.stream(), unstitchedReservedInstances.stream())
                            .collect(Collectors.toList()));
        }
        final Set<ReservedInstanceBought> stitchedRIs =
                createStitchedRIBoughtInstances(unstitchedReservedInstances);
        final Set<ReservedInstanceBought> updatedRis = adjustAvailableCouponsForPartialCloudEnv(stitchedRIs);
        final GetReservedInstanceBoughtForAnalysisResponse response =
                GetReservedInstanceBoughtForAnalysisResponse.newBuilder()
                        .addAllReservedInstanceBought(updatedRis)
                        .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    /**
     * If an RI is undicoverd, cap the available number of coupons to the number of coupons
     * used by discovered accounts.
     * @param stitchedRIs Set of RIs
     * @return Set of Ris with the undiscovered RIs updated with
     * the capacity capped to used coupons
     */
    private Set<ReservedInstanceBought> adjustAvailableCouponsForPartialCloudEnv(
            final Set<ReservedInstanceBought> stitchedRIs) {
        List<TopologyDTO.TopologyEntityDTO> allBusinessAccounts =
                repositoryClient.getAllBusinessAccounts(realtimeTopologyContextId);
        List<ReservedInstanceBought> riFromUndiscoveredAccounts = stitchedRIs.stream()
                .filter(ri -> !isRIPurchasedByDiscoveredAccount(ri, allBusinessAccounts))
                .collect(Collectors.toList());
        List<Long> undiscoveredRiIds = riFromUndiscoveredAccounts.stream()
                .map(ri -> ri.getId())
                .collect(Collectors.toList());
        // Retrieve the total  used coupons from discovered workloads for each RI
        final Map<Long, Double> discRiToUsedCouponMap = entityReservedInstanceMappingStore
                .getReservedInstanceUsedCouponsMapByFilter(
                        EntityReservedInstanceMappingFilter.newBuilder().riBoughtFilter(
                                Cost.ReservedInstanceBoughtFilter.newBuilder()
                                        .addAllRiBoughtId(undiscoveredRiIds).build()).build());

        // Retrieve the total  used coupons from undiscovered accounts for each RI
        final Map<Long, Double> undiscoveredAccountRIUsage = getUndiscoveredAccountUsageForRI(allBusinessAccounts.stream()
                .map(dto -> dto.getOid()).collect(Collectors.toList()));

        // Update the capacities for each RI
        return stitchedRIs.stream()
                .map(ReservedInstanceBought::toBuilder)
                .peek(riBuilder -> {
                    if (undiscoveredRiIds.contains(riBuilder.getId())) {
                        riBuilder.getReservedInstanceBoughtInfoBuilder()
                                .getReservedInstanceBoughtCouponsBuilder()
                                .setNumberOfCouponsUsed(
                                        discRiToUsedCouponMap.getOrDefault(riBuilder.getId(),
                                                0d))
                                .setNumberOfCoupons(discRiToUsedCouponMap.getOrDefault(riBuilder.getId(),
                                        0d).intValue());
                    } else {
                        int capacity = riBuilder.getReservedInstanceBoughtInfoBuilder()
                                .getReservedInstanceBoughtCouponsBuilder()
                                .getNumberOfCoupons();
                        riBuilder.getReservedInstanceBoughtInfoBuilder()
                                .getReservedInstanceBoughtCouponsBuilder()
                                .setNumberOfCoupons(capacity - undiscoveredAccountRIUsage.getOrDefault(riBuilder.getId(),
                                        0d).intValue());
                    }
                })
                .map(ReservedInstanceBought.Builder::build)
                .collect(Collectors.toSet());
    }

    private Map<Long, Double> getUndiscoveredAccountUsageForRI(final List<Long> baOids) {
        final Map<Long, List<AccountRIMappingItem>> usedCouponInUndiscAccounts =
                accountRIMappingStore.getAccountRICoverageMappings(baOids);
        Map<Long, Double> undiscoveredAccountRIUsage =
                usedCouponInUndiscAccounts.values().stream()
                    .flatMap(List::stream)
                    .collect(Collectors.toMap(AccountRIMappingItem::getReservedInstanceId,
                                                AccountRIMappingItem::getUsedCoupons,
                                                 (oldValue, newValue) -> oldValue + newValue));
        if (undiscoveredAccountRIUsage.isEmpty()) {
            logger.warn("No RI usage for undiscovered accounts recorded.");
        }
        return undiscoveredAccountRIUsage;
    }

    private boolean isRIPurchasedByDiscoveredAccount(final ReservedInstanceBought ri,
                                                     final List<TopologyEntityDTO> allBusinessAccounts) {
        long riPurchasingAccount = ri.getReservedInstanceBoughtInfo().getBusinessAccountId();
        Optional<TopologyEntityDTO> discoveredBA = allBusinessAccounts.stream()
                .filter(baDTO -> baDTO.hasTypeSpecificInfo()
                &&  baDTO.getTypeSpecificInfo().hasBusinessAccount()
                && baDTO.getTypeSpecificInfo().getBusinessAccount().hasAssociatedTargetId()
                && riPurchasingAccount
                        == baDTO.getTypeSpecificInfo().getBusinessAccount().getAssociatedTargetId())
                .findFirst();
        return discoveredBA.isPresent();
    }


    @Override
    public void getReservedInstanceBoughtForScope(GetReservedInstanceBoughtForScopeRequest request,
            final StreamObserver<GetReservedInstanceBoughtForScopeResponse> responseObserver) {

        // Retrieve the RIs selected by user to include in the plan.

        // If contextId is not real time, get the saved plan RIs, else get them from real-time.
        // When plan is still being configured, for instance, there will be no saved RIs.
        List<ReservedInstanceBought> unstitchedReservedInstances =
                    getBoughtReservedInstancesInScope(request.getScopeSeedOidsList(), realtimeTopologyContextId);

        logger.info("Retrieved # of RIs: {} for topologyContextId: {}", unstitchedReservedInstances.size(),
                realtimeTopologyContextId);

        final GetReservedInstanceBoughtForScopeResponse response =
                GetReservedInstanceBoughtForScopeResponse.newBuilder()
                        .addAllReservedInstanceBought(
                                createStitchedRIBoughtInstances(unstitchedReservedInstances))
                        .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    /**
     * Get Buy RIs (recommended RIs) for provided topology info.
     *
     * @param topoInfo topolgy info
     * @return list of Buy RIs (recommended RIs)
     */
    private List<ReservedInstanceBought> getBuyRIs(TopologyInfo topoInfo) {
        final GetBuyReservedInstancesByFilterResponse buyRIBoughtResponse =
                buyRIServiceClient.getBuyReservedInstancesByFilter(
                        GetBuyReservedInstancesByFilterRequest
                                .newBuilder().setTopologyContextId(topoInfo.getTopologyContextId())
                                .build());
       return buyRIBoughtResponse.getReservedInstanceBoughtsList();
    }

    /**
     * Gets all usable RIs in the specified scope- scopes available RIs by region, zone, and business account if
     * applicable, then filters out subscription-scope RIs if they cannot be applied to any workloads in the scope.
     *
     * @param scopeIds OIDs specified here seed the scoping expansion
     * @param topologyContextId the topology context Id from which RIs should be retrieved
     * @return a list of {@link ReservedInstanceBought} that are usable by workloads in the current scope
     */
    public List<ReservedInstanceBought> getBoughtReservedInstancesInScope(
            @Nonnull List<Long> scopeIds,
            @Nonnull final long topologyContextId) {
        Set<Long> scopeIdSet = Sets.newHashSet(scopeIds);
        List<TopologyDTO.TopologyEntityDTO> allBusinessAccounts =
            repositoryClient.getAllBusinessAccounts(realtimeTopologyContextId);
        Map<Long, Set<Long>> baOidToEaSiblingAccounts =
            RepositoryClient.getBaOidToEaSiblingAccounts(allBusinessAccounts);
        Set<Long> scopeBusinessAccountsOids =
            RepositoryClient.getFilteredScopeBusinessAccountOids(scopeIdSet, allBusinessAccounts);
        Set<Long> allBusinessAccountOidsInScope =
            RepositoryClient.getAllBusinessAccountOidsInScope(scopeBusinessAccountsOids, baOidToEaSiblingAccounts);

        final Map<EntityType, Set<Long>> cloudScopeTuples = repositoryClient.getEntityOidsByTypeForRIQuery(
                scopeIds,
                topologyContextId,
                supplyChainServiceBlockingStub,
                allBusinessAccountOidsInScope);
        Set<Long> zoneIds = cloudScopeTuples.get(EntityType.AVAILABILITY_ZONE);
        if (CollectionUtils.isNotEmpty(zoneIds) && scopeIdSet.equals(zoneIds)) {
            // If there is a zone-based filter, don't add region also. For zone scopes,
            // we need to return only zonal RIs, not zonal+regional.
            Set<Long> regionIds = cloudScopeTuples.remove(EntityType.REGION);
            logger.trace("Removed {} regions for {} zones from plan {} RI scope. Seeds: {}",
                    regionIds, zoneIds, topologyContextId, scopeIds);
        }
        final ReservedInstanceBoughtFilter riBoughtFilter = ReservedInstanceBoughtFilter.newBuilder()
                .cloudScopeTuples(cloudScopeTuples)
                .build();

        final List<ReservedInstanceBought> boughtReservedInstances = reservedInstanceBoughtStore
                .getReservedInstanceBoughtByFilter(riBoughtFilter);

        // If Business Account was considered in RI selection, (request.getScopeSeedOidsList() represented one or more
        // Business Accounts or workloads) filter on shared vs. subscription scope
        return cloudScopeTuples.containsKey(EntityType.BUSINESS_ACCOUNT)
            ? boughtReservedInstances.stream()
            .filter(boughtReservedInstance -> {
                if (!boughtReservedInstance.hasReservedInstanceBoughtInfo()) {
                    return false;
                }
                ReservedInstanceBought.ReservedInstanceBoughtInfo reservedInstanceBoughtInfo =
                    boughtReservedInstance.getReservedInstanceBoughtInfo();
                if (!reservedInstanceBoughtInfo.hasReservedInstanceScopeInfo()) {
                    return false;
                }
                ReservedInstanceBought.ReservedInstanceBoughtInfo.ReservedInstanceScopeInfo scopeInfo =
                    reservedInstanceBoughtInfo.getReservedInstanceScopeInfo();
                if (scopeInfo.getShared()) {
                    return true;
                }
                return scopeInfo.getApplicableBusinessAccountIdList().stream()
                    .filter(scopeBusinessAccountsOids::contains)
                    .collect(Collectors.counting()) > 0;
            })
            .collect(Collectors.toList())
            : boughtReservedInstances;
    }

    @Override
    public void getReservedInstanceBoughtByFilter(
            GetReservedInstanceBoughtByFilterRequest request,
            StreamObserver<GetReservedInstanceBoughtByFilterResponse> responseObserver) {
        try {
            final ReservedInstanceBoughtFilter filter = ReservedInstanceBoughtFilter.newBuilder()
                    .regionFilter(request.getRegionFilter())
                    .availabilityZoneFilter(request.getZoneFilter())
                    .accountFilter(request.getAccountFilter())
                    .build();
            final List<ReservedInstanceBought> reservedInstancesBought =
                           reservedInstanceBoughtStore
                               .getReservedInstanceBoughtByFilter(filter);

            final GetReservedInstanceBoughtByFilterResponse response =
                    GetReservedInstanceBoughtByFilterResponse.newBuilder()
                            .addAllReservedInstanceBoughts(
                                    createStitchedRIBoughtInstances(reservedInstancesBought))
                            .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (DataAccessException e) {
            responseObserver.onError(Status.INTERNAL
                    .withDescription("Failed to get reserved instance bought by filter.")
                    .asException());
        } catch (StatusRuntimeException e) {
            responseObserver.onError(Status.INTERNAL
                .withDescription("Failed to get reserved instance bought by filter due to " + e.getLocalizedMessage())
                .asException());
        }
    }

    @Override
    public void getReservedInstanceBoughtById(
            GetReservedInstanceBoughtByIdRequest request,
            StreamObserver<GetReservedInstanceBoughtByIdResponse> responseObserver) {
        try {
            final ReservedInstanceBoughtFilter filter = ReservedInstanceBoughtFilter.newBuilder()
                            .riBoughtFilter(Cost.ReservedInstanceBoughtFilter
                                            .newBuilder()
                                            .setExclusionFilter(false)
                                            .addAllRiBoughtId(request.getRiFilter().getRiIdList())
                                            .build())
                            .build();
            final List<ReservedInstanceBought> reservedInstancesBought =
                           reservedInstanceBoughtStore.getReservedInstanceBoughtByFilter(filter);

            final GetReservedInstanceBoughtByIdResponse response =
                    GetReservedInstanceBoughtByIdResponse.newBuilder()
                            .addAllReservedInstanceBought(
                                    createStitchedRIBoughtInstances(reservedInstancesBought))
                            .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (DataAccessException e) {
            responseObserver.onError(Status.INTERNAL
                    .withDescription("Failed to get reserved instance bought by Id filter.")
                    .asException());
        } catch (StatusRuntimeException e) {
            responseObserver.onError(Status.INTERNAL
                .withDescription("Failed to get reserved instance bought by Id filter due to " + e.getLocalizedMessage())
                .asException());
        }
    }

    private Stream<ReservedInstanceBought> stitchRICouponsUsed(
            List<ReservedInstanceBought> reservedInstancesBought, List<Long> riBoughtIds) {
        final EntityReservedInstanceMappingFilter filter = EntityReservedInstanceMappingFilter
                .newBuilder()
                .riBoughtFilter(Cost.ReservedInstanceBoughtFilter.newBuilder()
                        .addAllRiBoughtId(riBoughtIds)
                        .build())
                .build();
        final Map<Long, Double> reservedInstanceUsedCouponsMap = entityReservedInstanceMappingStore
                .getReservedInstanceUsedCouponsMapByFilter(filter);
        return reservedInstancesBought.stream()
                .map(ReservedInstanceBought::toBuilder)
                .peek(riBuilder -> riBuilder
                        .getReservedInstanceBoughtInfoBuilder()
                        .getReservedInstanceBoughtCouponsBuilder()
                        .setNumberOfCouponsUsed(
                                reservedInstanceUsedCouponsMap
                                        .getOrDefault(riBuilder.getId(), 0D)))
                .map(ReservedInstanceBought.Builder::build);
    }

    private Stream<ReservedInstanceBought> stitchOnDemandComputeTierCost(
            final Stream<ReservedInstanceBought> reservedInstancesBought,
            final Set<Long> riSpecIds) {
        final Map<Long, ReservedInstanceSpec> riSpecIdToRiSpec =
                reservedInstanceSpecStore.getReservedInstanceSpecByIds(riSpecIds).stream()
                .collect(Collectors.toMap(ReservedInstanceSpec::getId, Function.identity()));

        final Map<Long, Pricing.OnDemandPriceTable> priceTableByRegion =
                priceTableStore.getMergedPriceTable().getOnDemandPriceByRegionIdMap();

        return reservedInstancesBought.map(ReservedInstanceBought::toBuilder)
                .peek(riBoughtBuilder -> {
                    Optional.ofNullable(riSpecIdToRiSpec.get(riBoughtBuilder
                            .getReservedInstanceBoughtInfo().getReservedInstanceSpec()))
                            .flatMap(spec -> getOnDemandCurrencyAmountForRISpec(
                                    spec.getReservedInstanceSpecInfo(), priceTableByRegion))
                            .ifPresent(amount -> riBoughtBuilder
                            .getReservedInstanceBoughtInfoBuilder()
                            .getReservedInstanceDerivedCostBuilder()
                            .setOnDemandRatePerHour(amount));

                    logger.trace("ReservedInstanceBought after stitching currency amount: {}",
                            () -> riBoughtBuilder);
                })
                .map(ReservedInstanceBought.Builder::build);
    }

    private Optional<CurrencyAmount> getOnDemandCurrencyAmountForRISpec(
            final ReservedInstanceSpecInfo riSpecInfo,
            final Map<Long, Pricing.OnDemandPriceTable> priceTableByRegion) {
        final long regionId = riSpecInfo.getRegionId();
        final long tierId = riSpecInfo.getTierId();
        final OSType osType = riSpecInfo.getOs();
        return Optional.ofNullable(priceTableByRegion.get(regionId))
                .map(OnDemandPriceTable::getComputePricesByTierIdMap)
                .map(computeTierPrices -> computeTierPrices.get(tierId))
                .flatMap(priceList -> getOsAdjustedAmount(priceList, osType,
                        riSpecInfo.getPlatformFlexible()));
    }

    private Optional<CurrencyAmount> getOsAdjustedAmount(final ComputeTierPriceList priceList,
                                                         final OSType osType,
                                                         final boolean platformFlexible) {
        final Optional<CurrencyAmount> baseAmount = priceList.getBasePrice()
                .getPricesList()
                .stream()
                .findAny()
                .map(Price::getPriceAmount);
        if (platformFlexible || priceList.getBasePrice().getGuestOsType() == osType) {
            return baseAmount;
        } else {
            return priceList.getPerConfigurationPriceAdjustmentsList().stream()
                    .filter(configPrice -> configPrice.getGuestOsType() == osType)
                    .filter(configPrice -> !configPrice.getPricesList().isEmpty())
                    .findAny()
                    .map(configPrice -> configPrice.getPricesList().iterator().next()
                            .getPriceAmount())
                    .flatMap(osCost -> baseAmount.map(baseCost -> CurrencyAmount.newBuilder()
                            .setAmount(baseCost.getAmount() + osCost.getAmount())
                            .build()));
        }
    }

    @Override
    public void getReservedInstanceBoughtCount(
            GetReservedInstanceBoughtCountRequest request,
            StreamObserver<GetReservedInstanceBoughtCountResponse> responseObserver) {
        try {
            final ReservedInstanceBoughtFilter filter = ReservedInstanceBoughtFilter.newBuilder()
                    .regionFilter(request.getRegionFilter())
                    .accountFilter(request.getAccountFilter())
                    .availabilityZoneFilter(request.getAvailabilityZoneFilter())
                    .build();

            Map<Long, Long> reservedInstanceCountMap =
                    reservedInstanceBoughtStore.getReservedInstanceCountMap(filter);
            final GetReservedInstanceBoughtCountResponse response =
                    GetReservedInstanceBoughtCountResponse.newBuilder()
                        .putAllReservedInstanceCountMap(reservedInstanceCountMap)
                        .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (DataAccessException e) {
            responseObserver.onError(Status.INTERNAL
                    .withDescription("Failed to get reserved instance count map.")
                    .asException());
        }
    }

    @Override
    public void getReservedInstanceBoughtCountByTemplateType(
            GetReservedInstanceBoughtCountRequest request,
            StreamObserver<GetReservedInstanceBoughtCountByTemplateResponse> responseObserver) {
        try {
            final ReservedInstanceBoughtFilter filter = ReservedInstanceBoughtFilter.newBuilder()
                    .regionFilter(request.getRegionFilter())
                    .accountFilter(request.getAccountFilter())
                    .availabilityZoneFilter(request.getAvailabilityZoneFilter())
                    .build();

            final Map<Long, Long> riCountByRiSpecId = reservedInstanceBoughtStore
                    .getReservedInstanceCountByRISpecIdMap(filter);

            final Map<Long, ReservedInstanceSpec> riSpecBySpecId = reservedInstanceSpecStore
                    .getReservedInstanceSpecByIds(riCountByRiSpecId.keySet())
                    .stream().collect(Collectors.toMap(ReservedInstanceSpec::getId,
                            Function.identity()));

            final Map<Long, Long> riBoughtCountByTierId =
                    riCountByRiSpecId.entrySet().stream()
                            .filter(e -> riSpecBySpecId.containsKey(e.getKey()))
                            .collect(Collectors.toMap(e -> riSpecBySpecId.get(e.getKey())
                                            .getReservedInstanceSpecInfo().getTierId(),
                                    Entry::getValue, Long::sum));

            final GetReservedInstanceBoughtCountByTemplateResponse response =
                    GetReservedInstanceBoughtCountByTemplateResponse.newBuilder()
                            .putAllReservedInstanceCountMap(riBoughtCountByTierId)
                            .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (DataAccessException e) {
            responseObserver.onError(Status.INTERNAL
                    .withDescription("Failed to get reserved instance count map.")
                    .asException());
        }
    }

    private Set<ReservedInstanceBought> createStitchedRIBoughtInstances(
            @Nonnull List<ReservedInstanceBought> reservedInstances) {

        final List<Long> riOids = reservedInstances.stream()
                .map(ReservedInstanceBought::getId)
                .collect(ImmutableList.toImmutableList());

        final Set<Long> riSpecIds = reservedInstances.stream()
                .map(riBought -> riBought.getReservedInstanceBoughtInfo()
                        .getReservedInstanceSpec())
                .collect(ImmutableSet.toImmutableSet());

        return stitchOnDemandComputeTierCost(stitchRICouponsUsed(reservedInstances,
                        riOids), riSpecIds)
                .collect(ImmutableSet.toImmutableSet());
    }
}
