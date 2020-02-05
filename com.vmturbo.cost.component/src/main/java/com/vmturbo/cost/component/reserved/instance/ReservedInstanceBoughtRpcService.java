package com.vmturbo.cost.component.reserved.instance;

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

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.exception.DataAccessException;

import com.vmturbo.common.protobuf.cost.Cost;
import com.vmturbo.common.protobuf.cost.Cost.GetPlanReservedInstanceBoughtRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceBoughtByFilterRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceBoughtByFilterResponse;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceBoughtByIdRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceBoughtByIdResponse;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceBoughtByTopologyRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceBoughtByTopologyResponse;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceBoughtCountByTemplateResponse;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceBoughtCountRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceBoughtCountResponse;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.cost.PlanReservedInstanceServiceGrpc.PlanReservedInstanceServiceBlockingStub;
import com.vmturbo.common.protobuf.cost.Pricing;
import com.vmturbo.common.protobuf.cost.Pricing.OnDemandPriceTable;
import com.vmturbo.common.protobuf.cost.ReservedInstanceBoughtServiceGrpc.ReservedInstanceBoughtServiceImplBase;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc.SupplyChainServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.cost.component.pricing.PriceTableStore;
import com.vmturbo.cost.component.reserved.instance.filter.EntityReservedInstanceMappingFilter;
import com.vmturbo.cost.component.reserved.instance.filter.ReservedInstanceBoughtFilter;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.CurrencyAmount;
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

    private PriceTableStore priceTableStore;

    private ReservedInstanceSpecStore reservedInstanceSpecStore;

    public ReservedInstanceBoughtRpcService(
            @Nonnull final ReservedInstanceBoughtStore reservedInstanceBoughtStore,
            @Nonnull final EntityReservedInstanceMappingStore entityReservedInstanceMappingStore,
            @Nonnull final RepositoryClient repositoryClient,
            @Nonnull final SupplyChainServiceBlockingStub supplyChainServiceBlockingStub,
            @Nonnull final PlanReservedInstanceServiceBlockingStub planReservedInstanceService,
            final long realTimeTopologyContextId,
            @Nonnull final PriceTableStore priceTableStore,
            @Nonnull final ReservedInstanceSpecStore reservedInstanceSpecStore) {
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
    }

    @Override
    public void getReservedInstanceBoughtByTopology(
            final GetReservedInstanceBoughtByTopologyRequest request,
            final StreamObserver<GetReservedInstanceBoughtByTopologyResponse> responseObserver) {

        final List<ReservedInstanceBought> unstitchedReservedInstances;
        if (request.getTopologyType() == TopologyType.REALTIME) {

            unstitchedReservedInstances = reservedInstanceBoughtStore.getReservedInstanceBoughtByFilter(
                    ReservedInstanceBoughtFilter.SELECT_ALL_FILTER);

        } else {
            final long topologyContextId = request.hasTopologyContextId() ?
                                        request.getTopologyContextId() : realtimeTopologyContextId;
            // If getSaved is true, get the saved plan RIs, else get them from real-time.
            // When plan is still being configured, for instance, there will be no saved RIs.
            boolean getSaved = !request.hasGetSaved();
            // Retrieve the RIs selected by user to include in the plan.
            if (getSaved && topologyContextId != realtimeTopologyContextId) {
                final GetPlanReservedInstanceBoughtRequest planSavedRiRequest =
                                                      GetPlanReservedInstanceBoughtRequest
                                                                      .newBuilder()
                                                                      .setPlanId(topologyContextId)
                                                                      .build();
                unstitchedReservedInstances = planReservedInstanceService
                            .getPlanReservedInstanceBought(planSavedRiRequest)
                            .getReservedInstanceBoughtsList();
            } else {
                final Map<EntityType, Set<Long>> cloudScopeTuples = repositoryClient.getEntityOidsByType(
                        request.getScopeSeedOidsList(),
                        topologyContextId,
                        supplyChainServiceBlockingStub);

                final ReservedInstanceBoughtFilter riBoughtFilter = ReservedInstanceBoughtFilter.newBuilder()
                        .cloudScopeTuples(cloudScopeTuples)
                        .build();

                unstitchedReservedInstances = reservedInstanceBoughtStore
                        .getReservedInstanceBoughtByFilter(riBoughtFilter);
            }
            logger.info("Retrieved # of RIs: " + unstitchedReservedInstances.size() + " for planId: "
                                                    + topologyContextId);
        }


        final GetReservedInstanceBoughtByTopologyResponse response =
                GetReservedInstanceBoughtByTopologyResponse.newBuilder()
                        .addAllReservedInstanceBought(
                                createStitchedRIBoughtInstances(unstitchedReservedInstances))
                        .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
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
                            .flatMap(spec -> getOnDemandCurrencyAmountForRISpec(spec,
                                    priceTableByRegion))
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
            final ReservedInstanceSpec riSpec,
            final Map<Long, Pricing.OnDemandPriceTable> priceTableByRegion) {
        final long regionId = riSpec.getReservedInstanceSpecInfo().getRegionId();
        final long tierId = riSpec.getReservedInstanceSpecInfo().getTierId();
        return Optional.ofNullable(priceTableByRegion.get(regionId))
                .map(OnDemandPriceTable::getComputePricesByTierIdMap)
                .map(computeTierPrices -> computeTierPrices.get(tierId))
                .map(ComputeTierPriceList::getBasePrice)
                .flatMap(prices -> prices.getPricesList().stream().findAny())
                .map(Price::getPriceAmount);
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
