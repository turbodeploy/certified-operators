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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.exception.DataAccessException;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

import com.vmturbo.common.protobuf.cost.Cost.AccountFilter;
import com.vmturbo.common.protobuf.cost.Cost.AvailabilityZoneFilter;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceBoughtByFilterRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceBoughtByFilterResponse;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceBoughtCountByTemplateResponse;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceBoughtCountRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceBoughtCountResponse;
import com.vmturbo.common.protobuf.cost.Cost.RegionFilter;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.cost.Pricing;
import com.vmturbo.common.protobuf.cost.Pricing.OnDemandPriceTable;
import com.vmturbo.common.protobuf.cost.ReservedInstanceBoughtServiceGrpc.ReservedInstanceBoughtServiceImplBase;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc.SupplyChainServiceBlockingStub;
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

    private final Long realtimeTopologyContextId;

    private PriceTableStore priceTableStore;

    private ReservedInstanceSpecStore reservedInstanceSpecStore;

    public ReservedInstanceBoughtRpcService(
            @Nonnull final ReservedInstanceBoughtStore reservedInstanceBoughtStore,
            @Nonnull final EntityReservedInstanceMappingStore entityReservedInstanceMappingStore,
            @Nonnull final RepositoryClient repositoryClient,
            @Nonnull final SupplyChainServiceBlockingStub supplyChainServiceBlockingStub,
            final long realTimeTopologyContextId,
            @Nonnull final PriceTableStore priceTableStore,
            @Nonnull final ReservedInstanceSpecStore reservedInstanceSpecStore) {
        this.reservedInstanceBoughtStore =
                Objects.requireNonNull(reservedInstanceBoughtStore);
        this.entityReservedInstanceMappingStore =
                Objects.requireNonNull(entityReservedInstanceMappingStore);
        this.repositoryClient = repositoryClient;
        this.supplyChainServiceBlockingStub = supplyChainServiceBlockingStub;
        this.realtimeTopologyContextId = realTimeTopologyContextId;
        this.priceTableStore = Objects.requireNonNull(priceTableStore);
        this.reservedInstanceSpecStore = Objects.requireNonNull(reservedInstanceSpecStore);
    }

    @Override
    public void getReservedInstanceBoughtByFilter(
            GetReservedInstanceBoughtByFilterRequest request,
            StreamObserver<GetReservedInstanceBoughtByFilterResponse> responseObserver) {
        try {
            final List<Long> scopeOids = request.getScopeSeedOidsList();
            final int scopeEntityType = request.getScopeEntityType();
            Map<EntityType, Set<Long>> cloudScopesTuple = repositoryClient
                            .getEntityOidsByType(scopeOids, realtimeTopologyContextId,
                                                 this.supplyChainServiceBlockingStub);
            final List<ReservedInstanceBought> reservedInstancesBought =
                           reservedInstanceBoughtStore
                               .getReservedInstanceBoughtByFilter(ReservedInstanceBoughtFilter
                                                                  .newBuilder()
                                  .addAllScopeId(scopeOids)
                                  .setScopeEntityType(Optional.of(scopeEntityType))
                                  .setCloudScopesTuple(cloudScopesTuple)
                                  .build());
            final List<Long> riOids = reservedInstancesBought.stream()
                    .map(ReservedInstanceBought::getId).collect(Collectors.toList());
            final Set<Long> riSpecIds = reservedInstancesBought.stream()
                    .map(riBought -> riBought.getReservedInstanceBoughtInfo()
                            .getReservedInstanceSpec()).collect(Collectors.toSet());
            final Stream<ReservedInstanceBought> rebuiltReservedInstanceBoughtStream =
                    stitchOnDemandComputeTierCost(stitchRICouponsUsed(reservedInstancesBought,
                            riOids), riSpecIds);

            final GetReservedInstanceBoughtByFilterResponse.Builder responseBuilder =
                    GetReservedInstanceBoughtByFilterResponse.newBuilder();
            rebuiltReservedInstanceBoughtStream.forEach(responseBuilder::addReservedInstanceBoughts);
            responseObserver.onNext(responseBuilder.build());
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

    private Stream<ReservedInstanceBought> stitchRICouponsUsed(
            List<ReservedInstanceBought> reservedInstancesBought, List<Long> scopeId) {
        final EntityReservedInstanceMappingFilter filter = EntityReservedInstanceMappingFilter
                .newBuilder().addAllScopeId(scopeId).build();
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
                            .getReservedInstanceBoughtCostBuilder()
                            .setOndemandCostPerHour(amount));

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
            final Optional<RegionFilter> regionFilter = request.hasRegionFilter()
                            ? Optional.of(request.getRegionFilter())
                            : Optional.empty();
                    final Optional<AvailabilityZoneFilter> azFilter = request.hasAvailabilityZoneFilter()
                            ? Optional.of(request.getAvailabilityZoneFilter())
                            : Optional.empty();
                    final Optional<AccountFilter> accountFilter = request.hasAccountFilter()
                            ? Optional.of(request.getAccountFilter())
                            : Optional.empty();
                    final ReservedInstanceBoughtFilter filter =
                            createReservedInstanceBoughtFilter(regionFilter, azFilter, accountFilter);
                    Map<Long, Long> reservedInstanceCountMap
                            = reservedInstanceBoughtStore.getReservedInstanceCountMap(filter);
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

            final Optional<RegionFilter> regionFilter = request.hasRegionFilter()
                                                        ? Optional.of(request.getRegionFilter())
                                                        : Optional.empty();
            final Optional<AvailabilityZoneFilter> azFilter = request.hasAvailabilityZoneFilter()
                                                ? Optional.of(request.getAvailabilityZoneFilter())
                                                : Optional.empty();
            final Optional<AccountFilter> accountFilter = request.hasAccountFilter()
                                                        ? Optional.of(request.getAccountFilter())
                                                        : Optional.empty();
            final ReservedInstanceBoughtFilter filter =
                            createReservedInstanceBoughtFilter(regionFilter, azFilter, accountFilter);

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

    private ReservedInstanceBoughtFilter createReservedInstanceBoughtFilter(
            @Nonnull final Optional<RegionFilter> regionFilter,
            @Nonnull final Optional<AvailabilityZoneFilter> azFilter,
            @Nonnull final Optional<AccountFilter> accountFilter) {
        final ReservedInstanceBoughtFilter.Builder filterBuilder = ReservedInstanceBoughtFilter
                        .newBuilder();
        if (regionFilter.isPresent()) {
            filterBuilder.addAllScopeId(regionFilter.get().getRegionIdList())
                        .setScopeEntityType(Optional.of(EntityType.REGION_VALUE));
            // Because region id is stored at RI spec table, it needs join operation.
            filterBuilder.setJoinWithSpecTable(true);
        } else if (azFilter.isPresent()) {
            filterBuilder.addAllScopeId(azFilter.get().getAvailabilityZoneIdList())
                        .setScopeEntityType(Optional.of(EntityType.AVAILABILITY_ZONE_VALUE));
        } else if (accountFilter.isPresent()) {
            filterBuilder.addAllScopeId(accountFilter.get().getAccountIdList())
                        .setScopeEntityType(Optional.of(EntityType.BUSINESS_ACCOUNT_VALUE));
        }
        return filterBuilder.build();
    }
}
