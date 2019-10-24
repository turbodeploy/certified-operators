package com.vmturbo.cost.component.reserved.instance;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.exception.DataAccessException;

import com.vmturbo.common.protobuf.cost.Cost.AccountFilter;
import com.vmturbo.common.protobuf.cost.Cost.AvailabilityZoneFilter;
import com.vmturbo.common.protobuf.cost.Cost.EntityReservedInstanceCoverage;
import com.vmturbo.common.protobuf.cost.Cost.GetEntityReservedInstanceCoverageRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetEntityReservedInstanceCoverageResponse;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceBoughtByFilterRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceBoughtByFilterResponse;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceBoughtCountByTemplateResponse;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceBoughtCountRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceBoughtCountResponse;
import com.vmturbo.common.protobuf.cost.Cost.RegionFilter;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.ReservedInstanceBoughtServiceGrpc.ReservedInstanceBoughtServiceImplBase;
import com.vmturbo.cost.component.reserved.instance.filter.EntityReservedInstanceMappingFilter;
import com.vmturbo.cost.component.reserved.instance.filter.ReservedInstanceBoughtFilter;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.repository.api.RepositoryClient;

public class ReservedInstanceBoughtRpcService extends ReservedInstanceBoughtServiceImplBase {

    private final Logger logger = LogManager.getLogger();

    private final ReservedInstanceBoughtStore reservedInstanceBoughtStore;

    private final EntityReservedInstanceMappingStore entityReservedInstanceMappingStore;

    private final RepositoryClient repositoryClient;

    private final Long realtimeTopologyContextId;

    public ReservedInstanceBoughtRpcService(
            @Nonnull final ReservedInstanceBoughtStore reservedInstanceBoughtStore,
            @Nonnull final EntityReservedInstanceMappingStore entityReservedInstanceMappingStore,
            @Nonnull final RepositoryClient repositoryClient,
            final Long realtimeTopologyContextId) {
        this.reservedInstanceBoughtStore =
                Objects.requireNonNull(reservedInstanceBoughtStore);
        this.entityReservedInstanceMappingStore =
                Objects.requireNonNull(entityReservedInstanceMappingStore);
        this.repositoryClient = Objects.requireNonNull(repositoryClient);
        this.realtimeTopologyContextId = realtimeTopologyContextId;
    }

    @Override
    public void getReservedInstanceBoughtByFilter(
            GetReservedInstanceBoughtByFilterRequest request,
            StreamObserver<GetReservedInstanceBoughtByFilterResponse> responseObserver) {
        try {
            final List<Long> scopeOids = request.getScopeSeedOidsList();
            final int scopeEntityType = request.getScopeEntityType();
            List<Long> relatedBusinessAccountOrSubscriptionOids = repositoryClient
                            .getRelatedBusinessAccountOrSubscriptionOids(scopeOids,
                                                         realtimeTopologyContextId, false);
            final List<ReservedInstanceBought> reservedInstanceBoughts =
                           reservedInstanceBoughtStore
                               .getReservedInstanceBoughtByFilter(ReservedInstanceBoughtFilter
                                                                  .newBuilder()
                                  .addAllScopeId(scopeOids)
                                  .setScopeEntityType(scopeEntityType)
                                  .addAllBillingAccountId(relatedBusinessAccountOrSubscriptionOids)
                                  .build());
            final List<Long> riOids = reservedInstanceBoughts.stream().map(riBought -> riBought.getId())
                            .collect(Collectors.toList());
            final Stream<ReservedInstanceBought> rebuiltReservedInstanceBoughtStream = stitchRICouponsUsed(reservedInstanceBoughts, riOids);

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

    private Stream<ReservedInstanceBought> stitchRICouponsUsed(List<ReservedInstanceBought> reservedInstanceBoughts, List<Long> scopeId) {
        final EntityReservedInstanceMappingFilter filter = EntityReservedInstanceMappingFilter.newBuilder().addAllScopeId(scopeId).build();
        final Map<Long, Double> reservedInstanceUsedCouponsMap =
                        entityReservedInstanceMappingStore.getReservedInstanceUsedCouponsMapByFilter(filter);
        return reservedInstanceBoughts.stream()
                        .map(ReservedInstanceBought::toBuilder)
                        .peek(riBuilder -> riBuilder
                                        .getReservedInstanceBoughtInfoBuilder()
                                        .getReservedInstanceBoughtCouponsBuilder()
                                        .setNumberOfCouponsUsed(
                                                        reservedInstanceUsedCouponsMap
                                                                        .getOrDefault(riBuilder.getId(), 0D)))
                        .map(ReservedInstanceBought.Builder::build);
    }

    @Override
    public void getReservedInstanceBoughtCount(
            GetReservedInstanceBoughtCountRequest request,
            StreamObserver<GetReservedInstanceBoughtCountResponse> responseObserver) {
        try {
            final Optional<RegionFilter> regionFilter = request.hasRegionFilter()
                            ? Optional.of(request.getRegionFilter())
                            : Optional.empty();
                    final Optional<AvailabilityZoneFilter> azFilter = request.hasRegionFilter()
                            ? Optional.of(request.getAvailabilityZoneFilter())
                            : Optional.empty();
                    final Optional<AccountFilter> accountFilter = request.hasRegionFilter()
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
            Map<String, Long> reservedInstanceCountMap = reservedInstanceBoughtStore.getReservedInstanceCountByRISpecIdMap();
            final GetReservedInstanceBoughtCountByTemplateResponse response =
                    GetReservedInstanceBoughtCountByTemplateResponse.newBuilder()
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
    public void getEntityReservedInstanceCoverage(GetEntityReservedInstanceCoverageRequest request,
              StreamObserver<GetEntityReservedInstanceCoverageResponse> responseObserver) {
        try {
            logger.debug("Request for Entity RI coverage: {}", request);
            final Map<Long, EntityReservedInstanceCoverage> retCoverage =
                    entityReservedInstanceMappingStore.getEntityRiCoverage();
            logger.debug("Retrieved and returning RI coverage for {} entities.", retCoverage.size());
            responseObserver.onNext(GetEntityReservedInstanceCoverageResponse.newBuilder()
                .putAllCoverageByEntityId(retCoverage)
                .build());
            responseObserver.onCompleted();
        } catch (DataAccessException e) {
            responseObserver.onError(Status.INTERNAL
                .withDescription("Failed to retrieve RI coverage from DB: " + e.getLocalizedMessage())
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
                        .setScopeEntityType(EntityType.REGION_VALUE);
            // Because region id is stored at RI spec table, it needs join operation.
            filterBuilder.setJoinWithSpecTable(true);
        } else if (azFilter.isPresent()) {
            filterBuilder.addAllScopeId(azFilter.get().getAvailabilityZoneIdList())
                        .setScopeEntityType(EntityType.AVAILABILITY_ZONE_VALUE);
        } else if (accountFilter.isPresent()) {
            filterBuilder.addAllScopeId(accountFilter.get().getAccountIdList())
                        .setScopeEntityType(EntityType.BUSINESS_ACCOUNT_VALUE);
        }
        return filterBuilder.build();
    }
}
