package com.vmturbo.market.rpc;

import java.util.Objects;

import javax.annotation.Nonnull;

import com.google.common.collect.Table;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.market.InitialPlacement.DeleteInitialPlacementBuyerRequest;
import com.vmturbo.common.protobuf.market.InitialPlacement.DeleteInitialPlacementBuyerResponse;
import com.vmturbo.common.protobuf.market.InitialPlacement.FindInitialPlacementRequest;
import com.vmturbo.common.protobuf.market.InitialPlacement.FindInitialPlacementResponse;
import com.vmturbo.common.protobuf.market.InitialPlacement.GetProvidersOfExistingReservationsRequest;
import com.vmturbo.common.protobuf.market.InitialPlacement.GetProvidersOfExistingReservationsResponse;
import com.vmturbo.common.protobuf.market.InitialPlacement.InitialPlacementBuyerPlacementInfo;
import com.vmturbo.common.protobuf.market.InitialPlacement.InitialPlacementFailure;
import com.vmturbo.common.protobuf.market.InitialPlacement.InitialPlacementSuccess;
import com.vmturbo.common.protobuf.market.InitialPlacement.UpdateHistoricalCachedEconomyRequest;
import com.vmturbo.common.protobuf.market.InitialPlacement.UpdateHistoricalCachedEconomyResponse;
import com.vmturbo.common.protobuf.market.InitialPlacementServiceGrpc.InitialPlacementServiceImplBase;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.UnplacementReason;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.UnplacementReason.FailedResources;
import com.vmturbo.market.reservations.InitialPlacementFinder;
import com.vmturbo.market.reservations.InitialPlacementFinderResult;
import com.vmturbo.market.reservations.InitialPlacementFinderResult.FailureInfo;

/**
 * Implementation of gRpc service for Reservation.
 */
public class InitialPlacementRpcService extends InitialPlacementServiceImplBase {
    private final Logger logger = LogManager.getLogger();

    /**
     * prefix for initial placement log messages.
     */
    private final String logPrefix = "FindInitialPlacement: ";


    private final InitialPlacementFinder initPlacementFinder;

    /**
     * The grpc call to handle fast reservation.
     *
     * @param initialPlacementFinder {@link InitialPlacementFinder}
     */
    public InitialPlacementRpcService(@Nonnull final InitialPlacementFinder initialPlacementFinder) {
        this.initPlacementFinder = Objects.requireNonNull(initialPlacementFinder);
    }

    @Override
    public void getProvidersOfExistingReservations(final GetProvidersOfExistingReservationsRequest request,
                                                           final StreamObserver<GetProvidersOfExistingReservationsResponse> responseObserver) {
        GetProvidersOfExistingReservationsResponse response =
                initPlacementFinder.buildGetProvidersOfExistingReservationsResponse();
        try {
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(Status.INTERNAL
                    .withDescription("Failed to create ProvidersOfExistingReservations.")
                    .asException());
        }
        return;
    }

    @Override
    public void findInitialPlacement(final FindInitialPlacementRequest request,
                                     final StreamObserver<FindInitialPlacementResponse> responseObserver) {
        request.getInitialPlacementList().forEach(initialPlacement -> {
            logger.info("{} The number of workloads for reservation {} is {} with mode {} "
                + "and grouping {}", logPrefix, initialPlacement.getId(), initialPlacement.getInitialPlacementBuyerCount(),
                initialPlacement.getReservationMode(), initialPlacement.getReservationGrouping());
        });
        Table<Long, Long, InitialPlacementFinderResult> result = initPlacementFinder
                .findPlacement(request);
        FindInitialPlacementResponse.Builder response = FindInitialPlacementResponse.newBuilder();
        for (Table.Cell<Long, Long, InitialPlacementFinderResult> triplet : result.cellSet()) {
            InitialPlacementBuyerPlacementInfo.Builder builder = InitialPlacementBuyerPlacementInfo
                    .newBuilder()
                    .setBuyerId(triplet.getRowKey())
                    .setCommoditiesBoughtFromProviderId(triplet.getColumnKey());
            // InitialPlacementFinderResult provider oid exist means placement succeeded
            InitialPlacementFinderResult reservationResult = triplet.getValue();
            if (reservationResult.getProviderOid().isPresent()) {
                InitialPlacementSuccess.Builder successBuilder = InitialPlacementSuccess.newBuilder()
                        .setProviderOid(reservationResult.getProviderOid().get());
                if (reservationResult.getClusterComm().isPresent()) {
                    successBuilder.setCluster(reservationResult.getClusterComm().get());
                }
                builder.setInitialPlacementSuccess(successBuilder);
            } else {
                InitialPlacementFailure.Builder failureBuilder = InitialPlacementFailure.newBuilder();
                for (FailureInfo info: triplet.getValue().getFailureInfoList()) {
                    CommodityType commodityType = info.getCommodityType();
                    UnplacementReason reason = UnplacementReason.newBuilder()
                            .addFailedResources(FailedResources.newBuilder().setCommType(commodityType)
                                    .setRequestedAmount(info.getRequestedAmount())
                                    .setMaxAvailable(info.getMaxQuantity()).build())
                            .setClosestSeller(info.getClosestSellerOid())
                            .build();
                    failureBuilder.addUnplacedReason(reason);
                }
                builder.setInitialPlacementFailure(failureBuilder);
            }
            response.addInitialPlacementBuyerPlacementInfo(builder.build());
        }
        try {
            responseObserver.onNext(response.build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(Status.INTERNAL
                    .withDescription("Failed to create reservation.")
                    .asException());
        }
        return;
    }

    @Override
    public void deleteInitialPlacementBuyer(final DeleteInitialPlacementBuyerRequest request,
                                            final StreamObserver<DeleteInitialPlacementBuyerResponse> responseObserver) {
        logger.info(logPrefix + "The number of workloads to delete is " + request.getBuyerIdList().size());
        boolean deployed = request.hasDeployed() && request.getDeployed();
        boolean remove = initPlacementFinder.buyersToBeDeleted(request.getBuyerIdList(), deployed);
        DeleteInitialPlacementBuyerResponse.Builder response = DeleteInitialPlacementBuyerResponse
                .newBuilder().setResult(remove);
        try {
            responseObserver.onNext(response.build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(Status.INTERNAL
                    .withDescription("Failed to delete reservation.")
                    .asException());
        }
        return;

    }

    @Override
    public void updateHistoricalCachedEconomy(final UpdateHistoricalCachedEconomyRequest request,
                                      final StreamObserver<UpdateHistoricalCachedEconomyResponse> responseObserver) {
        logger.info(logPrefix + "Received a request to update historical cache from Plan Orchestrator");
        initPlacementFinder.resetHistoricalCacheReceived();
        UpdateHistoricalCachedEconomyResponse.Builder response = UpdateHistoricalCachedEconomyResponse
                .newBuilder();
        try {
            responseObserver.onNext(response.build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(Status.INTERNAL
                    .withDescription("Failed to update historical cached economy.")
                    .asException());
        }
        return;
    }

}