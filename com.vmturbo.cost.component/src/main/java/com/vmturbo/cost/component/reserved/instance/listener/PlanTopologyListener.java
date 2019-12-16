package com.vmturbo.cost.component.reserved.instance.listener;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;

import javax.annotation.Nonnull;

import io.grpc.stub.StreamObserver;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.cost.Cost.AccountFilter;
import com.vmturbo.common.protobuf.cost.Cost.AvailabilityZoneFilter;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceBoughtByFilterRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceBoughtByFilterResponse;
import com.vmturbo.common.protobuf.cost.Cost.RegionFilter;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.communication.chunking.RemoteIterator;
import com.vmturbo.cost.component.reserved.instance.PlanReservedInstanceStore;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceBoughtRpcService;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceBoughtStore;
import com.vmturbo.market.component.api.PlanAnalysisTopologyListener;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Listen for updates to the plan topology and record them to the DB.
 **/
public class PlanTopologyListener implements PlanAnalysisTopologyListener {
    private static final Map<EntityType, BiConsumer<List<Long>, GetReservedInstanceBoughtByFilterRequest.Builder>> ENTITY_TYPE_TO_FILTER_CREATOR;

    static {
        Map<EntityType, BiConsumer<List<Long>, GetReservedInstanceBoughtByFilterRequest.Builder>> result = new HashMap<>();
        result.put(EntityType.BUSINESS_ACCOUNT, (oids, builder) -> {
            builder.setAccountFilter(
                            AccountFilter.newBuilder()
                                            .addAllAccountId(oids)
                                            .build());

        });
        result.put(EntityType.AVAILABILITY_ZONE, (oids, builder) -> {
            builder.setZoneFilter(
                            AvailabilityZoneFilter.newBuilder()
                                            .addAllAvailabilityZoneId(oids)
                                            .build());

        });
        result.put(EntityType.REGION, (oids, builder) -> {
            builder.setRegionFilter(
                            RegionFilter.newBuilder()
                                            .addAllRegionId(oids)
                                            .build());

        });
        ENTITY_TYPE_TO_FILTER_CREATOR = Collections.unmodifiableMap(result);
    }

    private final Logger logger = LogManager.getLogger();

    // the database access utility classes for the History-related RDB tables
    private final PlanReservedInstanceStore planReservedInstanceStore;

    private final ReservedInstanceBoughtRpcService reservedInstanceBoughtService;

    /**
     * Creates {@link PlanTopologyListener} instance.
     *
     * @param planReservedInstanceStore plan RI store.
     * @param reservedInstanceBoughtService RI bought service.
     */
    public PlanTopologyListener(
                    @Nonnull final PlanReservedInstanceStore planReservedInstanceStore,
                    @Nonnull final ReservedInstanceBoughtRpcService reservedInstanceBoughtService) {
        this.planReservedInstanceStore = Objects.requireNonNull(planReservedInstanceStore);
        this.reservedInstanceBoughtService = Objects.requireNonNull(reservedInstanceBoughtService);
    }

    /**
     * Receive a new Plan Analysis Topology and call the {@link ReservedInstanceBoughtStore} to
     * get all available RIs for the topology. {@link PlanReservedInstanceStore} store RIs in the cost DB.
     *
     * @param topologyInfo topology info
     * @param topologyDTOs topology DTOs
     */
    @Override
    public void onPlanAnalysisTopology(final TopologyInfo topologyInfo,
        @Nonnull final RemoteIterator<TopologyDTO.Topology.DataSegment> topologyDTOs) {
        final List<Long> scopedSeedOids = topologyInfo.getScopeSeedOidsList();
        final long topologyContextId = topologyInfo.getTopologyContextId();
        final GetReservedInstanceBoughtByFilterRequest.Builder requestBuilder =
                        GetReservedInstanceBoughtByFilterRequest.newBuilder();
        final BiConsumer<List<Long>, GetReservedInstanceBoughtByFilterRequest.Builder> filterCreator =
                        ENTITY_TYPE_TO_FILTER_CREATOR.getOrDefault(EntityType.forNumber(topologyInfo.getScopeEntityType()),
                                        (oids, builder) -> {
                                            // No filter.
                                        });
        filterCreator.accept(scopedSeedOids, requestBuilder);
        final ReservedInstanceBoughtStreamObserver responseObserver = new ReservedInstanceBoughtStreamObserver();
        // Getting bought RIs from grpc service allow to get data with RI utilization.
        reservedInstanceBoughtService.getReservedInstanceBoughtByFilter(requestBuilder.build(), responseObserver);
        final List<ReservedInstanceBought> allReservedInstancesBought = responseObserver.getReservedInstanceBougtList();
        planReservedInstanceStore.insertPlanReservedInstanceBought(allReservedInstancesBought,
                        topologyContextId);
    }

    /**
     * Stream observer implementation to get bought reserved instances.
     */
    private class ReservedInstanceBoughtStreamObserver
                    implements StreamObserver<GetReservedInstanceBoughtByFilterResponse> {
        private final List<ReservedInstanceBought> reservedInstanceBougtList = new ArrayList<>();

        @Override
        public void onNext(GetReservedInstanceBoughtByFilterResponse value) {
            reservedInstanceBougtList.addAll(value.getReservedInstanceBoughtsList());
        }

        @Override
        public void onError(Throwable t) {
            logger.error("Error during RIs bought request", t);
        }

        @Override
        public void onCompleted() {
            // Nothing to do.
        }

        /**
         * Returns bought reserved instances list.
         *
         * @return list of {@link ReservedInstanceBought}.
         */
        public List<ReservedInstanceBought> getReservedInstanceBougtList() {
            return reservedInstanceBougtList;
        }
    }

}

