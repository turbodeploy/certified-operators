package com.vmturbo.cost.component.pricing;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.grpc.stub.StreamObserver;

import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.cost.Pricing.GetPriceTableChecksumRequest;
import com.vmturbo.common.protobuf.cost.Pricing.GetPriceTableChecksumResponse;
import com.vmturbo.common.protobuf.cost.Pricing.GetPriceTableRequest;
import com.vmturbo.common.protobuf.cost.Pricing.GetPriceTableResponse;
import com.vmturbo.common.protobuf.cost.Pricing.PriceTable;
import com.vmturbo.common.protobuf.cost.Pricing.ProbePriceTableSegment;
import com.vmturbo.common.protobuf.cost.Pricing.ProbePriceTableSegment.ProbePriceTableChunk;
import com.vmturbo.common.protobuf.cost.Pricing.ProbePriceTableSegment.ProbePriceTableHeader;
import com.vmturbo.common.protobuf.cost.Pricing.ProbePriceTableSegment.ProbeRISpecPriceChunk;
import com.vmturbo.common.protobuf.cost.Pricing.ReservedInstancePriceTable;
import com.vmturbo.common.protobuf.cost.Pricing.ReservedInstanceSpecPrice;
import com.vmturbo.common.protobuf.cost.Pricing.UploadPriceTablesResponse;
import com.vmturbo.common.protobuf.cost.PricingServiceGrpc.PricingServiceImplBase;
import com.vmturbo.cost.component.pricing.PriceTableStore.PriceTables;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceSpecStore;
import com.vmturbo.platform.sdk.common.PricingDTO.ReservedInstancePrice;

@ThreadSafe
public class PricingRpcService extends PricingServiceImplBase {
    private static final Logger logger = LogManager.getLogger();

    private final PriceTableStore priceTableStore;

    private final ReservedInstanceSpecStore reservedInstanceSpecStore;


    // track the hash of the current price table we are storing. Note that this needs to come from
    // the original upload request.
    private long lastConfirmedChecksum = 0;

    public PricingRpcService(@Nonnull final PriceTableStore priceTableStore,
                             @Nonnull final ReservedInstanceSpecStore riSpecStore) {
        this.priceTableStore = Objects.requireNonNull(priceTableStore);
        this.reservedInstanceSpecStore = Objects.requireNonNull(riSpecStore);
    }

    @Override
    public void getPriceTable(GetPriceTableRequest request,
                              StreamObserver<GetPriceTableResponse> responseObserver) {
        responseObserver.onNext(GetPriceTableResponse.newBuilder()
                .setGlobalPriceTable(priceTableStore.getMergedPriceTable())
                .build());
        responseObserver.onCompleted();
    }

    @Override
    public void getPriceTableChecksum(final GetPriceTableChecksumRequest request, final StreamObserver<GetPriceTableChecksumResponse> responseObserver) {
        responseObserver.onNext(GetPriceTableChecksumResponse.newBuilder()
                .setPriceTableChecksum(lastConfirmedChecksum).build());
        responseObserver.onCompleted();;
    }

    @Override
    public StreamObserver<ProbePriceTableSegment> updatePriceTables(final StreamObserver<UploadPriceTablesResponse> responseObserver) {
        // Create a stream observer that will process all of the messages sent from the client.
        return new StreamObserver<ProbePriceTableSegment>() {
            private long checksum;
            private long createdTime;
            private Map<String, ProbePriceData> priceDataForProbe = new HashMap<>();

            @Override
            public void onNext(final ProbePriceTableSegment probePriceTableSegment) {
                // handle the incoming message.
                if (probePriceTableSegment.hasHeader()) {
                    // get the header information
                    ProbePriceTableHeader header = probePriceTableSegment.getHeader();
                    checksum = header.getChecksum();
                    createdTime = header.getCreatedTime();
                    logger.info("Updating price tables created at {}.", createdTime);
                } else if (probePriceTableSegment.hasProbePriceTable()) {
                    // this segment contains a probe price table. Add it to the map.
                    ProbePriceTableChunk priceTableChunk = probePriceTableSegment.getProbePriceTable();
                    priceDataForProbe
                            .computeIfAbsent(priceTableChunk.getProbeType(), key -> new ProbePriceData())
                            .priceTable = priceTableChunk.getPriceTable();
                } else if (probePriceTableSegment.hasProbeRiSpecPrices()) {
                    ProbeRISpecPriceChunk riSpecPriceChunk = probePriceTableSegment.getProbeRiSpecPrices();
                    // add a block of RI Spec Prices to the RI Price list for the probe type.
                    priceDataForProbe
                            .computeIfAbsent(riSpecPriceChunk.getProbeType(), key -> new ProbePriceData())
                            .riSpecPrices.addAll(riSpecPriceChunk.getReservedInstanceSpecPricesList());
                } else {
                    // unrecognized or empty segment.
                    logger.warn("Unrecognized Probe Price Table segment type.");
                }
            }

            @Override
            public void onError(final Throwable throwable) {
                logger.error("Error while receiving price table updates.", throwable);
            }

            @Override
            public void onCompleted() {
                // finalize the update.
                logger.debug("Saving updated probe price tables.");
                Map<String, PriceTables> tablesByProbeType = new HashMap<>();
                priceDataForProbe.forEach((probeType, probePriceData) -> {
                    ReservedInstancePriceTable riPriceTable = updateRISpecsAndBuildRIPriceTable(probePriceData.riSpecPrices);
                    tablesByProbeType.put(probeType, new PriceTables(probePriceData.priceTable, riPriceTable));
                });
                priceTableStore.putProbePriceTables(tablesByProbeType);
                lastConfirmedChecksum = checksum;
                responseObserver.onNext(UploadPriceTablesResponse.getDefaultInstance());
                responseObserver.onCompleted();
            }
        };
    }

    /**
     * Given a list of {@link ReservedInstanceSpecPrice}s, create an {@link ReservedInstancePriceTable} from the
     * included RI Spec and Price data. This method will also update the RI Spec list to account
     * for any new RI Specs discovered as part of this price table.
     *
     * @param specPrices the list of RI Spec Prices to include in the table.
     * @return A ReservedInstancePriceTable based on the specs and prices.
     */
    private ReservedInstancePriceTable updateRISpecsAndBuildRIPriceTable(List<ReservedInstanceSpecPrice> specPrices) {
        if (CollectionUtils.isEmpty(specPrices)) {
            return ReservedInstancePriceTable.getDefaultInstance();
        }
        // this will hold the set of RI specs to save, with id's set to index position.
        List<ReservedInstanceSpec> riSpecsToSave = new ArrayList<>();
        // this oddish map of ri prices -> ri spec temp id is used to facilitate building of
        // the ri price table
        Map<ReservedInstancePrice, Long> riPricesToTempId = new HashMap<>();
        for (int x = 0 ; x < specPrices.size() ; x++) {
            ReservedInstanceSpecPrice riSpecPrice = specPrices.get(x);
            ReservedInstanceSpec riSpec = ReservedInstanceSpec.newBuilder()
                    .setReservedInstanceSpecInfo(riSpecPrice.getRiSpecInfo())
                    .setId(x)
                    .build();
            riSpecsToSave.add(riSpec);
            riPricesToTempId.put(riSpecPrice.getPrice(), riSpec.getId());
        }
        // update the persisted RI specs and keep the map of our temp id's -> real id's,
        // which we will reference in the RI Price Table.
        Map<Long, Long> tempSpecIdToRealId = reservedInstanceSpecStore.updateReservedInstanceBoughtSpec(riSpecsToSave);

        // create the map of real spec id -> RI Prices that we will use to create the RI
        // price table with.
        Map<Long, ReservedInstancePrice> riSpecPrices = new HashMap<>();
        riPricesToTempId.forEach((price, tempId) -> {
            // don't add null keys
            if (tempSpecIdToRealId.containsKey(tempId)) {
                riSpecPrices.put(tempSpecIdToRealId.get(tempId), price );
            } else {
                logger.warn("Skipping RI spec price with temp id {} but no real id assigned.", tempId);
            }
        });

        // now create the RI Price Table
        ReservedInstancePriceTable.Builder riPriceTableBuilder = ReservedInstancePriceTable.newBuilder();
        riPriceTableBuilder.putAllRiPricesBySpecId(riSpecPrices);
        return riPriceTableBuilder.build();
    }

    // helper object for holding a probe's price table and ri spect prices.
    private static class ProbePriceData {
        public PriceTable priceTable;
        public List<ReservedInstanceSpecPrice> riSpecPrices = new ArrayList<>();
    }
}
