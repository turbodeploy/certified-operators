package com.vmturbo.cost.component.pricing;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.grpc.stub.StreamObserver;

import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.cost.Pricing.GetPriceTableChecksumRequest;
import com.vmturbo.common.protobuf.cost.Pricing.GetPriceTableChecksumResponse;
import com.vmturbo.common.protobuf.cost.Pricing.GetPriceTableRequest;
import com.vmturbo.common.protobuf.cost.Pricing.GetPriceTableResponse;
import com.vmturbo.common.protobuf.cost.Pricing.ProbePriceTable;
import com.vmturbo.common.protobuf.cost.Pricing.ProbePriceTable.ReservedInstanceSpecPrice;
import com.vmturbo.common.protobuf.cost.Pricing.ReservedInstancePriceTable;
import com.vmturbo.common.protobuf.cost.Pricing.UploadPriceTablesRequest;
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
    public void uploadPriceTables(UploadPriceTablesRequest request,
                                 StreamObserver<UploadPriceTablesResponse> responseObserver) {

        // we need to save the RI Specs and assemble the RI price table before we can save it.
        Map<String, PriceTables> tablesByProbeType = new HashMap<>();
        request.getProbePriceTablesMap().forEach((probeType, probePriceTable) -> {
            ReservedInstancePriceTable riPriceTable = updateRISpecsAndBuildRIPriceTable(probePriceTable);
            tablesByProbeType.put(probeType, new PriceTables(probePriceTable.getPriceTable(), riPriceTable));
        });

        priceTableStore.putProbePriceTables(tablesByProbeType);
        lastConfirmedChecksum = request.getChecksum();
        responseObserver.onNext(UploadPriceTablesResponse.newBuilder()
                .build());
        responseObserver.onCompleted();
    }

    /**
     * Given a {@link ProbePriceTable}, create an {@link ReservedInstancePriceTable} from the
     * included RI Spec and Price data. This method will also update the RI Spec list to account
     * for any new RI Specs discovered as part of this price table.
     *
     * @param table the probe price table to pull the RI specs and prices from
     * @return A ReservedInstancePriceTable based on the specs and prices.
     */
    private ReservedInstancePriceTable updateRISpecsAndBuildRIPriceTable(ProbePriceTable table) {
        // this will hold the set of RI specs to save, with id's set to index position.
        List<ReservedInstanceSpec> riSpecsToSave = new ArrayList<>();
        // this oddish map of ri prices -> ri spec temp id is used to facilitate building of
        // the ri price table
        Map<ReservedInstancePrice, Long> riPricesToTempId = new HashMap<>();
        List<ReservedInstanceSpecPrice> specPrices = table.getReservedInstanceSpecPricesList();
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
}
