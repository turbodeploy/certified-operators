package com.vmturbo.cost.component.pricing;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

import io.grpc.stub.StreamObserver;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpecInfo;
import com.vmturbo.common.protobuf.cost.Pricing.GetAccountPriceTableRequest;
import com.vmturbo.common.protobuf.cost.Pricing.GetAccountPriceTableResponse;
import com.vmturbo.common.protobuf.cost.Pricing.GetPriceTableChecksumRequest;
import com.vmturbo.common.protobuf.cost.Pricing.GetPriceTableChecksumResponse;
import com.vmturbo.common.protobuf.cost.Pricing.GetPriceTableChecksumResponse.Builder;
import com.vmturbo.common.protobuf.cost.Pricing.GetPriceTableRequest;
import com.vmturbo.common.protobuf.cost.Pricing.GetPriceTableResponse;
import com.vmturbo.common.protobuf.cost.Pricing.GetPriceTablesRequest;
import com.vmturbo.common.protobuf.cost.Pricing.GetPriceTablesResponse;
import com.vmturbo.common.protobuf.cost.Pricing.OnDemandPriceTable;
import com.vmturbo.common.protobuf.cost.Pricing.PriceTable;
import com.vmturbo.common.protobuf.cost.Pricing.PriceTableChecksum;
import com.vmturbo.common.protobuf.cost.Pricing.PriceTableChunk;
import com.vmturbo.common.protobuf.cost.Pricing.PriceTableChunk.OnDemandLicensePriceEntrySegment;
import com.vmturbo.common.protobuf.cost.Pricing.PriceTableChunk.OnDemandPriceTableByRegionSegment;
import com.vmturbo.common.protobuf.cost.Pricing.PriceTableChunk.ReservedLicenseSegment;
import com.vmturbo.common.protobuf.cost.Pricing.PriceTableChunk.SpotInstancePriceTableByRegionSegment;
import com.vmturbo.common.protobuf.cost.Pricing.PriceTableKey;
import com.vmturbo.common.protobuf.cost.Pricing.ProbePriceTableSegment;
import com.vmturbo.common.protobuf.cost.Pricing.ProbePriceTableSegment.ProbePriceTableChunk;
import com.vmturbo.common.protobuf.cost.Pricing.ProbePriceTableSegment.ProbePriceTableHeader;
import com.vmturbo.common.protobuf.cost.Pricing.ProbePriceTableSegment.ProbeRISpecPriceChunk;
import com.vmturbo.common.protobuf.cost.Pricing.ReservedInstancePriceTable;
import com.vmturbo.common.protobuf.cost.Pricing.ReservedInstanceSpecPrice;
import com.vmturbo.common.protobuf.cost.Pricing.SpotInstancePriceTable;
import com.vmturbo.common.protobuf.cost.Pricing.UploadAccountPriceTableKeyRequest;
import com.vmturbo.common.protobuf.cost.Pricing.UploadAccountPriceTableKeysResponse;
import com.vmturbo.common.protobuf.cost.Pricing.UploadPriceTablesResponse;
import com.vmturbo.common.protobuf.cost.PricingServiceGrpc.PricingServiceImplBase;
import com.vmturbo.components.common.utils.TriFunction;
import com.vmturbo.cost.component.CostComponentGlobalConfig;
import com.vmturbo.cost.component.pricing.PriceTableStore.PriceTables;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceBoughtStore;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceSpecCleanup;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceSpecStore;
import com.vmturbo.platform.sdk.common.PricingDTO.LicenseOverrides;
import com.vmturbo.platform.sdk.common.PricingDTO.LicensePriceEntry;
import com.vmturbo.platform.sdk.common.PricingDTO.ReservedInstancePrice;

/**
 * This class provides RPC service for the PriceTableStore.
 */
@ThreadSafe
public class PricingRpcService extends PricingServiceImplBase {
    private static final Logger logger = LogManager.getLogger();

    private final PriceTableStore priceTableStore;
    private final ReservedInstanceSpecStore reservedInstanceSpecStore;
    private final BusinessAccountPriceTableKeyStore businessAccountPriceTableKeyStore;
    private final ReservedInstanceBoughtStore reservedInstanceBoughtStore;
    private final ReservedInstanceSpecCleanup reservedInstanceSpecCleanup;
    private final CostComponentGlobalConfig costComponentGlobalConfig;
    private final Long priceTableSegmentSizeLimitBytes;

    /**
     * Constructor.
     * @param priceTableStore price table store.
     * @param riSpecStore reserved instance spec store.
     * @param reservedInstanceBoughtStore reserved Instance bought store.
     * @param businessAccountPriceTableKeyStore business account to price table key store.
     * @param reservedInstanceSpecCleanup for periodically checking for unreferenced RI specs.
     * @param costComponentGlobalConfig used for referring to global clock().
     * @param priceTableSegmentSizeLimitBytes max size of priceTableSegments.
     */
    public PricingRpcService(@Nonnull final PriceTableStore priceTableStore,
                             @Nonnull final ReservedInstanceSpecStore riSpecStore,
                             @Nonnull final ReservedInstanceBoughtStore reservedInstanceBoughtStore,
                             @Nonnull final BusinessAccountPriceTableKeyStore businessAccountPriceTableKeyStore,
                             @Nonnull final ReservedInstanceSpecCleanup reservedInstanceSpecCleanup,
                             @Nonnull final CostComponentGlobalConfig costComponentGlobalConfig,
                             @Nonnull final Long priceTableSegmentSizeLimitBytes) {
        this.priceTableStore = Objects.requireNonNull(priceTableStore);
        this.reservedInstanceSpecStore = Objects.requireNonNull(riSpecStore);
        this.reservedInstanceBoughtStore = Objects.requireNonNull(reservedInstanceBoughtStore);
        this.businessAccountPriceTableKeyStore = Objects.requireNonNull(businessAccountPriceTableKeyStore);
        this.reservedInstanceSpecCleanup = Objects.requireNonNull(reservedInstanceSpecCleanup);
        this.costComponentGlobalConfig = Objects.requireNonNull(costComponentGlobalConfig);
        this.priceTableSegmentSizeLimitBytes = Objects.requireNonNull(priceTableSegmentSizeLimitBytes);
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
    public void getPriceTables(GetPriceTablesRequest request, StreamObserver<GetPriceTablesResponse> responseObserver) {
        final Map<Long, PriceTable> priceTables = priceTableStore.getPriceTables(request.getOidList());
        for (Entry<Long, PriceTable> priceTableEntry : priceTables.entrySet()) {
            final long priceTableOid = priceTableEntry.getKey();
            final PriceTable priceTable = priceTableEntry.getValue();
            final long totalSerializedSize = priceTable.getSerializedSize();
            sendPriceTableSegmentsByRegionIds(responseObserver, priceTable.getOnDemandPriceByRegionIdMap(), priceTableOid, totalSerializedSize,
                    this::addOnDemandPriceTableToPriceTableChunk);
            sendPriceTableSegmentsByRegionIds(responseObserver, priceTable.getSpotPriceByZoneOrRegionIdMap(), priceTableOid, totalSerializedSize,
                    this::addSpotInstancePriceTableToPriceTableChunk);
            sendPriceTableSegmentsReservedLicensePricesList(responseObserver, priceTable.getReservedLicensePricesList(),
                    priceTable.getReservedLicenseOverridesMap(),
                    priceTableOid, totalSerializedSize);
            sendPriceTableSegmentsOnDemandLicensePricesList(responseObserver, priceTable.getOnDemandLicensePricesList(),
                    priceTable.getOnDemandLicenseOverridesMap(),
                    priceTableOid, totalSerializedSize);
        }
        responseObserver.onCompleted();
    }

    @Override
    public void getPriceTableChecksum(final GetPriceTableChecksumRequest request,
                                      final StreamObserver<GetPriceTableChecksumResponse> responseObserver) {
        Map<PriceTableKey, Long> priceTableKeyLongMap =
                priceTableStore.getChecksumByPriceTableKeys(request.getPriceTableKeyList());
        Builder builder = GetPriceTableChecksumResponse.newBuilder();
        for (Entry<PriceTableKey, Long> priceTableKeyLongEntry : priceTableKeyLongMap.entrySet()) {
            builder.addPricetableToChecksum(PriceTableChecksum.newBuilder()
                    .setCheckSum(priceTableKeyLongEntry.getValue())
                    .setPriceTableKey(priceTableKeyLongEntry.getKey()));
        }
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    @Override
    public StreamObserver<ProbePriceTableSegment> updatePriceTables(final StreamObserver<UploadPriceTablesResponse> responseObserver) {
        // Create a stream observer that will process all of the messages sent from the client.
        return new StreamObserver<ProbePriceTableSegment>() {
            private long createdTime;
            private Map<PriceTableKey, ProbePriceData> priceDataForProbe = new HashMap<>();

            @Override
            public void onNext(final ProbePriceTableSegment probePriceTableSegment) {
                // handle the incoming message.
                Long checkSum = null;
                if (probePriceTableSegment.hasHeader()) {
                    // get the header information
                    ProbePriceTableHeader header = probePriceTableSegment.getHeader();
                    Optional<PriceTableChecksum> checkSumField = header.getPriceTableChecksumsList().stream().findAny();
                    if (checkSumField.isPresent()) {
                        checkSum = checkSumField.get().getCheckSum();
                    }
                    createdTime = header.getCreatedTime();
                    logger.info("Updating pricetables created at {} (key={}).", createdTime);
                }
                // this segment contains a probe price table and should also contain a checksum. Add it to the map.
                if (probePriceTableSegment.hasProbePriceTable()) {
                    if (checkSum == null) {
                        logger.error("Checksum not received from Topology processor. This will cause issues in" +
                                " the functioning of the cost component.");
                    }
                    ProbePriceTableChunk priceTableChunk = probePriceTableSegment.getProbePriceTable();
                    priceDataForProbe
                            .computeIfAbsent(priceTableChunk.getPriceTableKey(), key -> new ProbePriceData())
                            .setPriceTable(priceTableChunk.getPriceTable())
                            .setChecksum(checkSum);
                } else if (probePriceTableSegment.hasProbeRiSpecPrices()) {
                    ProbeRISpecPriceChunk riSpecPriceChunk = probePriceTableSegment.getProbeRiSpecPrices();
                    // add a block of RI Spec Prices to the RI Price list for the probe type.
                    priceDataForProbe
                            .computeIfAbsent(riSpecPriceChunk.getPriceTableKey(), key -> new ProbePriceData())
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
                logger.info("Saving updated probe price tables.");
                Map<PriceTableKey, PriceTables> tablesByProbeType = new HashMap<>();
                priceDataForProbe.forEach((priceTableKey, probePriceData) -> {
                    ReservedInstancePriceTable riPriceTable =
                            updateRISpecsAndBuildRIPriceTable(probePriceData.riSpecPrices);
                    reservedInstanceBoughtStore.updateRIBoughtFromRIPriceList(ImmutableMap.copyOf(
                            riPriceTable.getRiPricesBySpecIdMap()));
                    logger.info("Saving price table with price table key {} and checksum {}",
                            priceTableKey, probePriceData.checksum);
                    tablesByProbeType.put(
                            priceTableKey,
                            new PriceTables(probePriceData.priceTable,
                                    riPriceTable, probePriceData.checksum));
                });
                priceTableStore.putProbePriceTables(tablesByProbeType);
                responseObserver.onNext(UploadPriceTablesResponse.getDefaultInstance());
                responseObserver.onCompleted();

                // after the response has completed
                reservedInstanceSpecCleanup.cleanupUnreferencedRISpecs();
            }
        };
    }

    @Override
    public void uploadAccountPriceTableKeys(
            final UploadAccountPriceTableKeyRequest request, final StreamObserver<UploadAccountPriceTableKeysResponse> responseObserver) {
        UploadAccountPriceTableKeysResponse.Builder builder = UploadAccountPriceTableKeysResponse.newBuilder();

        businessAccountPriceTableKeyStore.uploadBusinessAccount(request.getBusinessAccountPriceTableKey());
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void getAccountPriceTable(final GetAccountPriceTableRequest request,
                                         final StreamObserver<GetAccountPriceTableResponse> responseStreamObserver) {
        GetAccountPriceTableResponse.Builder builder = GetAccountPriceTableResponse.newBuilder();

        Map<Long, Long> priceTableKeyMap = businessAccountPriceTableKeyStore
                .fetchPriceTableKeyOidsByBusinessAccount(Sets.newHashSet(request.getBusinessAccountOidList()));

        builder.putAllBusinessAccountPriceTableKey(priceTableKeyMap);

        responseStreamObserver.onNext(builder.build());
        responseStreamObserver.onCompleted();
    }

    /**
     * Given a list of {@link ReservedInstanceSpecPrice}s, create an {@link ReservedInstancePriceTable} from the
     * included RI Spec and Price data. This method will also update the RI Spec list to account
     * for any new RI Specs discovered as part of this price table.
     *
     * @param specPrices the list of RI Spec Prices to include in the table.
     * @return A ReservedInstancePriceTable based on the specs and prices.
     */
    @VisibleForTesting
    ReservedInstancePriceTable updateRISpecsAndBuildRIPriceTable(List<ReservedInstanceSpecPrice> specPrices) {
        if (CollectionUtils.isEmpty(specPrices)) {
            return ReservedInstancePriceTable.getDefaultInstance();
        }
        // this will hold the set of RI specs to save, with id's set to index position.
        List<ReservedInstanceSpec> riSpecsToSave = new ArrayList<>();

        // The ReservedInstanceSpecInfo is unique, whereas ReservedInstancePrice is not.
        // Auxiliary maps to generate riSpecPrices map: ReservedInstanceMap.OID -> ReservedInstancePrice.
        Map<ReservedInstanceSpecInfo, ReservedInstancePrice> infoToPrice = new HashMap<>();
        Map<ReservedInstanceSpecInfo, Long> infoToTempId = new HashMap<>();
        for (int x = 0; x < specPrices.size(); x++) {
            ReservedInstanceSpecPrice riSpecPrice = specPrices.get(x);
            ReservedInstanceSpec riSpec = ReservedInstanceSpec.newBuilder()
                    .setReservedInstanceSpecInfo(riSpecPrice.getRiSpecInfo())
                    .setId(x)
                    .build();
            riSpecsToSave.add(riSpec);
            infoToPrice.put(riSpecPrice.getRiSpecInfo(), riSpecPrice.getPrice());
            infoToTempId.put(riSpecPrice.getRiSpecInfo(), riSpec.getId());
        }
        // update the persisted RI specs and keep the map of our temp id's -> real id's,
        // which we will reference in the RI Price Table.
        Map<Long, Long> tempSpecIdToRealId = reservedInstanceSpecStore.updateReservedInstanceSpec(riSpecsToSave);

        // create the map of real spec id -> RI Prices that we will use to create the RI
        // price table with.
        Map<Long, ReservedInstancePrice> riSpecPrices = new HashMap<>();
        infoToTempId.forEach((info, tempId) -> {
            // don't add null keys
            if (tempSpecIdToRealId.containsKey(tempId)) {

                ReservedInstancePrice price = infoToPrice.get(info);
                riSpecPrices.put(tempSpecIdToRealId.get(tempId), price );
            } else {
                logger.warn("Skipping RI spec price with temp id {} but no real id assigned.", tempId);
            }
        });
        logger.debug("updateRISpecsAndBuildRIPriceTable: input:List<ReservedInstanceSpecPrice>.size={} infoToTempId.size={} output:ReservedInstancePriceTable.size={}",
            specPrices.size(), infoToTempId.size(), riSpecPrices.size());

        if (riSpecPrices.size() != specPrices.size()) {
            logger.error("updateRISpecsAndBuildRIPriceTable: input:List<ReservedInstanceSpecPrice>.size={} != output:ReservedInstanceSpecPrice.size={}, some RIs dropped!",
                specPrices.size(), riSpecPrices.size());
        }
        // now create the RI Price Table
        ReservedInstancePriceTable.Builder riPriceTableBuilder = ReservedInstancePriceTable.newBuilder();
        riPriceTableBuilder.putAllRiPricesBySpecId(riSpecPrices);
        return riPriceTableBuilder.build();
    }

    private <T> void sendPriceTableSegmentsByRegionIds(
            @Nonnull final StreamObserver<GetPriceTablesResponse> responseObserver,
            @Nonnull final Map<Long, T> priceTableMap,
            final long priceTableOid,
            final long totalSerializedSize,
            @Nonnull TriFunction<T, Long, PriceTableChunk.Builder, PriceTableChunk> functionApplier) {
        final Collection<PriceTableChunk> priceTableChunks = new ArrayList<>();
        long serializedSize = 0L;
        for (Entry<Long, T> priceTableEntry : priceTableMap.entrySet()) {
            PriceTableChunk.Builder priceTableChunkBuilder = PriceTableChunk.newBuilder()
                    .setCreatedTime(costComponentGlobalConfig.clock().millis())
                    .setPriceTableOid(priceTableOid);
            PriceTableChunk priceTableChunk = functionApplier.apply(
                    priceTableEntry.getValue(),
                    priceTableEntry.getKey(),
                    priceTableChunkBuilder);
            long currentSerializedSize = priceTableChunk.getSerializedSize();
            if (currentSerializedSize + serializedSize > priceTableSegmentSizeLimitBytes) {
                // send priceTableChunk now.
                sendPriceTableChunk(responseObserver, totalSerializedSize, priceTableChunks);
                serializedSize = currentSerializedSize;
                priceTableChunks.clear();
            } else {
                serializedSize += currentSerializedSize;
                priceTableChunks.add(priceTableChunk);
            }
        }
        if (!priceTableChunks.isEmpty()) {
            sendPriceTableChunk(responseObserver, totalSerializedSize, priceTableChunks);
        }
    }

    private PriceTableChunk addOnDemandPriceTableToPriceTableChunk(
            @Nonnull final OnDemandPriceTable onDemandPriceTable,
            @Nonnull final Long regionId,
            @Nonnull final PriceTableChunk.Builder builder) {
        return builder.setOndemandPriceTableByRegion(OnDemandPriceTableByRegionSegment.newBuilder()
                .setOnDemandPriceTable(onDemandPriceTable)
                .setRegionId(regionId)
                .build()).build();
    }

    private PriceTableChunk addSpotInstancePriceTableToPriceTableChunk(
            @Nonnull final SpotInstancePriceTable spotInstancePriceTable,
            @Nonnull Long regionId,
            @Nonnull final PriceTableChunk.Builder builder) {
        return builder.setSpotInstancePriceTableByRegion(SpotInstancePriceTableByRegionSegment.newBuilder()
                .setSpotInstancePriceTable(spotInstancePriceTable)
                .setRegionId(regionId)
                .build()).build();
    }

    private void sendPriceTableSegmentsReservedLicensePricesList(@Nonnull final StreamObserver<GetPriceTablesResponse> responseObserver,
                                                                 @Nonnull final List<LicensePriceEntry> reservedLicensePricesList,
                                                                 @Nonnull final Map<Long, LicenseOverrides> reservedLicenseOverridesMap,
                                                                 final long priceTableOid,
                                                                 final long serializedSize) {
        final PriceTableChunk priceTableChunk = PriceTableChunk.newBuilder()
                .setCreatedTime(costComponentGlobalConfig.clock().millis())
                .setPriceTableOid(priceTableOid)
                .setReservedLicenseSegment(ReservedLicenseSegment.newBuilder()
                        .addAllReservedLicensePriceEntry(reservedLicensePricesList)
                        .putAllReservedLicenseOverrides(reservedLicenseOverridesMap)
                        .build())
                .build();
        sendPriceTableChunk(responseObserver, serializedSize, Collections.singleton(priceTableChunk));
    }

    private void sendPriceTableSegmentsOnDemandLicensePricesList(@Nonnull final StreamObserver<GetPriceTablesResponse> responseObserver,
                                                                 @Nonnull final List<LicensePriceEntry> onDemandLicensePricesList,
                                                                 @Nonnull final Map<Long, LicenseOverrides> onDemandLicenseOverridesMap,
                                                                 final long priceTableOid,
                                                                 final long serializedSize) {
        final PriceTableChunk priceTableChunk = PriceTableChunk.newBuilder()
                .setCreatedTime(costComponentGlobalConfig.clock().millis())
                .setPriceTableOid(priceTableOid)
                .setOndemandLicensePriceEntrySegment(OnDemandLicensePriceEntrySegment.newBuilder()
                        .addAllLicensePriceEntry(onDemandLicensePricesList)
                        .putAllOnDemandLicenseOverrides(onDemandLicenseOverridesMap)
                        .build())
                .build();
        sendPriceTableChunk(responseObserver, serializedSize, Collections.singleton(priceTableChunk));
    }

    private void sendPriceTableChunk(@Nonnull final StreamObserver<GetPriceTablesResponse> responseObserver,
                                     final long serializedSize,
                                     @Nonnull final Collection<PriceTableChunk> priceTableChunks) {
            GetPriceTablesResponse getPriceTableResponse = GetPriceTablesResponse.newBuilder()
                    .setPriceTableSerializedSize(serializedSize)
                    .addAllPriceTableChunk(priceTableChunks).build();
            responseObserver.onNext(getPriceTableResponse);
    }


    /**
     * helper object for holding a probe's price table and ri spect prices.
     */
    private static class ProbePriceData {
        public Long checksum;
        public PriceTable priceTable;
        public List<ReservedInstanceSpecPrice> riSpecPrices = new ArrayList<>();

        /**
         * Setter for the checksum.
         *
         * @param checksum The checksum to be set
         */
        public void setChecksum(Long checksum) {
            this.checksum = checksum;
        }

        /**
         * Sets the price table and returns the instance.
         *
         * @param priceTable The price table.
         * @return This instance.
         */
        public ProbePriceData setPriceTable(PriceTable priceTable) {
            this.priceTable = priceTable;
            return this;
        }
    }
}
