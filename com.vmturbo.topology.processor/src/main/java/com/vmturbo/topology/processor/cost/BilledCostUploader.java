package com.vmturbo.topology.processor.cost;

import static com.vmturbo.topology.processor.cost.DiscoveredCloudCostUploader.BILLED_COST_SECTION;
import static com.vmturbo.topology.processor.cost.DiscoveredCloudCostUploader.CLOUD_COST_UPLOAD_TIME;
import static com.vmturbo.topology.processor.cost.DiscoveredCloudCostUploader.UPLOAD_REQUEST_UPLOAD_STAGE;
import static com.vmturbo.topology.processor.cost.DiscoveredCloudCostUploader.getBillingId;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;

import io.grpc.stub.StreamObserver;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.cost.BilledCostUploadServiceGrpc.BilledCostUploadServiceStub;
import com.vmturbo.common.protobuf.cost.Cost.UploadBilledCostRequest;
import com.vmturbo.common.protobuf.cost.Cost.UploadBilledCostRequest.BillingDataPoint;
import com.vmturbo.common.protobuf.cost.Cost.UploadBilledCostRequest.CostTagGroupMap;
import com.vmturbo.common.protobuf.cost.Cost.UploadBilledCostResponse;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CostBilling.CloudBillingData;
import com.vmturbo.platform.sdk.common.CostBilling.CloudBillingData.CloudBillingBucket;
import com.vmturbo.platform.sdk.common.CostBilling.CloudBillingData.CloudBillingBucket.Granularity;
import com.vmturbo.platform.sdk.common.CostBilling.CloudBillingDataPoint;
import com.vmturbo.platform.sdk.common.util.Units;
import com.vmturbo.proactivesupport.DataMetricTimer;
import com.vmturbo.topology.processor.stitching.StitchingContext;

/**
 * Upload billed cost discovered by cloud billing targets, to the cost component.
 */
public class BilledCostUploader {
    private static final Logger logger = LogManager.getLogger();

    private BilledCostUploadServiceStub billServiceClient;

    /**
     * a cache of TargetBillingData received per target.
     * When Topology Processor receives billed data discovered from a billing target, TargetBillingData will be
     * populated in the cache for the target. And after billed data is uploaded to Cost component, the cache will
     * be cleared for the successfully uploaded targets.
     */
    private final Map<Long, TargetBillingData> targetBillingDataCache = Collections.synchronizedMap(new HashMap<>());

    /**
     * Max size of each UploadBilledCostRequest. Must be >= 0.
     * By default 4 MB - gRPC limit per incoming message.
     */
    private final double maximumUploadBilledCostRequestByteSize;

    /**
     * Extra space allocated for building upload request.
     */
    private static final long REQUEST_BYTE_ALLOCATED = (long)(0.07 * Units.MBYTE);

    /**
     * Constructor for BilledCostUploader.
     *
     * @param billServiceClient async client
     * @param maximumUploadBilledCostRequestSizeMB maximum size for each request object
     */
    public BilledCostUploader(BilledCostUploadServiceStub billServiceClient,
            float maximumUploadBilledCostRequestSizeMB) {
        this.billServiceClient = billServiceClient;
        if (maximumUploadBilledCostRequestSizeMB < 0) {
            throw new IllegalArgumentException("maximumUploadBilledCostRequestSizeMB cannot be less than 0.");
        }
        this.maximumUploadBilledCostRequestByteSize = maximumUploadBilledCostRequestSizeMB * Units.MBYTE;
    }

    /**
     * Record cloud billing data for certain target in the targetBillingDataCache.
     *
     * @param targetId target id
     * @param cloudBillingDataList discovered cloud billing data for the target
     */
    public void recordCloudBillingData(final long targetId, @Nonnull final List<CloudBillingData> cloudBillingDataList) {
        if (!cloudBillingDataList.isEmpty()) {
            logger.debug("Received new CloudBillingData for target {}", targetId);
            targetBillingDataCache.put(targetId, new TargetBillingData(targetId, cloudBillingDataList));
        }
    }

    /**
     * Create billed cost upload requests and upload to cost component.
     *
     * @param topologyInfo topology info
     * @param stitchingContext stitching context
     * @param cloudEntitiesMap cloud entities map from local id to oid
     */
    public void uploadBilledCost(TopologyInfo topologyInfo,
            StitchingContext stitchingContext, CloudEntitiesMap cloudEntitiesMap) {
        if (targetBillingDataCache.isEmpty()) {
            logger.debug("No CloudBillingData to be uploaded.");
            return;
        }
        // Record of the TargetBillingData timestamp that are processed in current upload cycle.
        final Map<Long, Long> processedCacheTimestampPerTarget = new HashMap<>();
        // Build BilledCostDataWrappers -
        // 1. Each CloudBillingData DTO is mapped to one BilledCostDataWrapper.
        // 2. For each CloudBillingDataPoint, replace accountId, cloudServiceId, regionId and entityId to oid if present.
        final List<BilledCostDataWrapper> billedCostDataWrappers
                = buildBilledCostDataWrappers(stitchingContext, cloudEntitiesMap, processedCacheTimestampPerTarget);
        // Build UploadBilledCostRequests, and upload to server
        try (DataMetricTimer uploadTimer = CLOUD_COST_UPLOAD_TIME.labels(
                BILLED_COST_SECTION, UPLOAD_REQUEST_UPLOAD_STAGE).startTimer()) {
            logger.info("Start uploading billed cost for {} CloudBillingData DTOs", billedCostDataWrappers.size());
            final UploadBilledCostResponseStreamObserver responseObserver = new UploadBilledCostResponseStreamObserver();
            final BilledCostDataSender sender = new BilledCostDataSender(billServiceClient.uploadBilledCost(responseObserver));
            sender.sendBilledCostData(billedCostDataWrappers, topologyInfo.getTopologyId());
            responseObserver.waitLatchToFinish();
            uploadTimer.observe();
            logger.info("Billed cost upload took {} secs", uploadTimer.getTimeElapsedSecs());
            // If upload is successful, remove the processed TargetBillingData from cache.
            // Note that if newly discovered TargetBillingData(with a different timestamp) is present
            // for certain target in the cache, the newly discovered TargetBillingData will be kept.
            // Only TargetBillingData objects with timestamps in processedCacheTimestampPerTarget will
            // be cleared from targetBillingDataCache.
            if (responseObserver.isUploadSuccessful()) {
                synchronized (targetBillingDataCache) {
                    processedCacheTimestampPerTarget.forEach((targetId, timestamp) -> {
                        final TargetBillingData latestCache = targetBillingDataCache.get(targetId);
                        if (latestCache != null && latestCache.getCachedTimestampUTC() == timestamp) {
                            targetBillingDataCache.remove(targetId);
                        }
                    });
                }
            }
        } catch (RuntimeException e1) {
            logger.error("Error uploading billed cost", e1);
        } catch (InterruptedException e2) {
            logger.error("InterruptedException during waiting for uploading billed cost response", e2);
            Thread.currentThread().interrupt();
        }
    }

    @VisibleForTesting
    protected List<BilledCostDataWrapper> buildBilledCostDataWrappers(StitchingContext stitchingContext,
            CloudEntitiesMap cloudEntitiesMap, Map<Long, Long> processedCacheTimestampPerTarget) {
        final Map<String, Long> entityBillingIdToOid = getEntityBillingIdToOidMap(stitchingContext);
        final List<BilledCostDataWrapper> billedCostDataWrappers = new ArrayList<>();
        synchronized (targetBillingDataCache) {
            targetBillingDataCache.forEach((targetId, targetBillingData) -> {
                final List<CloudBillingData> cloudBillingDataList = targetBillingData.getCloudBillingDataList();
                cloudBillingDataList.forEach(cloudBillingData -> billedCostDataWrappers.add(
                        createBilledCostDataWrapper(cloudBillingData, cloudEntitiesMap,
                                entityBillingIdToOid, targetId)));
                // Record the processed targetBillingData timestamp in processedCacheTimestampPerTarget.
                processedCacheTimestampPerTarget.put(targetId, targetBillingData.getCachedTimestampUTC());
            });
        }
        return billedCostDataWrappers;
    }

    private BilledCostDataWrapper createBilledCostDataWrapper(CloudBillingData cloudBillingData,
            final CloudEntitiesMap cloudEntitiesMap, final Map<String, Long> entityBillingIdToOid, final Long targetId) {
        // cost tag group by id
        final Map<Long, CostTagGroupMap> costTagGroupMapById = cloudBillingData.getCostTagGroupMapMap().entrySet().stream()
                .collect(Collectors.toMap(Entry::getKey, entry -> CostTagGroupMap.newBuilder()
                        .setGroupId(entry.getKey())
                        .setTags(entry.getValue())
                        .build()));
        // samples by cost tag group id
        final List<CloudBillingBucket> cloudBillingBuckets = cloudBillingData.getCloudCostBucketsList();
        final Map<Long, List<BillingDataPoint>> dataPointsByCostTagGroupId = new HashMap<>();
        for (CloudBillingBucket bucket : cloudBillingBuckets) {
            final long timestamp = bucket.getTimestampUtcMillis();
            final List<BillingDataPoint> dataPoints = bucket.getSamplesList().stream()
                    .map(cloudBillingDataPoint -> createBillingDataPoint(cloudEntitiesMap, entityBillingIdToOid,
                            cloudBillingDataPoint, timestamp, targetId))
                    .collect(Collectors.toList());
            dataPoints.forEach(d -> {
                if (d.hasCostTagGroupId()) {
                    dataPointsByCostTagGroupId.computeIfAbsent(d.getCostTagGroupId(), k -> new ArrayList<>()).add(d);
                } else {
                    dataPointsByCostTagGroupId.computeIfAbsent(null, k -> new ArrayList<>()).add(d);
                }
            });
        }
        final Map<CostTagGroupMap, List<BillingDataPoint>> samplesByCostTagGroupMap = new HashMap<>();
        dataPointsByCostTagGroupId.entrySet().forEach(entry -> {
            CostTagGroupMap costTagGroupMap = costTagGroupMapById.get(entry.getKey());
            samplesByCostTagGroupMap.put(costTagGroupMap, entry.getValue());
        });
        final String billingIdentifier = cloudBillingData.hasBillingIdentifier() ? cloudBillingData.getBillingIdentifier() : null;
        final Granularity granularity = cloudBillingBuckets.stream()
                .filter(CloudBillingBucket::hasGranularity)
                .map(CloudBillingBucket::getGranularity)
                .findAny().orElse(null);
        return new BilledCostDataWrapper(billingIdentifier, granularity, samplesByCostTagGroupMap);
    }

    /**
     * Create BillingDataPoint based on CloudBillingDataPoint. Set oids corresponding to accountId, cloudServiceId,
     * regionId and entityId, replacing localId/billingId with oid.
     *
     * @param cloudEntitiesMap cloud entities map from local id to oid
     * @param entityBillingIdToOid VM/DB/Volume billing id to oid map
     * @param originalDataPoint CloudBillingDataPoint
     * @param timestamp timestamp for the billing data point
     * @param targetId target id
     * @return BillingDataPoint
     */
    private BillingDataPoint createBillingDataPoint(CloudEntitiesMap cloudEntitiesMap,
            Map<String, Long> entityBillingIdToOid, CloudBillingDataPoint originalDataPoint,
            long timestamp, Long targetId) {
        final BillingDataPoint.Builder billingDp = BillingDataPoint.newBuilder();
        billingDp.setTimestampUtcMillis(timestamp);
        // account oid
        if (originalDataPoint.hasAccountId()) {
            final String localAccountId = originalDataPoint.getAccountId();
            final Long accountOid = cloudEntitiesMap.getOrDefault(localAccountId, cloudEntitiesMap.getFallbackAccountOid(targetId));
            if (!cloudEntitiesMap.containsKey(localAccountId)) {
                logger.warn(
                        "Couldn't find business account oid for local id {}, using fallback account {}.",
                        localAccountId, accountOid);
            }
            billingDp.setAccountOid(accountOid);
        } else {
            logger.error("Required field missing - CloudBillingDataPoint doesn't have accountId.");
        }
        // cloud service oid
        if (originalDataPoint.hasCloudServiceId()) {
            Long cloudServiceOid = cloudEntitiesMap.get(originalDataPoint.getCloudServiceId());
            if (cloudServiceOid == null) {
                logger.warn("Oid not found for cloud service {}, using fallback oid as 0",
                        originalDataPoint.getCloudServiceId());
                cloudServiceOid = 0L;
            }
            billingDp.setCloudServiceOid(cloudServiceOid);
        } else {
            logger.error("Required field missing - CloudBillingDataPoint doesn't have cloudServiceId.");
        }
        // region oid
        if (originalDataPoint.hasRegionId()) {
            Long regionOid = cloudEntitiesMap.get(originalDataPoint.getRegionId());
            if (regionOid == null) {
                logger.warn("Oid not found for region {}, using fallback oid as 0.", originalDataPoint.getRegionId());
                regionOid = 0L;
            }
            billingDp.setRegionOid(regionOid);
        }
        // entity type
        if (originalDataPoint.hasEntityType()) {
            billingDp.setEntityType(originalDataPoint.getEntityType());
        }
        // entity oid
        if (originalDataPoint.hasEntityId()) {
            Long entityOid = entityBillingIdToOid.get(originalDataPoint.getEntityId());
            if (entityOid == null) {
                // This is valid scenario. Some entities were present in the past but got deleted later.
                logger.debug("Oid not found for entity {}, using fallback oid as 0.", originalDataPoint.getEntityId());
                entityOid = 0L;
            }
            billingDp.setEntityOid(entityOid);
        }
        // price model
        if (originalDataPoint.hasPriceModel()) {
            billingDp.setPriceModel(originalDataPoint.getPriceModel());
        }
        // cost category
        if (originalDataPoint.hasCostCategory()) {
            billingDp.setCostCategory(originalDataPoint.getCostCategory());
        }
        // usage amount
        if (originalDataPoint.hasUsageAmount()) {
            billingDp.setUsageAmount(originalDataPoint.getUsageAmount());
        }
        // cost
        if (originalDataPoint.hasCost()) {
            billingDp.setCost(originalDataPoint.getCost());
        }
        // cost tag group id
        if (originalDataPoint.hasCostTagGroupId()) {
            billingDp.setCostTagGroupId(originalDataPoint.getCostTagGroupId());
        }
        return billingDp.build();
    }

    private Map<String, Long> getEntityBillingIdToOidMap(StitchingContext stitchingContext) {
        final Map<String, Long> entityBillingIdToOid = new HashMap<>();
        logger.debug("Building entity billing id to oid map");
        stitchingContext.getEntitiesOfType(EntityType.VIRTUAL_MACHINE)
                .forEach(vm -> entityBillingIdToOid.put(getBillingId(vm), vm.getOid()));
        stitchingContext.getEntitiesOfType(EntityType.VIRTUAL_VOLUME)
                .forEach(volume -> entityBillingIdToOid.put(getBillingId(volume), volume.getOid()));
        stitchingContext.getEntitiesOfType(EntityType.DATABASE)
                .forEach(db -> entityBillingIdToOid.put(getBillingId(db), db.getOid()));
        stitchingContext.getEntitiesOfType(EntityType.DATABASE_SERVER)
                .forEach(dbs -> entityBillingIdToOid.put(getBillingId(dbs), dbs.getOid()));
        return entityBillingIdToOid;
    }

    /**
     * StreamObserver for UploadBilledCostResponse.
     */
    public class UploadBilledCostResponseStreamObserver implements StreamObserver<UploadBilledCostResponse> {
        // Streaming client call makes async request, so use a latch to block until the
        // request is complete.
        private CountDownLatch latch;
        // Flag will be set to true when upload successfully finishes.
        private Boolean uploadSuccessful;

        /**
         * Constructor for UploadBilledCostResponseStreamObserver.
         */
        UploadBilledCostResponseStreamObserver() {
            latch = new CountDownLatch(1);
            uploadSuccessful = null;
        }

        @Override
        public void onNext(UploadBilledCostResponse response) {}

        @Override
        public void onError(Throwable throwable) {
            logger.error("Failed to upload billed cost - {}", throwable.getMessage());
            latch.countDown();
            uploadSuccessful = Boolean.FALSE;
        }

        @Override
        public void onCompleted() {
            latch.countDown();
            uploadSuccessful = Boolean.TRUE;
        }

        /**
         * Whether upload has successfully finished.
         *
         * @return true if upload has successfully finished.
         */
        public boolean isUploadSuccessful() {
            return Boolean.TRUE.equals(uploadSuccessful);
        }

        /**
         * Block until get a response or an exception occurs.
         *
         * @throws InterruptedException if the current thread is interrupted while waiting.
         */
        public void waitLatchToFinish() throws InterruptedException {
            latch.await();
        }
    }

    /**
     * Caches CloudBillingData for a target.
     */
    private static class TargetBillingData {
        private long targetId;

        private List<CloudBillingData> cloudBillingDataList;

        // timestamp when TargetBillingData object is created.
        private long cachedTimestampUTC;

        private TargetBillingData(long targetId, List<CloudBillingData> cloudBillingDataList) {
            this.targetId = targetId;
            this.cloudBillingDataList = cloudBillingDataList;
            this.cachedTimestampUTC = System.currentTimeMillis();
        }

        private long getTargetId() {
            return targetId;
        }

        private long getCachedTimestampUTC() {
            return cachedTimestampUTC;
        }

        @Nonnull
        private List<CloudBillingData> getCloudBillingDataList() {
            return cloudBillingDataList != null ? cloudBillingDataList : new ArrayList<>();
        }
    }

    /**
     * When a target is removed, clear its cached CloudBillingData.
     *
     * @param targetId id for the target to be removed.
     */
    public void targetRemoved(final long targetId) {
        if (targetBillingDataCache.containsKey(targetId)) {
            logger.info("Clearing cached CloudBillingData for target {}", targetId);
            targetBillingDataCache.remove(targetId);
        }
    }

    /**
     * Intermediate wrapper that is converted from CloudBillingData, and will be converted to upload requests.
     */
    protected class BilledCostDataWrapper {
        protected final String billingIdentifier;
        protected final Granularity granularity;
        protected final Map<CostTagGroupMap, List<BillingDataPoint>> samplesByCostTagGroupMap;

        protected BilledCostDataWrapper(String billingIdentifier, Granularity granularity,
                Map<CostTagGroupMap, List<BillingDataPoint>> samplesByCostTagGroupMap) {
            this.billingIdentifier = billingIdentifier;
            this.granularity = granularity;
            this.samplesByCostTagGroupMap = (samplesByCostTagGroupMap != null ? samplesByCostTagGroupMap : new HashMap<>());
        }
    }

    /**
     * Helper class to send billed cost over a GRPC stream.
     */
    private class BilledCostDataSender {
        private final StreamObserver<UploadBilledCostRequest> requestObserver;
        private long totalBytesSent = 0;
        private long numRequestsSent = 0;

        private BilledCostDataSender(StreamObserver<UploadBilledCostRequest> requestObserver) {
            this.requestObserver = requestObserver;
        }

        private void sendBilledCostData(List<BilledCostDataWrapper> billedCostDataWrappers, long topologyId) {
            for (BilledCostDataWrapper billedCostDataWrapper : billedCostDataWrappers) {
                UploadBilledCostRequest.Builder requestBuilder = initRequestBuilder(billedCostDataWrapper, topologyId);
                long currentByte = REQUEST_BYTE_ALLOCATED;
                for (Map.Entry<CostTagGroupMap, List<BillingDataPoint>> entry : billedCostDataWrapper.samplesByCostTagGroupMap.entrySet()) {
                    final CostTagGroupMap costTagGroupMap = entry.getKey();
                    final List<BillingDataPoint> dataPoints = entry.getValue();
                    final long costTagGroupMapByteSize = (costTagGroupMap == null) ? 0 : costTagGroupMap.getSerializedSize();
                    final long dataPointsByteSize = dataPoints.stream().map(d -> (long)d.getSerializedSize()).reduce(Long::sum).orElse(0L);
                    final long pairByteSize = costTagGroupMapByteSize + dataPointsByteSize;
                    if (pairByteSize >= maximumUploadBilledCostRequestByteSize) {
                        sendLargeAmountDataPoints(costTagGroupMap, dataPoints, dataPointsByteSize, billedCostDataWrapper, topologyId);
                    } else if (pairByteSize + currentByte < maximumUploadBilledCostRequestByteSize) {
                        if (costTagGroupMap != null) {
                            requestBuilder.addCostTagGroupMap(costTagGroupMap);
                        }
                        requestBuilder.addAllSamples(dataPoints);
                        currentByte += pairByteSize;
                    } else {
                        sendRequest(requestBuilder.build());
                        requestBuilder = initRequestBuilder(billedCostDataWrapper, topologyId);
                        if (costTagGroupMap != null) {
                            requestBuilder.addCostTagGroupMap(costTagGroupMap);
                        }
                        requestBuilder.addAllSamples(dataPoints);
                        currentByte = REQUEST_BYTE_ALLOCATED + pairByteSize;
                    }
                }
                sendRequest(requestBuilder.build());
            }
            requestObserver.onCompleted();
            logger.info("Sent {} total bytes over {} requests.", totalBytesSent, numRequestsSent);
        }

        /**
         * For a pair of costTagGroupMap and billingDataPoints, if size is larger than
         * maximumUploadBilledCostRequestByteSize, the data points will be divided to different upload requests.
         * 1. Calculate requests number with formula -
         *    Math.ceil((dataPointsByteSize + REQUEST_BYTE_ALLOCATED) / maximumUploadBilledCostRequestByteSize)
         *    REQUEST_BYTE_ALLOCATED here will allocate a bit extra space when dataPointsByteSize is close to
         *    maximumUploadBilledCostRequestByteSize. For example, when dataPointsByteSize is 3.98MB or 7.98MB,
         *    without extra allocation data points will be sent by 1 and 2 requests accordingly. With the
         *    extra space introduced, requests number will increase to 2 and 3.
         *    Why extra space is needed - Serialized size for each data point is not fixed, where some data points
         *    are larger in size than other ones. Introducing extra space can help each request to stay within
         *    maximumUploadBilledCostRequestByteSize limit, even when large data points gather together in one request.
         * 2. Calculate average data points per request with formula -
         *    Math.ceil(dataPointsNumber / requestsNumber)
         * 3. Split data points to separate requests, and upload the requests.
         *
         * @param costTagGroupMap costTagGroupMap
         * @param dataPoints billingDataPoints related to the costTagGroupMap
         * @param dataPointsByteSize total byte size for data points
         * @param billedCostDataWrapper billedCostDataWrapper to initialize request
         * @param topologyId topologyId to initialize request
         */
        private void sendLargeAmountDataPoints(@Nullable CostTagGroupMap costTagGroupMap, List<BillingDataPoint> dataPoints,
                long dataPointsByteSize, BilledCostDataWrapper billedCostDataWrapper, long topologyId) {
            final double requestsNumber
                    = Math.ceil((dataPointsByteSize + REQUEST_BYTE_ALLOCATED) / maximumUploadBilledCostRequestByteSize);
            final int dataPointsCountPerRequest = (int)Math.ceil(dataPoints.size() / requestsNumber);
            List<List<BillingDataPoint>> dataPointsGroups = Lists.partition(dataPoints, dataPointsCountPerRequest);
            dataPointsGroups.forEach(dataPointsGroup -> {
                final UploadBilledCostRequest.Builder requestBuilder = initRequestBuilder(billedCostDataWrapper, topologyId)
                        .addAllSamples(dataPointsGroup)
                        .setAtomicRequest(false);
                if (costTagGroupMap != null) {
                    requestBuilder.addCostTagGroupMap(costTagGroupMap);
                }
                sendRequest(requestBuilder.build());
            });
        }

        private UploadBilledCostRequest.Builder initRequestBuilder(BilledCostDataWrapper billedCostDataWrapper,
                long topologyId) {
            UploadBilledCostRequest.Builder requestBuilder = UploadBilledCostRequest.newBuilder()
                    .setTopologyId(topologyId);
            final String billingIdentifier = billedCostDataWrapper.billingIdentifier;
            if (billingIdentifier != null) {
                requestBuilder.setBillingIdentifier(billingIdentifier);
            }
            final Granularity granularity = billedCostDataWrapper.granularity;
            if (granularity != null) {
                requestBuilder.setGranularity(granularity);
            }
            requestBuilder.setCreatedTime(System.currentTimeMillis());
            return requestBuilder;
        }

        private void sendRequest(UploadBilledCostRequest request) {
            numRequestsSent++;
            totalBytesSent += request.getSerializedSize();
            requestObserver.onNext(request);
        }
    }
}
