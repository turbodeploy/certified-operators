package com.vmturbo.topology.processor.cost;

import static com.vmturbo.topology.processor.cost.DiscoveredCloudCostUploader.getBillingId;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.SetMultimap;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.protobuf.AbstractMessage;

import io.grpc.stub.StreamObserver;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import com.vmturbo.common.protobuf.cost.BilledCost.BilledCostBucket;
import com.vmturbo.common.protobuf.cost.BilledCost.BilledCostBucket.BilledCostBucketKey;
import com.vmturbo.common.protobuf.cost.BilledCost.BilledCostData;
import com.vmturbo.common.protobuf.cost.BilledCost.BilledCostItem;
import com.vmturbo.common.protobuf.cost.BilledCostServiceGrpc.BilledCostServiceStub;
import com.vmturbo.common.protobuf.cost.BilledCostServices.UploadBilledCloudCostRequest;
import com.vmturbo.common.protobuf.cost.BilledCostServices.UploadBilledCloudCostRequest.BilledCostSegment;
import com.vmturbo.common.protobuf.cost.BilledCostServices.UploadBilledCloudCostRequest.BilledCostSegment.Builder;
import com.vmturbo.common.protobuf.cost.BilledCostServices.UploadBilledCloudCostRequest.CostTagsSegment;
import com.vmturbo.common.protobuf.cost.BilledCostServices.UploadBilledCloudCostRequest.MetadataSegment;
import com.vmturbo.common.protobuf.cost.BilledCostServices.UploadBilledCloudCostResponse;
import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.components.common.diagnostics.DiagnosticsAppender;
import com.vmturbo.components.common.diagnostics.DiagnosticsException;
import com.vmturbo.components.common.diagnostics.DiagsRestorable;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CostBilling.CloudBillingData;
import com.vmturbo.platform.sdk.common.CostBilling.CloudBillingData.CloudBillingBucket;
import com.vmturbo.platform.sdk.common.CostBilling.CloudBillingDataPoint;
import com.vmturbo.platform.sdk.common.CostBilling.CostTagGroup;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.topology.processor.cost.util.MessageChunker;
import com.vmturbo.topology.processor.cost.util.ProtobufUtils;
import com.vmturbo.topology.processor.stitching.StitchingContext;

/**
 * Queues and uploads {@link CloudBillingData} discovered by mediation targets in batches to the
 * cost component via RPC, where it is persisted into the partitioned billed cost tables.
 *
 * <p>A typical workflow is: {@link com.vmturbo.topology.processor.operation.OperationManager}
 * enqueues billing data from targets as they return it as part of discovery. Later, when the live
 * topology pipeline executes, the {@link com.vmturbo.topology.processor.topology.pipeline.Stages.UploadBilledCloudCostDataStage}
 * stage triggers an upload of the entire queue to the cost component. We can infer that the cost
 * component will then persist the data that it receives into its database.
 */
public class BilledCloudCostUploader implements DiagsRestorable<Void> {

    private static final Logger logger = LogManager.getLogger(BilledCloudCostUploader.class);

    /**
     * The smallest possible upload request is about 4 bytes, but we can't realistically break
     * billing data down into chunks that small, so we enforce it being at least as high as the size
     * of an empty cost bucket. Be wary of setting maxRequestSizeBytes too low in spite of this
     * guard rail, or individual cost tag groups or billing items may not fit into a request.
     */
    private static final int MINIMUM_UPLOAD_REQUEST_SIZE_BYTES =
            ProtobufUtils.getMaximumMessageSizeExcludingRepeatedFields(
                    BilledCostBucket.getDescriptor());

    /**
     * Keep track of all currently active targets and their type. Useful for constructing a
     * {@link CloudEntitiesMap} which we use for OID resolution.
     */
    private final Map<Long, SDKProbeType> activeTargetProbeTypes = new ConcurrentHashMap<>();

    /**
     * As mediation targets discover billing data, it ends up here in a queueing state while we
     * wait for the next topology broadcast. When the broadcast occurs, we'll try to convert and
     * upload this cost data to the cost component.
     */
    @GuardedBy("targetBillingDataUploadQueueLock")
    private final SetMultimap<Long, CloudBillingData> targetBillingDataUploadQueue =
            HashMultimap.create();

    private final ReadWriteLock targetBillingDataUploadQueueLock = new ReentrantReadWriteLock();

    private final BilledCostServiceStub billedCostService;

    private final long maxRequestSizeBytes;

    private final int uploadTimeoutSeconds;

    /**
     * Constructor for {@link BilledCloudCostUploader}.
     *
     * @param billedCostService billed cost RPC service
     * @param maxRequestSizeBytes upper limit on the size of a single upload request
     * @param uploadTimeoutSeconds upload requests will time out after this many seconds
     */
    public BilledCloudCostUploader(@Nonnull final BilledCostServiceStub billedCostService,
            final long maxRequestSizeBytes, final int uploadTimeoutSeconds) {
        Preconditions.checkArgument(maxRequestSizeBytes >= MINIMUM_UPLOAD_REQUEST_SIZE_BYTES,
                "The maximum request size is likely too small for even a minimal request to be sent");
        Preconditions.checkArgument(uploadTimeoutSeconds > 0,
                "Billed cost upload request timeout must be greater than 0 seconds");

        this.billedCostService = billedCostService;
        this.maxRequestSizeBytes = maxRequestSizeBytes;
        this.uploadTimeoutSeconds = uploadTimeoutSeconds;
    }

    /**
     * Accept some {@link CloudBillingData} from a mediation target. It won't necessarily get
     * uploaded right away. If the target already has billing data waiting in the upload queue, it
     * will be replaced.
     *
     * @param targetId mediation target ID
     * @param targetProbeType type of the mediation probe
     * @param cloudBillingData discovered billing data
     */
    public void enqueueTargetBillingData(@Nonnull final Long targetId,
            @Nullable final SDKProbeType targetProbeType,
            @Nonnull final List<CloudBillingData> cloudBillingData) {

        if (targetProbeType == null) {
            logger.warn("Ignoring billing data for target <{}>; it has an unknown probe type",
                    targetId);
            return;
        }

        // While it's tempting to return even earlier if this target didn't discover any billing
        // data and discard this probe type information, things like database tier entities are
        // typically discovered by non-billing probes, and we have some OID-resolution workarounds
        // (see CloudEntitiesMap#populateFrom) that need to know what kind of target discovered the
        // entity to apply provider-specific transformations to the data that we get from the
        // stitching context, so we always want to record the target's probe type even if it isn't
        // explicitly providing billing data.
        insertTargetProbeType(targetId, targetProbeType);

        if (cloudBillingData.isEmpty()) {
            return;
        }

        logger.debug("Received new billing data for <{}> target <{}>", targetProbeType, targetId);
        insertTargetBillingData(targetId, cloudBillingData, true);
    }

    /**
     * Notify that a mediation target has been removed. There's no need to upload any pending cost
     * data for a target that is no longer being discovered.
     *
     * @param targetId the ID of the target
     */
    public void targetRemoved(final long targetId) {
        activeTargetProbeTypes.remove(targetId);
        targetBillingDataUploadQueueLock.writeLock().lock();
        try {
            targetBillingDataUploadQueue.removeAll(targetId);
        } finally {
            targetBillingDataUploadQueueLock.writeLock().unlock();
        }
    }

    /**
     * Insert some cost data from a mediation target into the upload queue.
     *
     * @param targetId target ID
     * @param cloudBillingData discovered cost data
     * @param replaceExisting if true, and data already exists for the target, replace it,
     *         or if false, and data already exists for the target, do not insert it
     */
    private void insertTargetBillingData(final long targetId,
            @Nonnull final Iterable<CloudBillingData> cloudBillingData,
            final boolean replaceExisting) {
        targetBillingDataUploadQueueLock.writeLock().lock();
        try {
            if (replaceExisting) {
                targetBillingDataUploadQueue.removeAll(targetId);
                targetBillingDataUploadQueue.putAll(targetId, cloudBillingData);
            } else if (!targetBillingDataUploadQueue.containsKey(targetId)) {
                targetBillingDataUploadQueue.putAll(targetId, cloudBillingData);
            }
        } finally {
            targetBillingDataUploadQueueLock.writeLock().unlock();
        }
    }

    /**
     * Keep track of the types of the targets that have provided cost data. Mostly so that if a
     * billing account's OID is missing later, we can use the probe type to generate a backup OID.
     *
     * @param targetId target ID
     * @param targetProbeType target's probe type
     */
    private void insertTargetProbeType(final long targetId,
            @Nonnull final SDKProbeType targetProbeType) {
        activeTargetProbeTypes.put(targetId, targetProbeType);
    }

    /**
     * Attempt to upload all billed cost that has been recorded and temporarily placed into the
     * queue. If the upload for a mediation target's billing data fails with a remote error, it will
     * be re-tried on the next upload, unless the affected target has discovered new data by then.
     *
     * @param stitchingContext the {@link StitchingContext} from the ongoing topology
     *         pipeline
     */
    public void processUploadQueue(@Nonnull final StitchingContext stitchingContext) {

        final SetMultimap<Long, CloudBillingData> billingDataToUpload =
                snapshotAndClearTargetBillingDataCache();

        logger.debug("Processing billing data currently in the upload queue for {} target(s)",
                () -> billingDataToUpload.keySet().size());

        final CloudEntitiesMap cloudEntitiesMap =
                createCloudEntitiesMap(stitchingContext, activeTargetProbeTypes);
        final Map<String, Long> entityBillingIdToOidMap =
                createEntityBillingIdToOidMap(stitchingContext);

        final ListMultimap<Long, CloudBillingData> targetFailedUploads = ArrayListMultimap.create();

        billingDataToUpload.forEach((targetId, cloudBillingData) -> {
            final Stopwatch timer = Stopwatch.createStarted();

            final Map<String, Long> entityLocalIdToOidMap =
                    stitchingContext.getTargetEntityLocalIdToOid(targetId);

            final BilledCostData billedCostData =
                    resolveOidsAndConvertToBilledCostData(targetId, cloudBillingData,
                            cloudEntitiesMap, entityBillingIdToOidMap, entityLocalIdToOidMap);

            final ResponseHandler<UploadBilledCloudCostResponse> responseHandler =
                    new ResponseHandler<>();

            try (RequestSender<UploadBilledCloudCostRequest> requestSender = new RequestSender<>(
                    billedCostService.withDeadlineAfter(uploadTimeoutSeconds, TimeUnit.SECONDS)
                            .uploadBilledCloudCost(responseHandler))) {

                logger.info("Uploading billing data for billing family <{}> from target <{}>",
                        billedCostData::getBillingFamilyId, () -> targetId);

                final List<CostTagsSegment> costTagsSegments =
                        chunkCostTagGroups(billedCostData.getCostTagGroupMap());
                final List<BilledCostSegment> billedCostSegments =
                        chunkBilledCostBuckets(billedCostData.getCostBucketsList());

                logger.info("Chunking statistics for cost tags: {} (bytes)",
                        () -> costTagsSegments.stream()
                                .mapToInt(AbstractMessage::getSerializedSize)
                                .summaryStatistics());
                logger.info("Chunking statistics for billed cost buckets: {} (bytes)",
                        () -> billedCostSegments.stream()
                                .mapToInt(AbstractMessage::getSerializedSize)
                                .summaryStatistics());

                // Upload billed cost metadata
                requestSender.send(UploadBilledCloudCostRequest.newBuilder()
                        .setBilledCostMetadata(MetadataSegment.newBuilder()
                                .setGranularity(billedCostData.getGranularity())
                                .setBillingFamilyId(billedCostData.getBillingFamilyId())
                                .setServiceProviderId(billedCostData.getServiceProviderId())
                                .build())
                        .build());

                // Upload cost tag groups
                for (final CostTagsSegment costTagsSegment : costTagsSegments) {
                    requestSender.send(UploadBilledCloudCostRequest.newBuilder()
                            .setCostTags(costTagsSegment)
                            .build());
                }

                // Upload billed cost buckets
                for (final BilledCostSegment billedCostSegment : billedCostSegments) {
                    requestSender.send(UploadBilledCloudCostRequest.newBuilder()
                            .setBilledCost(billedCostSegment)
                            .build());
                }
            } catch (final MessageChunker.OversizedElementException exOversizedElement) {
                logger.error("Skipping billed cost upload for target <{}>; "
                                + "Billed cost item too large to be uploaded; {}", targetId,
                        exOversizedElement.getMessage());
            }

            // Block until the receiver completes the response
            try {
                responseHandler.awaitCompletion();
            } catch (final InterruptedException interruptedException) {
                logger.error("Interrupted while waiting on billed cost upload to finish",
                        interruptedException);
            }

            responseHandler.getLastError().ifPresent(error -> {
                logger.error("Billed cost upload for target <{}> encountered a "
                        + "remote error and will re-queue: {}", () -> targetId, error::getMessage);

                targetFailedUploads.put(targetId, cloudBillingData);
            });

            logger.info("Finished billing data upload for target <{}> in {}", targetId, timer);
        });

        // On a remote error, try again during the next upload, unless there's new data or
        // the target was removed during the upload process
        targetFailedUploads.asMap().forEach((targetId, failedUploads) -> {
            if (activeTargetProbeTypes.containsKey(targetId)) {
                insertTargetBillingData(targetId, failedUploads, false);
            }
        });
    }

    /**
     * Make a copy of the current billing data upload queue, and clear it for new entries to begin
     * accumulating.
     *
     * @return a snapshot of the current upload queue
     */
    @Nonnull
    private SetMultimap<Long, CloudBillingData> snapshotAndClearTargetBillingDataCache() {
        targetBillingDataUploadQueueLock.writeLock().lock();
        try {
            final SetMultimap<Long, CloudBillingData> snapshot =
                    ImmutableSetMultimap.copyOf(targetBillingDataUploadQueue);
            targetBillingDataUploadQueue.clear();
            return snapshot;
        } finally {
            targetBillingDataUploadQueueLock.writeLock().unlock();
        }
    }

    /**
     * Convert {@link BilledCostBucket}s to Protobuf, split up buckets larger than the maximum size,
     * and merge the rest into chunks of up to the maximum size.
     *
     * <p>Always returns at least 1 segment which will be empty if no billed cost buckets were provided.
     */
    @Nonnull
    private List<BilledCostSegment> chunkBilledCostBuckets(
            @Nonnull final List<BilledCostBucket> billedCostBuckets)
            throws MessageChunker.OversizedElementException {

        if (billedCostBuckets.isEmpty()) {
            return ImmutableList.of(BilledCostSegment.getDefaultInstance());
        }

        final ImmutableList.Builder<BilledCostBucket> sizeCappedBuckets = ImmutableList.builder();

        for (final BilledCostBucket billedCostBucket : billedCostBuckets) {

            if (billedCostBucket.getSerializedSize() > maxRequestSizeBytes) {
                logger.debug(
                        "Splitting large bucket of size {} into smaller buckets of max size {}",
                        billedCostBucket::getSerializedSize, () -> maxRequestSizeBytes);

                final List<BilledCostBucket> bucketParts =
                        splitLargeBucket(billedCostBucket, maxRequestSizeBytes);

                sizeCappedBuckets.addAll(bucketParts);
            } else {
                sizeCappedBuckets.add(billedCostBucket);
            }
        }

        return MessageChunker.chunkMessages(sizeCappedBuckets.build(),
                        BilledCostSegment::newBuilder, BilledCostSegment.Builder::addCostBuckets, false,
                        maxRequestSizeBytes)
                .stream()
                .map(Builder::build)
                .collect(ImmutableList.toImmutableList());
    }

    /**
     * Split a large {@link BilledCostBucket} into multiple smaller buckets each smaller than
     * maximumBucketSize. Each bucket will be identical to the original apart from the {@link
     * BilledCostItem} list. If the bucket is not larger than maximumBucketSize, a list of just the
     * 1 bucket is returned.
     *
     * @param billedCostBucket the large bucket
     * @param maximumBucketSize maximum bucket size
     * @return multiple buckets divided into chunks
     * @throws MessageChunker.OversizedElementException if any {@link BilledCostItem} added
     *         to an empty bucket creates a bucket larger than maximumBucketSize
     */
    @Nonnull
    private List<BilledCostBucket> splitLargeBucket(final BilledCostBucket billedCostBucket,
            final long maximumBucketSize) throws MessageChunker.OversizedElementException {

        final int padding = ProtobufUtils.getMaximumMessageSizeExcludingRepeatedFields(
                BilledCostBucket.getDescriptor());

        final List<BilledCostBucket.Builder> billedCostBuckets =
                MessageChunker.chunkMessages(billedCostBucket.getCostItemsList(),
                        BilledCostBucket::newBuilder, BilledCostBucket.Builder::addCostItems, false,
                        maximumBucketSize - padding);

        final BilledCostBucket prototype = billedCostBucket.toBuilder().clearCostItems().build();

        return billedCostBuckets.stream()
                .map(builder -> builder.mergeFrom(prototype))
                .map(BilledCostBucket.Builder::build)
                .collect(ImmutableList.toImmutableList());
    }

    /**
     * Convert {@link CostTagGroup}s to Protobuf and merge them into chunks of a maximum size.
     *
     * <p>Always returns at least 1 segment which will be empty if no cost tag groups were provided.
     */
    @Nonnull
    private List<CostTagsSegment> chunkCostTagGroups(
            @Nonnull final Map<Long, CostTagGroup> costTagGroups)
            throws MessageChunker.OversizedElementException {

        if (costTagGroups.isEmpty()) {
            return ImmutableList.of(CostTagsSegment.getDefaultInstance());
        }

        final List<CostTagsSegment> costTagsSegments = costTagGroups.entrySet()
                .stream()
                .map(mapEntry -> CostTagsSegment.newBuilder()
                        .putCostTagGroup(mapEntry.getKey(), mapEntry.getValue())
                        .build())
                .collect(ImmutableList.toImmutableList());

        return MessageChunker.chunkMessages(costTagsSegments, CostTagsSegment::newBuilder,
                        (builder, message) -> builder.putAllCostTagGroup(message.getCostTagGroupMap()),
                        false, maxRequestSizeBytes)
                .stream()
                .map(CostTagsSegment.Builder::build)
                .collect(ImmutableList.toImmutableList());
    }

    /**
     * Convert the mediation data format, {@link CloudBillingData}, to the leaner {@link
     * BilledCostData} which references OIDs. OIDs are resolved from textual IDs mostly using the
     * {@link CloudEntitiesMap} but also from some supplemental sources.
     *
     * @param targetId ID of the mediation target
     * @param cloudBillingData billing data to convert
     * @param cloudEntitiesMap utility for resolving OIDs
     * @param entityBillingIdToOidMap extra tool for getting entity OIDs from their billing
     *         IDs
     * @param entityLocalIdToOidMap extra tool for resolving OIDs based on {@link
     *         com.vmturbo.platform.common.dto.CommonDTO.EntityIdentifyingPropertyValues}
     * @return converted and populated billed cost data
     */
    @Nonnull
    private BilledCostData resolveOidsAndConvertToBilledCostData(final long targetId,
            @Nonnull final CloudBillingData cloudBillingData,
            @Nonnull final CloudEntitiesMap cloudEntitiesMap,
            @Nonnull final Map<String, Long> entityBillingIdToOidMap,
            @Nonnull final Map<String, Long> entityLocalIdToOidMap) {

        final BilledCostData.Builder billedCostData = BilledCostData.newBuilder();

        final Function<String, Long> resolveCloudOid = cloudEntitiesMap::get;
        final Function<String, Long> resolveBillingEntityOid = entityBillingIdToOidMap::get;
        final Function<String, Long> resolveTargetLocalOid = entityLocalIdToOidMap::get;
        final Function<String, Long> resolveFallbackAccountOid =
                accountId -> cloudEntitiesMap.getFallbackAccountOid(targetId);

        final Function<String, Long> resolveDefaultOid = id -> {
            final long defaultOid = 0L;
            logger.warn("Oid not found for id <{}>, using fallback oid <{}>", id, defaultOid);
            return defaultOid;
        };

        if (cloudBillingData.hasBillingIdentifier()) {
            resolveOid(cloudBillingData.getBillingIdentifier(), resolveCloudOid).ifPresent(
                    billedCostData::setBillingFamilyId);
        }

        final boolean hasCloudBillingBuckets = cloudBillingData.getCloudCostBucketsCount() > 0;
        final boolean hasCloudBillingDataPoints = hasCloudBillingBuckets
                && cloudBillingData.getCloudCostBuckets(0).getSamplesCount() > 0;

        // Get the granularity from the first bucket; assume they're all the same
        if (hasCloudBillingBuckets) {
            final CloudBillingBucket firstBucket = cloudBillingData.getCloudCostBuckets(0);
            if (firstBucket.hasGranularity()) {
                billedCostData.setGranularity(firstBucket.getGranularity());
            }
        }

        // Get the service provider ID from the first data point; assume they're all the same
        if (hasCloudBillingDataPoints) {
            final CloudBillingDataPoint firstSample =
                    cloudBillingData.getCloudCostBuckets(0).getSamples(0);
            if (firstSample.hasServiceProviderId()) {
                resolveOid(firstSample.getServiceProviderId(), resolveCloudOid,
                        resolveDefaultOid).ifPresent(billedCostData::setServiceProviderId);
            }
        }

        // Map CloudBillingBuckets to BilledCostBuckets, resolving OIDs in the process
        for (final CloudBillingBucket cloudBillingBucket : cloudBillingData.getCloudCostBucketsList()) {
            final BilledCostBucket.Builder billedCostBucket = BilledCostBucket.newBuilder();

            billedCostBucket.setSampleTsUtc(cloudBillingBucket.getTimestampUtcMillis());

            if (cloudBillingBucket.hasBucketKey()) {
                final CloudBillingBucket.BucketKey cloudBillingBucketKey =
                        cloudBillingBucket.getBucketKey();
                final BilledCostBucketKey.Builder billedCostBucketKey =
                        BilledCostBucketKey.newBuilder();

                if (cloudBillingBucketKey.hasAccountId()) {
                    resolveOid(cloudBillingBucketKey.getAccountId(), resolveCloudOid,
                            resolveTargetLocalOid, resolveFallbackAccountOid).ifPresent(
                            billedCostBucketKey::setAccountOid);
                }
                if (cloudBillingBucketKey.hasCloudServiceId()) {
                    resolveOid(cloudBillingBucketKey.getCloudServiceId(), resolveCloudOid,
                            resolveTargetLocalOid, resolveDefaultOid).ifPresent(
                            billedCostBucketKey::setCloudServiceOid);
                }
                if (cloudBillingBucketKey.hasRegionId()) {
                    resolveOid(cloudBillingBucketKey.getRegionId(), resolveCloudOid,
                            resolveTargetLocalOid, resolveDefaultOid).ifPresent(
                            billedCostBucketKey::setRegionOid);
                }
            }

            for (final CloudBillingDataPoint cloudBillingDataPoint : cloudBillingBucket.getSamplesList()) {
                final BilledCostItem.Builder billedCostItem = BilledCostItem.newBuilder();

                if (cloudBillingDataPoint.hasEntityId()) {
                    final Optional<Long> entityOid =
                            resolveOid(cloudBillingDataPoint.getEntityId(), resolveBillingEntityOid,
                                    resolveTargetLocalOid);

                    // Entity-level cost belonging to an undiscovered entity is ignored.
                    // For now, entities are discovered by metrics targets, not billing targets,
                    // so a billing target in isolation cannot persist entity-level cost, yet.
                    // As we move to allowing billing targets to discover entities, the lack of a
                    // metrics target should become less disastrous and more of an error case.
                    if (!entityOid.isPresent()) {
                        logger.debug("Oid not found for entity with id <{}>; dropping its cost",
                                cloudBillingDataPoint::getEntityId);
                        continue;
                    }

                    entityOid.ifPresent(billedCostItem::setEntityId);
                }
                if (cloudBillingDataPoint.hasAccountId()) {
                    resolveOid(cloudBillingDataPoint.getAccountId(), resolveCloudOid,
                            resolveTargetLocalOid, resolveFallbackAccountOid).ifPresent(
                            billedCostItem::setAccountId);
                }
                if (cloudBillingDataPoint.hasCloudServiceId()) {
                    resolveOid(cloudBillingDataPoint.getCloudServiceId(), resolveCloudOid,
                            resolveTargetLocalOid, resolveDefaultOid).ifPresent(
                            billedCostItem::setCloudServiceId);
                }
                if (cloudBillingDataPoint.hasRegionId()) {
                    resolveOid(cloudBillingDataPoint.getRegionId(), resolveCloudOid,
                            resolveTargetLocalOid, resolveDefaultOid).ifPresent(
                            billedCostItem::setRegionId);
                }
                if (cloudBillingDataPoint.hasProviderId()) {
                    resolveOid(cloudBillingDataPoint.getProviderId(), resolveCloudOid,
                            resolveDefaultOid).ifPresent(billedCostItem::setProviderId);
                }

                if (cloudBillingDataPoint.hasPurchasedCommodity()) {
                    billedCostItem.setCommodityType(
                            cloudBillingDataPoint.getPurchasedCommodity().getNumber());
                }
                if (cloudBillingDataPoint.hasPriceModel()) {
                    billedCostItem.setPriceModel(cloudBillingDataPoint.getPriceModel());
                }
                if (cloudBillingDataPoint.hasUsageAmount()) {
                    billedCostItem.setUsageAmount(cloudBillingDataPoint.getUsageAmount());
                }
                if (cloudBillingDataPoint.hasCost()) {
                    billedCostItem.setCost(cloudBillingDataPoint.getCost());
                }
                if (cloudBillingDataPoint.hasCostTagGroupId()) {
                    billedCostItem.setCostTagGroupId(cloudBillingDataPoint.getCostTagGroupId());
                }
                if (cloudBillingDataPoint.hasEntityType()) {
                    billedCostItem.setEntityType(cloudBillingDataPoint.getEntityType());
                }
                if (cloudBillingDataPoint.hasProviderType()) {
                    billedCostItem.setProviderType(cloudBillingDataPoint.getProviderType());
                }
                if (cloudBillingDataPoint.hasCostCategory()) {
                    billedCostItem.setCostCategory(cloudBillingDataPoint.getCostCategory());
                }

                billedCostBucket.addCostItems(billedCostItem);
            }

            billedCostData.addCostBuckets(billedCostBucket);
        }

        billedCostData.putAllCostTagGroup(cloudBillingData.getCostTagGroupMapMap());

        return billedCostData.build();
    }

    /**
     * Apply a list of OID resolution strategies to a mediation identifier, returning the first one
     * that's successfully resolved, or an empty optional otherwise.
     *
     * @param identifier entity identifier
     * @param resolvers list of OID resolution strategies
     * @return first OID found by applying the resolvers in order
     */
    @SafeVarargs
    private static Optional<Long> resolveOid(@Nonnull final String identifier,
            @Nonnull final Function<String, Long>... resolvers) {
        return Arrays.stream(resolvers)
                .map(resolver -> resolver.apply(identifier))
                .filter(Objects::nonNull)
                .findFirst();
    }

    @Nonnull
    protected CloudEntitiesMap createCloudEntitiesMap(
            @Nonnull final StitchingContext stitchingContext,
            @Nonnull final Map<Long, SDKProbeType> probeTypeByTargetId) {
        return new CloudEntitiesMap(stitchingContext, probeTypeByTargetId);
    }

    @Nonnull
    protected Map<String, Long> createEntityBillingIdToOidMap(
            @Nonnull final StitchingContext stitchingContext) {
        final Map<String, Long> entityBillingIdToOid = new HashMap<>();
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

    @NotNull
    @Override
    public String getFileName() {
        return "BilledCloudCostUploader";
    }

    @Override
    public void collectDiags(@NotNull final DiagnosticsAppender appender)
            throws DiagnosticsException {
        final Gson gson = ComponentGsonFactory.createGsonNoPrettyPrint();
        appender.appendString(gson.toJson(activeTargetProbeTypes));
        targetBillingDataUploadQueueLock.readLock().lock();
        try {
            appender.appendString(gson.toJson(targetBillingDataUploadQueue.asMap()));
        } finally {
            targetBillingDataUploadQueueLock.readLock().unlock();
        }
    }

    @Override
    public void restoreDiags(@NotNull final List<String> collectedDiags, final Void context) {
        if (collectedDiags.isEmpty()) {
            logger.info("Empty diags - not restoring anything");
            return;
        }
        final Gson gson = ComponentGsonFactory.createGsonNoPrettyPrint();
        final Map<Long, SDKProbeType> restoredProbeTypeByTargetId =
                gson.fromJson(collectedDiags.get(0),
                        new TypeToken<Map<Long, SDKProbeType>>() {}.getType());
        activeTargetProbeTypes.putAll(restoredProbeTypeByTargetId);

        final Map<Long, Collection<CloudBillingData>> restoredTargetBillingData =
                gson.fromJson(collectedDiags.get(1),
                        new TypeToken<Map<Long, Collection<CloudBillingData>>>() {}.getType());
        targetBillingDataUploadQueueLock.writeLock().lock();
        try {
            restoredTargetBillingData.forEach(targetBillingDataUploadQueue::putAll);
        } finally {
            targetBillingDataUploadQueueLock.writeLock().unlock();
        }
    }

    /**
     * Utility for sending requests to a {@link StreamObserver} that implements {@link
     * AutoCloseable}.
     *
     * @param <RequestT> request type
     */
    private static class RequestSender<RequestT extends AbstractMessage> implements AutoCloseable {

        private final StreamObserver<RequestT> requestStream;

        /**
         * Constructor for {@link RequestSender}.
         *
         * @param requestStream destination stream for requests
         */
        RequestSender(@Nonnull final StreamObserver<RequestT> requestStream) {
            this.requestStream = requestStream;
        }

        public void send(@Nonnull final RequestT request) {
            requestStream.onNext(request);
        }

        @Override
        public void close() {
            requestStream.onCompleted();
        }
    }

    /**
     * Utility for handling responses from a {@link StreamObserver} that indicates when the request
     * has been completed by the receiver. This observer will also indicate that the request is
     * completed if one or more errors have been received.
     *
     * @param <ResponseT> response type
     */
    private static class ResponseHandler<ResponseT extends AbstractMessage>
            implements StreamObserver<ResponseT> {

        private final CountDownLatch completed = new CountDownLatch(1);

        private Throwable lastError;

        @Override
        public void onNext(@Nonnull final ResponseT responseMessage) {
            // no-op
        }

        @Override
        public void onError(@Nonnull final Throwable throwable) {
            lastError = throwable;
            completed.countDown();
        }

        @Override
        public void onCompleted() {
            completed.countDown();
        }

        public void awaitCompletion() throws InterruptedException {
            completed.await();
        }

        @Nonnull
        public Optional<Throwable> getLastError() {
            return Optional.ofNullable(lastError);
        }
    }
}
