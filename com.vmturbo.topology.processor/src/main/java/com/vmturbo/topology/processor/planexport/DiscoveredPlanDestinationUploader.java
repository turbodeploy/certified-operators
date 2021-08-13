package com.vmturbo.topology.processor.planexport;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.StampedLock;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.plan.PlanExportDTO.PlanDestination;
import com.vmturbo.common.protobuf.plan.PlanExportDTO.PlanDestination.Builder;
import com.vmturbo.common.protobuf.plan.PlanExportDTO.PlanDestinationCriteria;
import com.vmturbo.common.protobuf.plan.PlanExportDTO.StoreDiscoveredPlanDestinationsResponse;
import com.vmturbo.common.protobuf.plan.PlanExportServiceGrpc.PlanExportServiceStub;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityProperty;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.NonMarketDTO.NonMarketEntityDTO;
import com.vmturbo.platform.common.dto.NonMarketDTO.NonMarketEntityDTO.NonMarketEntityType;
import com.vmturbo.platform.sdk.common.util.SDKUtil;
import com.vmturbo.topology.processor.stitching.StitchingContext;
import com.vmturbo.topology.processor.stitching.TopologyStitchingEntity;

/**
 * This class is responsible for extracting discovered plan destinations
 * and sending them to the Plan Orchestrator.
 */
public class DiscoveredPlanDestinationUploader {
    private static final Logger logger = LogManager.getLogger();

    private final PlanExportServiceStub planExportServiceStub;

    // A cache of all the plan export destinations discovered by probes.
    private final Map<Long, List<NonMarketEntityDTO>> destinatinonsByTargetId = new ConcurrentHashMap<>();

    // we'll be using a stamped lock to guard the data map. However, we are inverting
    // the operations -- we are aiming to support concurrent puts (it's a concurrent map) but
    // lock for a single reader when we make a copy of the map.
    private StampedLock targetPlanDestinationDataCacheLock = new StampedLock();

    /**
     * Create an instance of thr uploader.
     *
     * @param planExportServiceStub the stub used to communicate with the Plan Orchestrator.
     */
    public DiscoveredPlanDestinationUploader(@Nonnull PlanExportServiceStub planExportServiceStub) {
        this.planExportServiceStub = Objects.requireNonNull(planExportServiceStub);
    }

    /**
     * Add the discovered destinations to the per-target cache. If there is an existing entry
     * with the same target it, it will get replaced (or removed if there are no
     * discovered destinations)
     *
     * @param targetId the target that discovered the destinations
     * @param destinations the PlanDestinations to add
     */
    private void cacheDestinationData(long targetId,
                                      @Nonnull List<NonMarketEntityDTO> destinations) {
        logger.trace("Getting read lock for destination data map");
        long stamp = targetPlanDestinationDataCacheLock.readLock();
        logger.trace("Got read lock for destination data map");
        try {
            if (destinations.isEmpty()) {
                destinatinonsByTargetId.remove(targetId);
            } else {
                destinatinonsByTargetId.put(targetId, destinations);
            }
        } finally {
            logger.trace("Releasing read lock for plan destinations map");
            targetPlanDestinationDataCacheLock.unlock(stamp);
        }
    }

    /**
     * Get an immutable snapshot of the destinations data map in its current state.
     *
     * @return an {@link ImmutableMap} of the destination data objects, by target id.
     */
    @VisibleForTesting
    Map<Long, List<NonMarketEntityDTO>> getPlanDestinationDataByTargetIdSnapshot() {
        logger.trace("Getting write lock for destination data map");
        long stamp = targetPlanDestinationDataCacheLock.writeLock();
        logger.trace("Got write lock for destinationt data map");
        try {
            return ImmutableMap.copyOf(this.destinatinonsByTargetId);
        } finally {
            logger.trace("Releasing write lock for destination data map");
            targetPlanDestinationDataCacheLock.unlock(stamp);
        }
    }

    /**
     * <p>This is called when a discovery completes.
     * </p>
     * Set aside any plan destination data contained in the discovery response for the given target.
     * We will use this data later, in the topology pipeline.
     *
     * @param targetId target id
     * @param nonMarketEntityDTOS non market entity DTOs
     */
    public void recordPlanDestinations(long targetId,
                                       @Nonnull final List<NonMarketEntityDTO> nonMarketEntityDTOS) {
        cacheDestinationData(targetId, nonMarketEntityDTOS.stream()
            .filter(nme -> NonMarketEntityType.PLAN_DESTINATION == nme.getEntityType())
            .collect(Collectors.toList()));
    }

    /**
     * When a target is removed, we will remove any cached plan destinations associated with it.
     *
     * @param targetId target id
     */
    public void targetRemoved(long targetId) {
         long stamp = targetPlanDestinationDataCacheLock.readLock();
        try {
            destinatinonsByTargetId.remove(targetId);
        } finally {
            logger.trace("Releasing read lock for destination data map");
            targetPlanDestinationDataCacheLock.unlock(stamp);
        }
    }

    /**
     * <p>Upload the plan destination data.
     * </p>
     * Called in the topology pipeline after the stitching context has been created, but before
     * it has been converted to a topology map. Ths is because this is the easiest point to
     * associate destinations with business accounts. In the future if the SDK plan destination
     * protobuf is changed to explicitly indicate an associated business account, this stage
     * could happen later in the pipeline.
     *
     * @param stitchingContext The context used to stitch the topology
     */
    public synchronized void uploadPlanDestinations(@Nonnull StitchingContext stitchingContext) {

        // create a copy of the destinations data map so we have a stable data set for this upload step.
        Map<Long, List<NonMarketEntityDTO>> destinationsByTargetIdSnapshot  =
            getPlanDestinationDataByTargetIdSnapshot();

        // Upload the plan destinations to the plan orchestrator component.
        final CountDownLatch finishLatch = new CountDownLatch(1);
        StreamObserver<StoreDiscoveredPlanDestinationsResponse> responseObserver =
            new StreamObserver<StoreDiscoveredPlanDestinationsResponse>() {
                @Override
                public void onNext(StoreDiscoveredPlanDestinationsResponse response) {
                }

                @Override
                public void onError(Throwable t) {
                    Status status = Status.fromThrowable(t);
                    logger.error("Error uploading discovered plan destinations due to: {}", status);
                    finishLatch.countDown();
                }

                @Override
                public void onCompleted() {
                    logger.debug("Finished uploading the discovered plan destinations");
                    finishLatch.countDown();
                }
            };

        final Map<Long, List<TopologyStitchingEntity>> accountsByTarget =
            stitchingContext.getEntitiesByEntityTypeAndTarget().get(EntityType.BUSINESS_ACCOUNT);

        StreamObserver<PlanDestination> requestObserver =
            planExportServiceStub.storeDiscoveredPlanDestinations(responseObserver);

        for (Entry<Long, List<NonMarketEntityDTO>> entry : destinationsByTargetIdSnapshot.entrySet()) {
            for (NonMarketEntityDTO entity : entry.getValue()) {
                Long accountOid = null;

                // There is no relationship in the discovery data between NonMarketEntities
                // and BusinessAccounts. Probes could be changed to explicitly indicate the
                // account that a plan destination is associated with, but for now, for
                // compatibility with the existing Azure probe, assume that the destination
                // is associated with the (only) account discovered by the same target.

                List<TopologyStitchingEntity> accounts = accountsByTarget.get(entry.getKey());
                if (accounts != null && !accounts.isEmpty()) {
                    accountOid = accounts.get(0).getOid();
                }

                PlanDestination record = convertEntityToDestination(entry.getKey(), accountOid, entity);
                try {
                    logger.debug("Sending plan destination: {}", record.toString());
                    requestObserver.onNext(record);
                } catch (RuntimeException e) {
                    logger.error("Error uploading plan destinations");
                    requestObserver.onError(e);
                    return;
                }
            }
        }

        requestObserver.onCompleted();
        try {
            // block until we get a response or an exception occurs.
            finishLatch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();  // set interrupt flag
            logger.error("Interrupted while waiting for response", e);
        }
    }

    @Nonnull
    private PlanDestination convertEntityToDestination(long targetId,
                                                       @Nullable Long accountOid,
                                                       @Nonnull NonMarketEntityDTO entity) {
        Builder builder = PlanDestination.newBuilder()
            .setExternalId(entity.getId())
            .setDisplayName(entity.getDisplayName())
            .setTargetId(targetId);

        if (accountOid != null) {
            builder.setCriteria(PlanDestinationCriteria.newBuilder().setAccountId(accountOid));
        }

        if (entity.getPlanDestinationData().hasHasExportedData()) {
            builder.setHasExportedData(entity.getPlanDestinationData().getHasExportedData());
        }

        for (EntityProperty property : entity.getEntityPropertiesList()) {
            if (property.getNamespace().equals(SDKUtil.DEFAULT_NAMESPACE)) {
                builder.putPropertyMap(property.getName(), property.getValue());
            }
        }

        return builder.build();
    }
}
