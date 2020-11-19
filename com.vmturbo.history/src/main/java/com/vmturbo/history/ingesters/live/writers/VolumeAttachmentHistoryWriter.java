package com.vmturbo.history.ingesters.live.writers;

import java.sql.Date;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import it.unimi.dsi.fastutil.longs.LongArraySet;
import it.unimi.dsi.fastutil.longs.LongSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.Topology.DataSegment;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.history.db.bulk.BulkLoader;
import com.vmturbo.history.db.bulk.SimpleBulkLoaderFactory;
import com.vmturbo.history.ingesters.common.IChunkProcessor;
import com.vmturbo.history.ingesters.common.writers.TopologyWriterBase;
import com.vmturbo.history.schema.abstraction.tables.VolumeAttachmentHistory;
import com.vmturbo.history.schema.abstraction.tables.records.VolumeAttachmentHistoryRecord;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Writer responsible for inserting data into the VOLUME_ATTACHMENT_HISTORY table for Cloud Volumes.
 * For Volumes attached to VMs, the Volume OID, VM OID and topology creation date (stored as Last
 * Attached Date and Last Discovered Date) are recorded. The Last Attached Date and Last
 * Discovered Date are updated while processing newer topologies.
 *
 * <p>Example, a record for Volume with oid 123 attached to VM with oid 789 would for a topology
 * with creation date 10-10-2020:
 * | Volume OID | VM OID | Last Attached Date | Last Discovered Date |
 * |------------|--------|--------------------|----------------------|
 * | 123        | 789    | 2020-10-10         | 2020-10-10           |
 * </p>
 * For Unattached Volumes, the Volume OID is recorded with a placeholder VM OID. The topology
 * creation date is stored as Last Attached Date and never updated after first insert. The Last
 * Attached Date is NOT updated while processing newer topologies, while the Last Discovered Date
 * is updated.
 *
 * <p>Example, a record for an Unattached Volume with oid 456:
 * | Volume OID | VM OID | Last Attached Date | Last Discovered Date |
 * |------------|--------|--------------------|----------------------|
 * | 456        | 0      | 2020-10-10         | 2020-10-10           |
 * </p>
 */
public class VolumeAttachmentHistoryWriter extends TopologyWriterBase {

    /**
     * Placeholder VM oid needed to write records for Unattached Volumes as the VM OID column of
     * Volume Attachment History table is part of the composite primary key (hence can't be null).
     */
    public static final long VM_OID_VALUE_FOR_UNATTACHED_VOLS = 0;

    private Logger logger = LogManager.getLogger();
    private final BulkLoader<VolumeAttachmentHistoryRecord> attachedVolumeLoader;
    private final BulkLoader<VolumeAttachmentHistoryRecord> unattachedVolumeLoader;
    private final TopologyInfo topologyInfo;
    private LongSet volumesNotVisitedFromVms = new LongArraySet();

    private VolumeAttachmentHistoryWriter(@Nonnull TopologyInfo topologyInfo,
                                          @Nonnull SimpleBulkLoaderFactory loaders) {
        this.attachedVolumeLoader = loaders
            .getLoader(VolumeAttachmentHistory.VOLUME_ATTACHMENT_HISTORY, Collections.emptySet());
        this.unattachedVolumeLoader =
            loaders.getLoader(VolumeAttachmentHistory.VOLUME_ATTACHMENT_HISTORY,
                Collections.singleton(VolumeAttachmentHistory
                    .VOLUME_ATTACHMENT_HISTORY.LAST_ATTACHED_DATE));
        this.topologyInfo = topologyInfo;
    }

    @Override
    protected ChunkDisposition processEntities(@Nonnull final Collection<TopologyEntityDTO> chunk,
                                               @Nonnull final String infoSummary)
        throws InterruptedException {
        for (final TopologyEntityDTO entity : chunk) {
            if (entity.getEnvironmentType() == EnvironmentType.CLOUD) {
                if (entity.getEntityType() == EntityType.VIRTUAL_MACHINE_VALUE) {
                    final Set<Long> volumeOids
                        = entity.getCommoditiesBoughtFromProvidersList().stream()
                        .filter(cb -> cb.getProviderEntityType() == EntityType.VIRTUAL_VOLUME_VALUE)
                        .map(CommoditiesBoughtFromProvider::getProviderId)
                        .collect(Collectors.toSet());
                    // Since the chunks are received in topological order (producers before
                    // consumers), Volumes are processed before VMs. As a result, at this point the
                    // volumesNotVisitedFromVMs set would contain oids of all Volumes in the
                    // topology. In the step below, volume oids visited from VMs (via bought
                    // commodities) are removed from this set. Once all the VMs are processed,
                    // the oids remaining in volumesNotVisitedFromVms would be those of
                    // unattached volumes. Therefore this logic strongly depends on the
                    // topological ordering (done in Topology Processor) of the received chunks.
                    volumesNotVisitedFromVms.removeAll(volumeOids);
                    attachedVolumeLoader.insertAll(createRecords(volumeOids, entity.getOid()));
                } else if (entity.getEntityType() == EntityType.VIRTUAL_VOLUME_VALUE) {
                    volumesNotVisitedFromVms.add(entity.getOid());
                }
            }
        }
        return ChunkDisposition.SUCCESS;
    }

    private List<VolumeAttachmentHistoryRecord> createRecords(final Set<Long> volumeOids,
                                                              final long vmOid) {
        final Date date = new Date(topologyInfo.getCreationTime());
        return volumeOids.stream()
            .map(volId -> new VolumeAttachmentHistoryRecord(volId, vmOid, date, date))
            .collect(Collectors.toList());
    }

    @Override
    public void finish(int entityCount, boolean expedite, String infoSummary)
        throws InterruptedException {
        logger.info("Total volumes not visited from any VMs: {}", volumesNotVisitedFromVms.size());
        logger.trace("Unattached volumes written into volume_attachment_history table: {}",
            () -> volumesNotVisitedFromVms);
        unattachedVolumeLoader.insertAll(createRecords(volumesNotVisitedFromVms,
            VM_OID_VALUE_FOR_UNATTACHED_VOLS));
    }

    /**
     * Factory for VolumeAttachmentHistoryWriter.
     */
    public static class Factory extends TopologyWriterBase.Factory {

        private final Logger logger = LogManager.getLogger();

        private final long intervalBetweenInsertsInMillis;
        private long lastInsertTopologyCreationTime;

        /**
         * Creates a VolumeAttachmentHistoryWriter.Factory.
         *
         * @param intervalBetweenInsertsInHours interval between inserts to
         *                                      VolumeAttachmentHistory table.
         */
        public Factory(final long intervalBetweenInsertsInHours) {
            this.intervalBetweenInsertsInMillis =
                TimeUnit.HOURS.toMillis(intervalBetweenInsertsInHours);
        }

        @Override
        public Optional<IChunkProcessor<DataSegment>> getChunkProcessor(
            final TopologyInfo topologyInfo, final SimpleBulkLoaderFactory state) {
            final long currentTopologyCreationTime = topologyInfo.getCreationTime();
            logger.debug("Current topology creation time: {}, Last insert topology creation time: "
                    + "{}, Interval between inserts: {}", currentTopologyCreationTime,
                lastInsertTopologyCreationTime, intervalBetweenInsertsInMillis);
            if (currentTopologyCreationTime >= lastInsertTopologyCreationTime
                + intervalBetweenInsertsInMillis) {
                lastInsertTopologyCreationTime = currentTopologyCreationTime;
                return Optional.of(new VolumeAttachmentHistoryWriter(topologyInfo, state));
            } else {
                return Optional.empty();
            }
        }
    }
}