package com.vmturbo.history.ingesters.live.writers;

import java.sql.Date;
import java.util.Collection;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

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
 * For Volumes attached to VMs, the Volume OID, VM OID would be recorded with the topology
 * creation time stored as both the Last Attached Date and Last Discovered Date.
 *
 * <p>Example, a record for Volume with oid 123 attached to VM with oid 789 would for a topology
 * with creation date 10-10-2020:
 * | Volume OID | VM OID | Last Attached Date | Last Discovered Date |
 * |------------|--------|--------------------|----------------------|
 * | 123        | 789    | 2020-10-10         | 2020-10-10           |
 * </p>
 */
public class VolumeAttachmentHistoryWriter extends TopologyWriterBase {

    private final BulkLoader<VolumeAttachmentHistoryRecord> loader;
    private final TopologyInfo topologyInfo;

    private VolumeAttachmentHistoryWriter(@Nonnull TopologyInfo topologyInfo,
                                          @Nonnull SimpleBulkLoaderFactory loaders) {
        this.loader = loaders.getLoader(VolumeAttachmentHistory.VOLUME_ATTACHMENT_HISTORY);
        this.topologyInfo = topologyInfo;
    }

    @Override
    protected ChunkDisposition processEntities(@Nonnull final Collection<TopologyEntityDTO> chunk,
                                               @Nonnull final String infoSummary)
        throws InterruptedException {
        //TODO(OM-63934): Directly return SUCCESS if it has not been 24 hours since last insertion
        for (final TopologyEntityDTO entity : chunk) {
            if (entity.getEnvironmentType() == EnvironmentType.CLOUD
                && entity.getEntityType() == EntityType.VIRTUAL_MACHINE_VALUE) {
                final Set<Long> volumeOids = entity.getCommoditiesBoughtFromProvidersList().stream()
                    .filter(cb -> cb.getProviderEntityType() == EntityType.VIRTUAL_VOLUME_VALUE)
                    .map(CommoditiesBoughtFromProvider::getProviderId)
                    .collect(Collectors.toSet());
                final Set<VolumeAttachmentHistoryRecord> recordsToInsert = volumeOids.stream()
                    .map(volId -> createRecord(volId, entity.getOid()))
                    .collect(Collectors.toSet());
                loader.insertAll(recordsToInsert);
            }
        }
        return ChunkDisposition.SUCCESS;
    }

    private VolumeAttachmentHistoryRecord createRecord(final long volumeOid, final long vmOid) {
        final Date date = new Date(topologyInfo.getCreationTime());
        return new VolumeAttachmentHistoryRecord(volumeOid, vmOid, date, date);
    }

    @Override
    public void finish(int entityCount, boolean expedite, String infoSummary) {
        //TODO(OM-63933, OM-63934): Accumulate Unattached Volumes while processing chunks and insert
        // records for them during finish stage. Skip insertions if expedite is true, or if it
        // has not been 24 hours since last insertion
    }

    /**
     * Factory for VolumeAttachmentHistoryWriter.
     */
    public static class Factory extends TopologyWriterBase.Factory {
        @Override
        public Optional<IChunkProcessor<DataSegment>> getChunkProcessor(
            final TopologyInfo topologyInfo, final SimpleBulkLoaderFactory state) {
            return Optional.of(new VolumeAttachmentHistoryWriter(topologyInfo, state));
        }
    }
}