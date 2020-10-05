package com.vmturbo.repository.plan.db;

import java.util.Collection;

import javax.annotation.Nonnull;

import com.google.gson.JsonObject;

import org.jooq.DSLContext;

import com.vmturbo.common.protobuf.repository.RepositoryDTO.TopologyType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.components.api.FormattedString;

/**
 * Responsible for ingesting the source topology.
 */
class MySQLSourceTopologyCreator extends MySQLTopologyCreator<TopologyEntityDTO> {

    MySQLSourceTopologyCreator(final DSLContext dsl,
            final TopologyInfo topologyInfo,
            final int insertionChunkSize,
            final int deletionChunkSize) {
        super(topologyInfo, topologyInfo.getTopologyId(), TopologyType.SOURCE, dsl,
                insertionChunkSize, deletionChunkSize);
    }

    @Override
    protected CompressionStats getTotalCompressionStats() {
        return compressionStatsByType.getTotal();
    }

    @Nonnull
    @Override
    protected JsonObject summaryForDb() {
        JsonObject object = new JsonObject();
        object.addProperty("ingestion_duration", timer.toString());
        object.add("compression", getTotalCompressionStats().toJson());
        return object;
    }

    @Override
    protected String compressionSummaryForLog() {
        return FormattedString.format("Entity compression: {}.\nCompression stats by type: {}",
                getTotalCompressionStats(), compressionStatsByType);
    }

    @Override
    protected Collection<TopologyEntityDTO> extractEntities(
            Collection<TopologyEntityDTO> entities) {
        return entities;
    }
}
