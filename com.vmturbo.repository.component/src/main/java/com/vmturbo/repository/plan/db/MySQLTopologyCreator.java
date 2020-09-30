package com.vmturbo.repository.plan.db;

import java.time.LocalDateTime;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.StringJoiner;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.collect.Iterators;
import com.google.gson.JsonObject;
import com.google.protobuf.AbstractMessage;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.BatchBindStep;
import org.jooq.DSLContext;

import com.vmturbo.common.protobuf.StringUtil;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.TopologyType;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopologyEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Builder;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTOUtil;
import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.components.api.CompressedProtobuf;
import com.vmturbo.components.api.FormattedString;
import com.vmturbo.components.api.SharedByteBuffer;
import com.vmturbo.components.common.utils.MultiStageTimer;
import com.vmturbo.components.common.utils.MultiStageTimer.Detail;
import com.vmturbo.proactivesupport.DataMetricCounter;
import com.vmturbo.proactivesupport.DataMetricHistogram;
import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.repository.db.Tables;
import com.vmturbo.repository.topology.TopologyID;
import com.vmturbo.repository.topology.TopologyLifecycleManager.TopologyCreator;
import com.vmturbo.sql.utils.jooq.JooqUtil;

/**
 * Common code shared by source and projected topology ingestion.
 *
 * @param <T> The type of entity in the topology. Should be either {@link TopologyEntityDTO}
 *         or
 *         {@link ProjectedTopologyEntity}.
 */
abstract class MySQLTopologyCreator<T extends AbstractMessage> implements TopologyCreator<T> {
    protected final Logger logger = LogManager.getLogger(getClass());

    protected final long planId;

    protected final long topologyId;

    protected final DSLContext dsl;

    protected final int insertionChunkSize;

    protected final int deletionChunkSize;

    protected final CompressionStatsByType compressionStatsByType = new CompressionStatsByType();

    protected int numEntities = 0;

    protected final SharedByteBuffer sharedByteBuffer = new SharedByteBuffer();

    protected TopologyType topologyType;

    protected final MultiStageTimer timer = new MultiStageTimer(logger);

    private static final String ENTITY_STAGE = "entity_ingestion";

    MySQLTopologyCreator(final TopologyInfo topologyInfo, final long topologyId,
            final TopologyType topologyType,
            DSLContext dsl,
            final int insertionChunkSize,
            final int deletionChunkSize) {
        this.planId = topologyInfo.getTopologyContextId();
        this.topologyId = topologyId;
        this.topologyType = topologyType;
        this.dsl = dsl;
        this.insertionChunkSize = insertionChunkSize;
        this.deletionChunkSize = deletionChunkSize;
    }

    @Nonnull
    protected BatchBindStep newEntityBindStep() {
        return dsl.batch(dsl.insertInto(Tables.PLAN_ENTITY, Tables.PLAN_ENTITY.TOPOLOGY_OID,
                Tables.PLAN_ENTITY.ENTITY_OID, Tables.PLAN_ENTITY.ENTITY_TYPE,
                Tables.PLAN_ENTITY.ORIGIN, Tables.PLAN_ENTITY.ENTITY_STATE,
                Tables.PLAN_ENTITY.IS_PLACED, Tables.PLAN_ENTITY.ENVIRONMENT_TYPE,
                Tables.PLAN_ENTITY.ENTITY, Tables.PLAN_ENTITY.ENTITY_UNCOMPRESSED_SIZE)
                .values((Long)null, null, null, null, null, null, null, null, null));
    }

    @Nonnull
    protected BatchBindStep bindEntity(@Nonnull final TopologyEntityDTO e,
            @Nonnull final BatchBindStep entityBind) {
        final CompressedProtobuf<TopologyEntityDTO, Builder> c = CompressedProtobuf.compress(e,
                sharedByteBuffer);
        compressionStatsByType.recordEntity(ApiEntityType.fromType(e.getEntityType()), c);
        return entityBind.bind(topologyId, e.getOid(), (short)e.getEntityType(),
                (byte)e.getOrigin().getOriginTypeCase().getNumber(),
                (byte)e.getEntityState().getNumber(), (byte)(TopologyDTOUtil.isPlaced(e) ? 1 : 0),
                (byte)e.getEnvironmentType().getNumber(), c.getCompressedBytes(),
                c.getUncompressedLength());
    }

    @Override
    public void initialize() {
        logger.info(
                "Initializing SQL {} topology creator for topology {} plan {}", topologyType,
                topologyId, planId);
        dsl.insertInto(Tables.TOPOLOGY_METADATA)
            .set(Tables.TOPOLOGY_METADATA.CONTEXT_OID, planId)
            .set(Tables.TOPOLOGY_METADATA.TOPOLOGY_OID, topologyId)
            .set(Tables.TOPOLOGY_METADATA.TOPOLOGY_TYPE, (byte)topologyType.getNumber())
            .set(Tables.TOPOLOGY_METADATA.STATUS, TopologyStatus.INGESTION_STARTED.getNum())
            .set(Tables.TOPOLOGY_METADATA.DEBUG_INFO,
                FormattedString.format("{ \"ingestion_started\" : \"{}\" }",
                        LocalDateTime.now()))
            .execute();
    }

    @Override
    public void addEntities(Collection<T> entities, TopologyID tid) {
        Collection<TopologyEntityDTO> extractedEntities = extractEntities(entities);
        logger.debug(
                "Starting to process chunk of {} {} entities. {} entities previously processed",
                extractedEntities.size(), topologyType, numEntities);
        if (!extractedEntities.isEmpty()) {
            timer.start(ENTITY_STAGE);
            numEntities += extractedEntities.size();
            Iterators.partition(extractedEntities.iterator(), insertionChunkSize).forEachRemaining(
                    entityChunk -> {
                        BatchBindStep entityBind = newEntityBindStep();
                        for (TopologyEntityDTO e : entityChunk) {
                            entityBind = bindEntity(e, entityBind);
                        }
                        entityBind.execute();
                    });
            timer.stop();
        }
    }

    /**
     * Get the total {@link CompressionStats} for any compressed blobs inserted by the
     * creator.
     *
     * @return The {@link CompressionStats}.
     */
    protected abstract CompressionStats getTotalCompressionStats();

    /**
     * A small JSON object summarizing the topology ingestion. Debugging only.
     * This gets inserted into the database.
     *
     * @return The {@link JsonObject}.
     */
    protected abstract JsonObject summaryForDb();

    /**
     * A more verbose summary of compression stats for logging purposes.
     *
     * @return The summary string.
     */
    protected abstract String compressionSummaryForLog();

    /**
     * Extract {@link TopologyEntityDTO}s from the entities received by this creator.
     *
     * @param entities The entities.
     * @return The {@link TopologyEntityDTO}s.
     */
    protected abstract Collection<TopologyEntityDTO> extractEntities(Collection<T> entities);

    @Override
    public void complete() {

        JsonObject summary = summaryForDb();

        dsl.update(Tables.TOPOLOGY_METADATA).set(Tables.TOPOLOGY_METADATA.STATUS,
            TopologyStatus.INGESTION_COMPLETED.getNum()).set(
            Tables.TOPOLOGY_METADATA.DEBUG_INFO,
                ComponentGsonFactory.createGsonNoPrettyPrint().toJson(summary)).where(
            Tables.TOPOLOGY_METADATA.TOPOLOGY_OID.eq(topologyId)).execute();

        CompressionStats totalCompressed = getTotalCompressionStats();
        Metrics.ENTITY_CNT_SUMMARY.labels(topologyType.name()).observe((double)numEntities);
        Metrics.ENTITY_SIZE_SUMMARY.labels(topologyType.name(), Metrics.COMPRESSED).observe(
                (double)(totalCompressed.compressedSizeBytes));
        Metrics.ENTITY_SIZE_SUMMARY.labels(topologyType.name(), Metrics.UNCOMPRESSED).observe(
                (double)(totalCompressed.uncompressedSizeBytes));

        timer.visit((stageName, stopped, totalDurationMs) -> {
            if (stopped) {
                Metrics.INGESTION_HISTOGRAM.labels(topologyType.name(), stageName).observe(
                        (double)totalDurationMs);
            }
        });

        String summaryForLog = compressionSummaryForLog();
        timer.info(FormattedString.format(
                "Completed {} topology ingestion (id: {}, plan: {}). {} entities ingested in {}.\n{}",
                topologyType, topologyId, planId, numEntities, timer.toString(), summaryForLog),
                Detail.STAGE_SUMMARY);
    }

    @Override
    public void rollback() {
        Metrics.PROCESSING_ERRORS_CNT.labels(
                topologyType == TopologyType.PROJECTED ? Metrics.PROJ_INGESTION_ERROR
                        : Metrics.SRC_INGESTION_ERROR).increment();
        logger.info("Rolling back entities for context: {} {} topology {}",
                planId, topologyType, topologyId);
        final int numDeleted = JooqUtil.deleteInChunks(dsl.deleteFrom(Tables.PLAN_ENTITY)
                .where(Tables.PLAN_ENTITY.TOPOLOGY_OID.eq(topologyId))
                .orderBy(Tables.PLAN_ENTITY.ENTITY_OID), deletionChunkSize);
        dsl.deleteFrom(Tables.TOPOLOGY_METADATA).where(
                Tables.TOPOLOGY_METADATA.TOPOLOGY_OID.eq(topologyId)).execute();
        logger.info("Deleted {} entity rows and metadata for {} topology {} in context {}",
                numDeleted, topologyType, topologyId, planId);
    }

    /**
     * Utility to record total and by-type {@link CompressionStats}.
     */
    static class CompressionStatsByType {
        private final CompressionStats totalCompressionStats = new CompressionStats();
        private final Map<ApiEntityType, CompressionStats> statsByType = new HashMap<>();

        void recordEntity(ApiEntityType entityType, CompressedProtobuf<?, ?> protobuf) {
            totalCompressionStats.recordEntity(protobuf);
            statsByType.computeIfAbsent(entityType, k -> new CompressionStats()).recordEntity(protobuf);
        }

        @Nonnull
        CompressionStats getTotal() {
            return totalCompressionStats;
        }

        @Nonnull
        Map<ApiEntityType, CompressionStats> getByType() {
            return statsByType;
        }

        @Override
        public String toString() {
            StringJoiner joiner = new StringJoiner(", ");
            statsByType.forEach((type, stats) -> {
                joiner.add(FormattedString.format("{} - {}", type, stats));
            });
            return joiner.toString();
        }
    }

    /**
     * Utility class to help print information about compression ratios for blobs.
     */
    static class CompressionStats {
        private int numEntities = 0;
        private long compressedSizeBytes = 0;
        private long uncompressedSizeBytes = 0;

        void recordEntity(CompressedProtobuf<?, ?> protobuf) {
            this.numEntities++;
            this.compressedSizeBytes += protobuf.getCompressedBytes().length;
            this.uncompressedSizeBytes += protobuf.getUncompressedLength();
        }

        @Override
        public String toString() {
            return FormattedString.format("{} entities. {} compressed to {}",
                    numEntities,
                    StringUtil.getHumanReadableSize(uncompressedSizeBytes),
                    StringUtil.getHumanReadableSize(compressedSizeBytes));
        }

        @Nonnull
        public JsonObject toJson() {
            JsonObject jsonObject = new JsonObject();
            jsonObject.addProperty("num_entities", numEntities);
            jsonObject.addProperty("uncompressed_size", StringUtil.getHumanReadableSize(uncompressedSizeBytes));
            jsonObject.addProperty("compressed_size", StringUtil.getHumanReadableSize(compressedSizeBytes));
            return jsonObject;
        }

        @Nonnull
        static CompressionStats combine(@Nonnull final Stream<CompressionStats> compressionStats) {
            CompressionStats f = new CompressionStats();
            Iterator<CompressionStats> it = compressionStats.iterator();
            while (it.hasNext()) {
                CompressionStats stat = it.next();
                f.numEntities += stat.numEntities;
                f.compressedSizeBytes += stat.compressedSizeBytes;
                f.uncompressedSizeBytes += stat.compressedSizeBytes;
            }
            return f;
        }
    }


    /**
     * Metrics relevant for plan processing.
     */
    private static class Metrics {

        private Metrics() {}

        private static final String SRC_INGESTION_ERROR = "src_ingestion";
        private static final String PROJ_INGESTION_ERROR = "proj_ingestion";
        private static final String COMPRESSED = "true";
        private static final String UNCOMPRESSED = "false";

        private static final DataMetricHistogram INGESTION_HISTOGRAM = DataMetricHistogram.builder()
                .withName("repo_plan_sql_ingestion_duration_seconds")
                .withHelp("Duration of topology ingestion for plans, in seconds.")
                .withLabelNames("topology_type", "stage")
                // 10s, 1min, 3min, 7min, 10min, 15min, 30min
                // The SLA is to process everything in under 10 minutes.
                .withBuckets(10, 60, 180, 420, 600, 900, 1800)
                .build()
                .register();

        private static final DataMetricCounter PROCESSING_ERRORS_CNT = DataMetricCounter.builder()
                .withName("repo_plan_sql_error_cnt")
                .withHelp("Errors encountered during operations on the SQL plan entity store.")
                .withLabelNames("type")
                .build()
                .register();

        private static final DataMetricSummary ENTITY_CNT_SUMMARY = DataMetricSummary.builder()
                .withName("repo_plan_sql_entity_cnt")
                .withHelp("Number of entities in a plan topology saved in SQL.")
                .withLabelNames("topology_type")
                .build()
                .register();

        private static final DataMetricSummary ENTITY_SIZE_SUMMARY = DataMetricSummary.builder()
                .withName("repo_plan_sql_entity_size_bytes")
                .withHelp("Size of the entity data for a plan topology saved in SQL.")
                .withLabelNames("topology_type", "compressed")
                .build()
                .register();
    }
}
