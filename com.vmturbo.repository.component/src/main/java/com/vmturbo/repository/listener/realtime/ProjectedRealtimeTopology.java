package com.vmturbo.repository.listener.realtime;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.ThreadSafe;

import com.google.common.base.Stopwatch;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopologyEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.components.api.SharedByteBuffer;
import com.vmturbo.components.common.diagnostics.DiagnosticsAppender;
import com.vmturbo.components.common.diagnostics.DiagnosticsException;
import com.vmturbo.proactivesupport.DataMetricSummary;

@Immutable
@ThreadSafe
public class ProjectedRealtimeTopology {
    private final long topologyId;

    private final TopologyInfo originalTopologyInfo;

    private final Map<Long, ProjectedEntity> projectedEntities;

    private final Map<Integer, Set<Long>> entitiesByType;

    private ProjectedRealtimeTopology(final long topologyId,
                                      final TopologyInfo originalTopologyInfo,
                                      final Map<Long, ProjectedEntity> projectedEntities,
                                      final Map<Integer, Set<Long>> entitiesByType) {
        this.topologyId = topologyId;
        this.originalTopologyInfo = originalTopologyInfo;
        this.projectedEntities = projectedEntities;
        this.entitiesByType = entitiesByType;
    }

    public long getTopologyId() {
        return topologyId;
    }

    @Nonnull
    public TopologyInfo getOriginalTopologyInfo() {
        return originalTopologyInfo;
    }

    public int size() {
        return projectedEntities.size();
    }

    @Nonnull
    public Stream<TopologyEntityDTO> getEntities(@Nonnull final Set<Long> entityIds,
                                                 @Nonnull final Set<Integer> targetTypes) {
        final Stream<Long> oids;
        if (!targetTypes.isEmpty()) {
            oids = targetTypes.stream()
                .flatMap(type ->
                        entitiesByType.getOrDefault(type, Collections.emptySet()).stream());
        } else {
            oids = projectedEntities.keySet().stream();
        }

        return oids.filter(oid -> entityIds.isEmpty() || entityIds.contains(oid))
            .map(projectedEntities::get)
            .filter(Objects::nonNull)
            .map(ProjectedEntity::getEntity)
            .filter(Objects::nonNull);
    }

    public void collectDiags(@Nonnull DiagnosticsAppender appender) throws DiagnosticsException {
        JsonFormat.Printer printer = JsonFormat.printer().omittingInsignificantWhitespace();
        for (ProjectedEntity entity: projectedEntities.values()) {
            try {
                final String str = printer.print(entity.getEntity());
                appender.appendString(str);
            } catch (InvalidProtocolBufferException e) {
                throw new DiagnosticsException("Failed to serialize entity " + entity, e);
            }
        }
    }

    public static class ProjectedTopologyBuilder {
        private static final Logger logger = LogManager.getLogger();

        /**
         * The size of the compression buffer in the most recently completed projected topology
         * builder. We assume that (in general) topologies stay roughly the same size once targets
         * are added, so we can use the last topology's buffer size to avoid unnecessary allocations
         * on the next one.
         *
         * Note - the buffer will be relavitely small - it is bounded by the largest
         * entity in the topology.
         */
        private static volatile int sharedBufferSize = 0;

        private final long topologyId;

        private final Consumer<ProjectedRealtimeTopology> onFinish;

        private final TopologyInfo originalTopologyInfo;

        private final Map<Long, ProjectedEntity> projectedEntities = new HashMap<>();

        private final Map<Integer, Set<Long>> entitiesByType = new HashMap<>();

        private final SharedByteBuffer compressionBuffer;

        private final Stopwatch builderStopwatch = Stopwatch.createUnstarted();

        ProjectedTopologyBuilder(@Nonnull final Consumer<ProjectedRealtimeTopology> onFinish,
                                 final long topologyId,
                                 @Nonnull final TopologyInfo originalTopologyInfo) {
            this.topologyId = topologyId;
            this.onFinish = onFinish;
            this.originalTopologyInfo = originalTopologyInfo;
            this.compressionBuffer = new SharedByteBuffer(sharedBufferSize);
        }

        @Nonnull
        public ProjectedTopologyBuilder addEntities(@Nonnull final Collection<ProjectedTopologyEntity> entities) {
            builderStopwatch.start();
            entities.forEach(entity -> {
                entitiesByType.computeIfAbsent(
                    entity.getEntity().getEntityType(), k -> new HashSet<>())
                        .add(entity.getEntity().getOid());
                projectedEntities.put(entity.getEntity().getOid(),
                    new ProjectedEntity(entity, compressionBuffer));
            });
            builderStopwatch.stop();
            return this;
        }

        @Nonnull
        public ProjectedRealtimeTopology finish() {
            builderStopwatch.start();
            final ProjectedRealtimeTopology projectedRealtimeTopology =
                new ProjectedRealtimeTopology(topologyId, originalTopologyInfo, projectedEntities, entitiesByType);
            onFinish.accept(projectedRealtimeTopology);
            builderStopwatch.stop();
            final long elapsedSec = builderStopwatch.elapsed(TimeUnit.SECONDS);
            Metrics.CONSTRUCTION_TIME_SUMMARY.observe((double)elapsedSec);
            logger.info("Spent total of {}s to construct projected realtime topology.", elapsedSec);
            // Update the shared buffer size, so the next projected topology can use it
            // as a starting point.
            sharedBufferSize = compressionBuffer.getSize();
            return projectedRealtimeTopology;
        }
    }

    private static class ProjectedEntity {
        private static final Logger logger = LogManager.getLogger();

        private final double originalPriceIdx;
        private final double projectedPriceIdx;
        private final byte[] compressedDto;
        private final int uncompressedLength;

        ProjectedEntity(@Nonnull final ProjectedTopologyEntity projectedTopologyEntity,
                        @Nonnull final SharedByteBuffer sharedByteBuffer) {
            this.originalPriceIdx = projectedTopologyEntity.getOriginalPriceIndex();
            this.projectedPriceIdx = projectedTopologyEntity.getOriginalPriceIndex();

            // Use the fastest java instance to avoid using JNI & off-heap memory.
            final LZ4Compressor compressor = LZ4Factory.fastestJavaInstance().fastCompressor();

            final byte[] uncompressedBytes = projectedTopologyEntity.getEntity().toByteArray();
            uncompressedLength = uncompressedBytes.length;
            final int maxCompressedLength = compressor.maxCompressedLength(uncompressedLength);
            final byte[] compressionBuffer = sharedByteBuffer.getBuffer(maxCompressedLength);
            final int compressedLength = compressor.compress(uncompressedBytes, compressionBuffer);
            this.compressedDto = Arrays.copyOf(compressionBuffer, compressedLength);
        }

        double getOriginalPriceIdx() {
            return originalPriceIdx;
        }

        double getProjectedPriceIdx() {
            return projectedPriceIdx;
        }

        @Nullable
        TopologyEntityDTO getEntity() {
            // Use the fastest available java instance to avoid using off-heap memory.
            final LZ4FastDecompressor decompressor = LZ4Factory.fastestJavaInstance().fastDecompressor();
            try {
                return TopologyEntityDTO.parseFrom(decompressor.decompress(compressedDto, uncompressedLength));
            } catch (InvalidProtocolBufferException e) {
                logger.error("Failed to decompress entity. Error: {}", e.getMessage());
                return null;
            }
        }
    }

    private static class Metrics {
        private static final DataMetricSummary CONSTRUCTION_TIME_SUMMARY = DataMetricSummary.builder()
            .withName("repo_projected_realtime_construction_seconds")
            .withHelp("Total time taken to build the projected realtime topology.")
            .build()
            .register();
    }
}
