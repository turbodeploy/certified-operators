package com.vmturbo.repository.listener.realtime;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.ThreadSafe;

import com.google.gson.Gson;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;

import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopologyEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.components.common.diagnostics.DiagnosticsException;
import com.vmturbo.components.common.diagnostics.StreamingDiagnosable;

@Immutable
@ThreadSafe
public class ProjectedRealtimeTopology implements StreamingDiagnosable {
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
                .flatMap(type -> entitiesByType.get(type).stream());
        } else {
            oids = projectedEntities.keySet().stream();
        }

        return oids.filter(oid -> entityIds.isEmpty() || entityIds.contains(oid))
            .map(projectedEntities::get)
            .filter(Objects::nonNull)
            .map(ProjectedEntity::getEntity)
            .filter(Objects::nonNull);
    }

    @Nonnull
    @Override
    public Stream<String> collectDiags() throws DiagnosticsException {
        JsonFormat.Printer printer = JsonFormat.printer().omittingInsignificantWhitespace();
        final Gson gson = ComponentGsonFactory.createGsonNoPrettyPrint();
        return projectedEntities.values().stream()
            .map(ProjectedEntity::getEntity)
            .map(entity -> {
                try {
                    return printer.print(entity);
                } catch (InvalidProtocolBufferException e) {
                    return null;
                }
            })
            .filter(Objects::nonNull);
    }

    @Override
    public void restoreDiags(@Nonnull final Stream<String> collectedDiags) throws DiagnosticsException {
        // Restoring diags not supported for now.
        //
        // It's not an important use case - we typically restore diags to Topology Processor and
        // broadcast.
    }

    public static class ProjectedTopologyBuilder {
        private final long topologyId;

        private final Consumer<ProjectedRealtimeTopology> onFinish;

        private final TopologyInfo originalTopologyInfo;

        private final Map<Long, ProjectedEntity> projectedEntities = new HashMap<>();

        private final Map<Integer, Set<Long>> entitiesByType = new HashMap<>();

        ProjectedTopologyBuilder(@Nonnull final Consumer<ProjectedRealtimeTopology> onFinish,
                                 final long topologyId,
                                 @Nonnull final TopologyInfo originalTopologyInfo) {
            this.topologyId = topologyId;
            this.onFinish = onFinish;
            this.originalTopologyInfo = originalTopologyInfo;
        }

        @Nonnull
        public ProjectedTopologyBuilder addEntities(@Nonnull final Collection<ProjectedTopologyEntity> entities) {
            entities.forEach(entity -> {
                entitiesByType.computeIfAbsent(
                    entity.getEntity().getEntityType(), k -> new HashSet<>())
                        .add(entity.getEntity().getOid());
                projectedEntities.put(entity.getEntity().getOid(), new ProjectedEntity(entity));
            });
            return this;
        }

        @Nonnull
        public ProjectedRealtimeTopology finish() {
            final ProjectedRealtimeTopology projectedRealtimeTopology =
                new ProjectedRealtimeTopology(topologyId, originalTopologyInfo, projectedEntities, entitiesByType);
            onFinish.accept(projectedRealtimeTopology);
            return projectedRealtimeTopology;
        }
    }

    private static class ProjectedEntity {

        private final double originalPriceIdx;
        private final double projectedPriceIdx;
        private final byte[] compressedDto;

        ProjectedEntity(@Nonnull final ProjectedTopologyEntity projectedTopologyEntity) {
            this.originalPriceIdx = projectedTopologyEntity.getOriginalPriceIndex();
            this.projectedPriceIdx = projectedTopologyEntity.getOriginalPriceIndex();
            byte[] bytes = null;
            try {
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                GZIPOutputStream gos = new GZIPOutputStream(bos);
                gos.write(projectedTopologyEntity.getEntity().toByteArray());
                gos.close();
                bytes = bos.toByteArray();
            } catch (IOException e) {
                // boo
            }
            this.compressedDto = bytes;
        }

        double getOriginalPriceIdx() {
            return originalPriceIdx;
        }

        double getProjectedPriceIdx() {
            return projectedPriceIdx;
        }

        @Nullable
        TopologyEntityDTO getEntity() {
            try {
                ByteArrayInputStream bis = new ByteArrayInputStream(compressedDto);
                GZIPInputStream zis = new GZIPInputStream(bis);
                return TopologyEntityDTO.parseFrom(zis);
            } catch (IOException e) {
                return null;
            }
        }
    }
}
