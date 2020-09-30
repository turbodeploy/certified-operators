package com.vmturbo.repository.plan.db;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.collect.Collections2;
import com.google.common.collect.Iterators;
import com.google.gson.JsonObject;

import org.jooq.BatchBindStep;
import org.jooq.DSLContext;

import com.vmturbo.common.protobuf.repository.RepositoryDTO.TopologyType;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChain;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChain.Builder;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopologyEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.components.api.CompressedProtobuf;
import com.vmturbo.components.api.FormattedString;
import com.vmturbo.repository.db.Tables;
import com.vmturbo.repository.plan.db.RepoPlanTopology.RepoPlanTopologyBuilder;
import com.vmturbo.repository.topology.TopologyID;
import com.vmturbo.sql.utils.jooq.JooqUtil;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.graph.supplychain.SupplyChainCalculator;
import com.vmturbo.topology.graph.supplychain.TraversalRulesLibrary;

/**
 * Responsible for ingesting the projected topology.
 */
class MySQLProjectedTopologyCreator extends MySQLTopologyCreator<ProjectedTopologyEntity> {
    /**
     * Traversal rules for the production of a scoped supply chain.
     */
    private static final TraversalRulesLibrary<RepoPlanGraphEntity> TRAVERSAL_RULES_LIBRARY =
            new TraversalRulesLibrary<>();

    private static final String PRICE_INDEX_STAGE = "price_index_ingestion";
    private static final String GRAPH_CONSTRUCTION = "graph_construction";
    private static final String PLAN_ENTITY_SCOPE_CALCULATION = "scope_calculation";
    private static final String PLAN_ENTITY_SCOPE_INGESTION = "scope_ingestion";

    private final RepoPlanTopologyBuilder repoPlanTopologyBuilder;
    private final SupplyChainCalculator supplyChainCalculator;

    private final CompressionStatsByType scopeCompressionByType = new CompressionStatsByType();

    MySQLProjectedTopologyCreator(final long projectedTopologyId,
            final TopologyInfo topologyInfo,
            final DSLContext dsl,
            final int insertionChunkSize,
            final int deletionChunkSize,
            SupplyChainCalculator supplyChainCalculator) {
        super(topologyInfo, projectedTopologyId, TopologyType.PROJECTED, dsl, insertionChunkSize, deletionChunkSize);
        repoPlanTopologyBuilder = new RepoPlanTopologyBuilder(topologyInfo, t -> {
            // No op on finish.
        });
        this.supplyChainCalculator = supplyChainCalculator;
    }

    @Override
    public void addEntities(final Collection<ProjectedTopologyEntity> entities, TopologyID tid) {
        // Add the regular entity entries.
        super.addEntities(entities, tid);
        addPriceIndexEntries(entities, tid);
    }

    private void addPriceIndexEntries(final Collection<ProjectedTopologyEntity> entities,
            TopologyID tid) {
        if (!entities.isEmpty()) {
            timer.start(PRICE_INDEX_STAGE);
            Iterators.partition(entities.iterator(), insertionChunkSize).forEachRemaining(entityChunk -> {
                BatchBindStep priceIndexBind = dsl.batch(
                        dsl.insertInto(Tables.PRICE_INDEX, Tables.PRICE_INDEX.TOPOLOGY_OID,
                                Tables.PRICE_INDEX.ENTITY_OID, Tables.PRICE_INDEX.ORIGINAL,
                                Tables.PRICE_INDEX.PROJECTED).values((Long)null, null, null, null));
                for (ProjectedTopologyEntity e : entityChunk) {
                    priceIndexBind = priceIndexBind.bind(topologyId, e.getEntity().getOid(),
                            e.getOriginalPriceIndex(),
                            e.getProjectedPriceIndex());
                    // Add the entity to the topology graph builder for the graph we are
                    // constructing to calculate supply chains.
                    repoPlanTopologyBuilder.addEntity(e.getEntity());
                }
                priceIndexBind.execute();
            });
            timer.stop();
        }
    }

    @Override
    protected CompressionStats getTotalCompressionStats() {
        return CompressionStats.combine(Stream.of(compressionStatsByType.getTotal(),
                scopeCompressionByType.getTotal()));
    }

    @Override
    public void complete() {
        if (numEntities > 0) {
            logger.info("Starting supply chain calculation on {} entities.",
                    numEntities);
            timer.start(GRAPH_CONSTRUCTION);
            RepoPlanTopology repoPlanTopology = repoPlanTopologyBuilder.finish();
            TopologyGraph<RepoPlanGraphEntity> graph = repoPlanTopology.entityGraph();
            Iterators.partition(graph.entities().iterator(), insertionChunkSize).forEachRemaining(
                    entityChunk -> {
                        timer.start(PLAN_ENTITY_SCOPE_CALCULATION);
                        BatchBindStep supplyChainBind = dsl.batch(dsl.insertInto(
                                Tables.PLAN_ENTITY_SCOPE, Tables.PLAN_ENTITY_SCOPE.TOPOLOGY_OID,
                                Tables.PLAN_ENTITY_SCOPE.ENTITY_OID,
                                Tables.PLAN_ENTITY_SCOPE.SUPPLY_CHAIN,
                                Tables.PLAN_ENTITY_SCOPE.SUPPLY_CHAIN_UNCOMPRESSED_SIZE)
                                .values((Long)null, null, null, null));
                        for (RepoPlanGraphEntity e : entityChunk) {
                            final Map<Integer, SupplyChainNode> ret =
                                    supplyChainCalculator.getSupplyChainNodes(graph,
                                            Collections.singleton(e.getOid()), x -> true,
                                            TRAVERSAL_RULES_LIBRARY);
                            final CompressedProtobuf<SupplyChain, Builder> compressedSc =
                                    CompressedProtobuf.compress(SupplyChain.newBuilder()
                                            .addAllSupplyChainNodes(ret.values())
                                            .build(), sharedByteBuffer);
                            scopeCompressionByType.recordEntity(
                                    ApiEntityType.fromType(e.getEntityType()), compressedSc);
                            supplyChainBind = supplyChainBind.bind(topologyId, e.getOid(),
                                    compressedSc.getCompressedBytes(),
                                    compressedSc.getUncompressedLength());
                        }
                        timer.start(PLAN_ENTITY_SCOPE_INGESTION);
                        supplyChainBind.execute();
                        timer.stop();
                    });
        }

        super.complete();
    }

    @Nonnull
    @Override
    protected JsonObject summaryForDb() {
        final CompressionStats entityCompression = compressionStatsByType.getTotal();
        final CompressionStats scCompression = scopeCompressionByType.getTotal();
        JsonObject object = new JsonObject();
        object.addProperty("ingestion_duration", timer.toString());
        object.add("entity_compression", entityCompression.toJson());
        object.add("scope_compression", scCompression.toJson());
        return object;
    }

    @Override
    protected String compressionSummaryForLog() {
        final CompressionStats entityCompression = compressionStatsByType.getTotal();
        final CompressionStats scCompression = scopeCompressionByType.getTotal();
        return FormattedString.format("Entity compression: {} Scope compression: {}\n"
                        + "Entity compression stats by type: {}\n"
                        + "Scope compression stats by type: {}\n", entityCompression, scCompression,
                compressionStatsByType, scopeCompressionByType);
    }

    @Override
    public void rollback() {
        // The scope and price index entries are unique to the projected topology.
        final int scopeDeleted = JooqUtil.deleteInChunks(dsl.deleteFrom(Tables.PLAN_ENTITY_SCOPE)
                .where(Tables.PLAN_ENTITY_SCOPE.TOPOLOGY_OID.eq(topologyId))
                .orderBy(Tables.PLAN_ENTITY_SCOPE.ENTITY_OID), deletionChunkSize);
        final int piDeleted = JooqUtil.deleteInChunks(dsl.deleteFrom(Tables.PRICE_INDEX)
                .where(Tables.PRICE_INDEX.TOPOLOGY_OID.eq(topologyId))
                .orderBy(Tables.PRICE_INDEX.ENTITY_OID), deletionChunkSize);

        logger.info("Deleted {} scope rows and {} price index rows for {} topology {} in context {}",
                scopeDeleted, piDeleted, topologyType, topologyId, planId);

        // Do the rest of the rollback after the projected-topology-specific deletions,
        // because we still want to delete the topology metadata last.
        super.rollback();
    }

    @Override
    protected Collection<TopologyEntityDTO> extractEntities(
            Collection<ProjectedTopologyEntity> entities) {
        return Collections2.transform(entities, ProjectedTopologyEntity::getEntity);
    }
}
