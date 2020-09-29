package com.vmturbo.repository.plan.db;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.protobuf.InvalidProtocolBufferException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Record1;

import com.vmturbo.common.protobuf.repository.RepositoryDTO.TopologyType;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChain;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode.MemberList;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainScope;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.Type;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopologyEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.components.api.BulkErrorCount;
import com.vmturbo.components.api.CompressedProtobuf;
import com.vmturbo.components.api.FormattedString;
import com.vmturbo.repository.db.Tables;
import com.vmturbo.repository.db.tables.records.TopologyMetadataRecord;
import com.vmturbo.repository.service.PartialEntityConverter;
import com.vmturbo.repository.topology.TopologyLifecycleManager.TopologyCreator;
import com.vmturbo.sql.utils.jooq.JooqUtil;
import com.vmturbo.topology.graph.supplychain.SupplyChainCalculator;

/**
 * Responsible for the storage and retrieval of plan source and projected topologies in
 * MySQL.
 *
 * <p/>For the time being we keep everything in once class to abstract away the details of the
 * database schema, which drives both the ingestion and the query implementation.
 *
 * <p/>We store the source and projected {@link TopologyEntityDTO} blobs. We also calculate the
 * supply chains of each entity in the projected topology and store them.
 */
public class MySQLPlanEntityStore implements PlanEntityStore {
    private static final Logger logger = LogManager.getLogger();
    private final DSLContext dsl;
    private final PartialEntityConverter partialEntityConverter;
    private final int insertionChunkSize;
    private final int deletionChunkSize;
    private final SupplyChainCalculator supplyChainCalculator;

    /**
     * Public constructor; should be called from a Spring configuration.
     *
     * @param dsl {@link DSLContext} to access the database.
     * @param partialEntityConverter {@link PartialEntityConverter} to convert {@link TopologyEntityDTO}s
     *     to the right type of {@link PartialEntity}.
     * @param supplyChainCalculator The {@link SupplyChainCalculator} used to calculate supply
     *     chains for entities in the plan. We use this calculator so that the supply chains we
     *     store for plans are calculated using the same rules we have in realtime.
     * @param insertionChunkSize Batch insert chunk size for ingestion.
     * @param deletionChunkSize Batch delete chunk size for plan deletion.
     */
    public MySQLPlanEntityStore(final DSLContext dsl,
            final PartialEntityConverter partialEntityConverter,
            final SupplyChainCalculator supplyChainCalculator,
            final int insertionChunkSize,
            final int deletionChunkSize) {
        this.dsl = dsl;
        this.partialEntityConverter = partialEntityConverter;
        this.insertionChunkSize = insertionChunkSize;
        this.deletionChunkSize = deletionChunkSize;
        this.supplyChainCalculator = supplyChainCalculator;
    }

    /**
     * Create a new {@link TopologyCreator} which is used in the topology lifecycle
     * manager to ingest topologies.
     *
     * @param topologyInfo The {@link TopologyInfo} of the source topology.
     * @return The {@link TopologyCreator}.
     */
    public TopologyCreator<TopologyEntityDTO> newSourceTopologyCreator(TopologyInfo topologyInfo) {
        return new MySQLSourceTopologyCreator(dsl, topologyInfo, insertionChunkSize, deletionChunkSize);
    }

    /**
     * Create a new {@link TopologyCreator} which is used in the topology lifecycle
     * manager to ingest projected topologies.
     *
     * @param projectedTopologyId The id of the projected topology.
     * @param topologyInfo The {@link TopologyInfo} of the source topology.
     * @return The {@link TopologyCreator}.
     */
    public TopologyCreator<ProjectedTopologyEntity> newProjectedTopologyCreator(final long projectedTopologyId, TopologyInfo topologyInfo) {
        return new MySQLProjectedTopologyCreator(projectedTopologyId, topologyInfo, dsl,
                insertionChunkSize, deletionChunkSize, supplyChainCalculator);
    }

    @Nonnull
    @Override
    public Iterator<List<ProjectedTopologyEntity>> getHackyStatsEntities(
            @Nonnull final TopologySelection topologySelection,
            @Nonnull final PlanEntityFilter planEntityFilter) {
        // We don't get the conditions fro the entity filter, because the PlanStatsService needs more than just the
        // requested entities to calculate stuff like densities and so on.
        // TODO - we should factor those dependencies in in the query to avoid retrieving the whole topology each time.
        List<Condition> conditions = new ArrayList<>();
        conditions.add(Tables.PLAN_ENTITY.TOPOLOGY_OID.eq(topologySelection.getTopologyId()));
        return getEntitiesWithPriceIndex(conditions)
            .map(bldr -> Collections.singletonList(bldr.build()))
            .iterator();
    }

    @Nonnull
    @Override
    public TopologySelection getTopologySelection(final long planId, @Nonnull TopologyType topologyType) throws TopologyNotFoundException {
        return dsl.selectFrom(Tables.TOPOLOGY_METADATA)
            .where(Tables.TOPOLOGY_METADATA.CONTEXT_OID.eq(planId)
                .and(Tables.TOPOLOGY_METADATA.TOPOLOGY_TYPE.eq((byte)topologyType.getNumber()))
                .and(Tables.TOPOLOGY_METADATA.STATUS.eq(TopologyStatus.INGESTION_COMPLETED.getNum())))
            .fetch().stream().findFirst()
            .map(record -> new TopologySelection(record.getContextOid(),
                    TopologyType.forNumber(record.getTopologyType()),
                    record.getTopologyOid()))
            .orElseThrow(() -> new TopologyNotFoundException(planId, topologyType));
    }

    @Nonnull
    @Override
    public TopologySelection getTopologySelection(final long topologyId) throws TopologyNotFoundException {
        return dsl.selectFrom(Tables.TOPOLOGY_METADATA)
                .where(Tables.TOPOLOGY_METADATA.TOPOLOGY_OID.eq(topologyId)
                    .and(Tables.TOPOLOGY_METADATA.STATUS.eq(TopologyStatus.INGESTION_COMPLETED.getNum())))
                .fetch().stream().findFirst()
                .map(record -> new TopologySelection(record.getContextOid(),
                        TopologyType.forNumber(record.getTopologyType()),
                        record.getTopologyOid()))
                .orElseThrow(() -> new TopologyNotFoundException(topologyId));
    }

    @Nonnull
    private static List<Condition> filterToConditions(@Nonnull final TopologySelection topologySelection,
            @Nonnull final PlanEntityFilter planEntityFilter) {
        final List<Condition> conditions = new ArrayList<>(4);
        conditions.add(Tables.PLAN_ENTITY.TOPOLOGY_OID.eq(topologySelection.getTopologyId()));

        planEntityFilter.getTargetEntities().ifPresent(targetEntities -> {
            conditions.add(Tables.PLAN_ENTITY.ENTITY_OID.in(targetEntities));
        });

        planEntityFilter.getTargetTypes().ifPresent(types -> {
            conditions.add(Tables.PLAN_ENTITY.ENTITY_TYPE.in(types));
        });

        if (planEntityFilter.getUnplacedOnly()) {
            conditions.add(Tables.PLAN_ENTITY.IS_PLACED.eq((byte)0));
        }

        return conditions;
    }

    private Stream<TopologyEntityDTO.Builder> getEntities(List<Condition> conditions) {
        // TODO: Use fetchLazy() or a .stream() to return a cursor instead of building
        // results up in memory.
        conditions.add(Tables.PLAN_ENTITY.ENTITY.isNotNull());
        return dsl.selectFrom(Tables.PLAN_ENTITY)
            .where(conditions)
            .fetch().stream()
            .map(this::getDecompressedEntity)
            .filter(Objects::nonNull);
    }

    private Stream<ProjectedTopologyEntity.Builder> getEntitiesWithPriceIndex(List<Condition> conditions) {
        conditions.add(Tables.PLAN_ENTITY.ENTITY.isNotNull());
        // We do a left outer join so that entities with no price index entry will still be returned.
        return dsl.selectFrom(Tables.PLAN_ENTITY.leftOuterJoin(Tables.PRICE_INDEX)
            .on(Tables.PLAN_ENTITY.TOPOLOGY_OID.eq(Tables.PRICE_INDEX.TOPOLOGY_OID))
                .and(Tables.PLAN_ENTITY.ENTITY_OID.eq(Tables.PRICE_INDEX.ENTITY_OID)))
            .where(conditions)
            .fetch().stream()
            .map(r -> {
                TopologyEntityDTO.Builder entity = getDecompressedEntity(r);
                if (entity != null) {
                    ProjectedTopologyEntity.Builder bldr = ProjectedTopologyEntity.newBuilder()
                        .setEntity(entity);
                    // When looking up the SOURCE topology there will be no price index entries,
                    // because they are stored for the projected topology only. Right now we don't
                    // need price index information with the source topology. If we need it, we
                    // can look up the projected topology ID given the source topology ID, and
                    // look up the price index records that way - or store the source topology id
                    // with the price index table.
                    if (r.get(Tables.PRICE_INDEX.ORIGINAL) != null) {
                        bldr.setOriginalPriceIndex(r.get(Tables.PRICE_INDEX.ORIGINAL).doubleValue());
                    }
                    if (r.get(Tables.PRICE_INDEX.PROJECTED) != null) {
                        bldr.setProjectedPriceIndex(r.get(Tables.PRICE_INDEX.PROJECTED).doubleValue());
                    }
                    return bldr;
                } else {
                    return null;
                }
            })
            .filter(Objects::nonNull);
    }

    @Nullable
    private TopologyEntityDTO.Builder getDecompressedEntity(Record r) {
        byte[] compressedEntity = r.get(Tables.PLAN_ENTITY.ENTITY);
        int uncompressedLength = r.get(Tables.PLAN_ENTITY.ENTITY_UNCOMPRESSED_SIZE);
        CompressedProtobuf<TopologyEntityDTO, TopologyEntityDTO.Builder> c = new CompressedProtobuf<>(compressedEntity, uncompressedLength);
        TopologyEntityDTO.Builder eBldr = TopologyEntityDTO.newBuilder();
        try {
            c.decompressInto(eBldr);
            return eBldr;
        } catch (InvalidProtocolBufferException e) {
            logger.warn("Unable to decompress entity due to error: " + e.toString());
            return null;
        }
    }

    @Nonnull
    @Override
    public Stream<PartialEntity> getPlanEntities(@Nonnull final TopologySelection topologySelection,
            @Nonnull final PlanEntityFilter planEntityFilter, @Nonnull final Type partialEntityType) {
        List<Condition> conditions = filterToConditions(topologySelection, planEntityFilter);

        // TODO - for minimal types we can keep info out of the blobs.
        return getEntities(conditions)
            .map(e -> partialEntityConverter.createPartialEntity(e, partialEntityType));
    }

    @Nonnull
    @Override
    public SupplyChain getSupplyChain(@Nonnull final TopologySelection topologySelection, @Nonnull final SupplyChainScope supplyChainScope) {
        if (topologySelection.getTopologyType() == TopologyType.SOURCE) {
            throw new IllegalArgumentException("Queries for source supply chain not supported in plans.");
        }

        // Normally we expect specific seed entities.
        List<Condition> conditions = new ArrayList<>();
        conditions.add(Tables.PLAN_ENTITY_SCOPE.TOPOLOGY_OID.eq(topologySelection.getTopologyId()));
        if (supplyChainScope.getStartingEntityOidList().isEmpty()) {
            logger.warn("Unexpected global supply chain request in plan. This will be slow.");
        } else {
            conditions.add(Tables.PLAN_ENTITY_SCOPE.ENTITY_OID.in(supplyChainScope.getStartingEntityOidList()));
        }

        Map<Integer, Map<Integer, Set<Long>>> membersByTypeAndState = new HashMap<>();
        try (BulkErrorCount decompressionErrorCnt = new BulkErrorCount(logger, "Supplychain Decompression")) {
            dsl.selectFrom(Tables.PLAN_ENTITY_SCOPE).where(conditions).fetch().forEach(scopeRecord -> {
                if (scopeRecord.getSupplyChain() == null || scopeRecord.getSupplyChainUncompressedSize() == null) {
                    logger.debug("Skipping empty record for topology id {} entity id {}",
                            scopeRecord.getTopologyOid(), scopeRecord.getEntityOid());
                } else {
                    final CompressedProtobuf<SupplyChain, SupplyChain.Builder> c = new CompressedProtobuf<>(scopeRecord.getSupplyChain(),
                            scopeRecord.getSupplyChainUncompressedSize());
                    SupplyChain.Builder scBldr = SupplyChain.newBuilder();
                    try {
                        c.decompressInto(scBldr);
                        scBldr.getSupplyChainNodesList().forEach(n -> {
                            final Map<Integer, Set<Long>> membersByState = membersByTypeAndState.computeIfAbsent(n.getEntityType(),
                                    k -> new HashMap<>());
                            n.getMembersByStateMap().forEach((state, memberList) -> {
                                membersByState.computeIfAbsent(state, k -> new HashSet<>()).addAll(memberList.getMemberOidsList());
                            });
                        });
                        decompressionErrorCnt.recordSuccess();
                    } catch (InvalidProtocolBufferException e) {
                        decompressionErrorCnt.recordError(e);
                    }
                }
            });
        }

        Set<Integer> typesToInclude = new HashSet<>(supplyChainScope.getEntityTypesToIncludeList());
        Set<Integer> acceptableStates = supplyChainScope.getEntityStatesToIncludeList().stream()
            .map(EntityState::getNumber)
            .collect(Collectors.toSet());
        SupplyChain.Builder retSupplyChain = SupplyChain.newBuilder();
        membersByTypeAndState.forEach((type, membersByState) -> {
            if (typesToInclude.isEmpty() || typesToInclude.contains(type)) {
                SupplyChainNode.Builder nodeBldr = SupplyChainNode.newBuilder()
                    .setEntityType(type);
                membersByState.forEach((state, memberIds) -> {
                    if (acceptableStates.isEmpty() || acceptableStates.contains(state)) {
                        nodeBldr.putMembersByState(state, MemberList.newBuilder()
                            .addAllMemberOids(memberIds)
                            .build());
                    }
                });
                retSupplyChain.addSupplyChainNodes(nodeBldr.build());
            }
        });

        logger.debug("Returning supply chain with members: {}", membersByTypeAndState);
        return retSupplyChain.build();
    }

    @Override
    public void deletePlanData(final long planId) {
        final List<TopologyMetadataRecord> metadataRecords = dsl.selectFrom(Tables.TOPOLOGY_METADATA)
            .where(Tables.TOPOLOGY_METADATA.CONTEXT_OID.eq(planId))
            .fetch();

        if (metadataRecords.isEmpty()) {
            logger.info("No topologies found for plan {}. Not deleting anything.", planId);
            return;
        }

        long totalDeleted = 0;
        for (TopologyMetadataRecord metadataRecord : metadataRecords) {
            final long topologyId = metadataRecord.getTopologyOid();

            // Record that we are deleting the topology.
            dsl.update(Tables.TOPOLOGY_METADATA)
                .set(Tables.TOPOLOGY_METADATA.STATUS, TopologyStatus.DELETION_STARTED.getNum())
                .where(Tables.TOPOLOGY_METADATA.TOPOLOGY_OID.eq(topologyId))
                .execute();

            // We delete plan data in chunks to avoid overly large transactions.
            // We move through the various tables, and continue deleting until there are no more
            // entries to delete. Last of all, we delete the entry from TOPOLOGY_METADATA.
            // If any of these operations fail, the entry for the plan will remain in the
            // TOPOLOGY_METADATA table and we will reattempt the delete at the next plan garbage collection
            // interval.
            final String topologyName = FormattedString.format("{} topology (id: {}) in plan {}",
                    TopologyType.forNumber(metadataRecord.getTopologyType().intValue()),
                    topologyId, planId);
            logger.info("Deleting topology data for {}", topologyName);
            final int deletedPriceIndex = JooqUtil.deleteInChunks(dsl.deleteFrom(Tables.PRICE_INDEX)
                    .where(Tables.PRICE_INDEX.TOPOLOGY_OID.eq(topologyId))
                    .orderBy(Tables.PRICE_INDEX.ENTITY_OID), deletionChunkSize);
            totalDeleted += deletedPriceIndex;
            logger.debug("Deleted {} price index entries ({})", deletedPriceIndex, topologyName);
            final int deletedScope = JooqUtil.deleteInChunks(dsl.deleteFrom(Tables.PLAN_ENTITY_SCOPE)
                    .where(Tables.PLAN_ENTITY_SCOPE.TOPOLOGY_OID.eq(topologyId))
                    .orderBy(Tables.PLAN_ENTITY_SCOPE.ENTITY_OID), deletionChunkSize);
            totalDeleted += deletedScope;
            logger.debug("Deleted {} scope entries ({})", deletedScope, topologyName);
            final int deletedSrc = JooqUtil.deleteInChunks(dsl.deleteFrom(Tables.PLAN_ENTITY)
                    .where(Tables.PLAN_ENTITY.TOPOLOGY_OID.eq(topologyId))
                    .orderBy(Tables.PLAN_ENTITY.ENTITY_OID), deletionChunkSize);
            totalDeleted += deletedSrc;
            logger.debug("Deleted {} entities ({})", deletedSrc, topologyName);

            // After this delete finishes, we won't list this plan as a registered plan anymore.
            dsl.deleteFrom(Tables.TOPOLOGY_METADATA)
                    .where(Tables.TOPOLOGY_METADATA.TOPOLOGY_OID.eq(topologyId))
                    .execute();
        }

        logger.info("Finished deleting plan data for plan {}. Total: {} rows", planId, totalDeleted);
    }

    @Override
    @Nonnull
    public Set<Long> listRegisteredPlans() {
        return dsl.selectDistinct(Tables.TOPOLOGY_METADATA.CONTEXT_OID)
                .from(Tables.TOPOLOGY_METADATA)
                .fetch().stream()
            .map(Record1::component1)
            .collect(Collectors.toSet());
    }
}
