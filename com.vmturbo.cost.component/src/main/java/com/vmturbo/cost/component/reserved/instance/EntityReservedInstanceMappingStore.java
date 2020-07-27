package com.vmturbo.cost.component.reserved.instance;

import static com.vmturbo.cost.component.db.Tables.ENTITY_TO_RESERVED_INSTANCE_MAPPING;
import static com.vmturbo.cost.component.db.Tables.HIST_ENTITY_RESERVED_INSTANCE_MAPPING;

import static org.jooq.impl.DSL.sum;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Table;

import org.apache.commons.collections4.MultiValuedMap;
import org.apache.commons.collections4.multimap.HashSetValuedHashMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.UpdatableRecord;
import org.jooq.impl.DSL;
import org.stringtemplate.v4.ST;

import com.vmturbo.common.protobuf.cost.Cost.EntityReservedInstanceCoverage;
import com.vmturbo.common.protobuf.cost.Cost.UploadRIDataRequest.EntityRICoverageUpload;
import com.vmturbo.common.protobuf.cost.Cost.UploadRIDataRequest.EntityRICoverageUpload.Coverage;
import com.vmturbo.common.protobuf.cost.Cost.UploadRIDataRequest.EntityRICoverageUpload.Coverage.RICoverageSource;
import com.vmturbo.components.common.diagnostics.DiagnosticsAppender;
import com.vmturbo.components.common.diagnostics.DiagnosticsException;
import com.vmturbo.components.common.diagnostics.DiagsRestorable;
import com.vmturbo.cost.component.db.Tables;
import com.vmturbo.cost.component.db.enums.EntityToReservedInstanceMappingRiSourceCoverage;
import com.vmturbo.cost.component.db.tables.pojos.EntityToReservedInstanceMapping;
import com.vmturbo.cost.component.db.tables.records.EntityToReservedInstanceMappingRecord;
import com.vmturbo.cost.component.db.tables.records.HistEntityReservedInstanceMappingRecord;
import com.vmturbo.cost.component.reserved.instance.filter.EntityReservedInstanceMappingFilter;

/**
 * This class responsible for storing the mapping relation between entity with reserved instance about
 * coupons coverage information. And the data is only comes from billing topology. For example:
 * VM1 use RI1 10 coupons, VM1 use RI2 20 coupons, VM2 use RI3 5 coupons.
 */
public class EntityReservedInstanceMappingStore implements DiagsRestorable {
    private static final Logger logger = LogManager.getLogger();
    private static final String entityReservedInstanceMappingFile = "entityToReserved_dump";

    private static final String RI_SUM_COUPONS = "RI_SUM_COUPONS";

    private static final String ENTITY_SUM_COUPONS = "ENTITY_SUM_COUPONS";

    private static final EntityReservedInstanceMappingFilter entityReservedInstanceMappingFilter = EntityReservedInstanceMappingFilter
            .newBuilder().build();

    private static final int chunkSize = 1000;

    private final DSLContext dsl;

    private static final String ENTITY_RI_COVERAGE_LOGGING_TEMPLATE_ENTITY_INFO =
            "\n================== NEW MAPPING =====================\n" +
                    "| Entity ID: <entityId>\n";
    private static final String ENTITY_RI_COVERAGE_LOGGING_TEMPLATE_COVERAGE_INFO =
            "============== COVERAGE INFORMATION ================\n" +
                    "| Reserved Instance ID: <riId>\n" +
                    "| Used Coupons : <usedCoupons>\n" +
                    "| Coverage Source : <coverageSource>\n";
    private static final String LOGGING_TEMPLATE_TERMINATE =
            "====================================================\n";

    private static final String RI_ENTITY_LOGGING_TEMPLATE_ENTITY_INFO =
            "\n================= NEW MAPPING ====================\n" +
                    "| Reserved Instance ID: <riId>\n";
    private static final String RI_ENTITY_COVERAGE_LOGGING_TEMPLATE_COVERAGE_INFO =
            "============== COVERAGE INFORMATION ================\n" +
                    "| Entity ID: <entityId>\n" +
                    "| Used Coupons : <usedCoupons>\n" +
                    "| Coverage Source : <coverageSource>\n";

    /**
     * Construct instance of EntityReservedInstanceMappingStore.
     *
     * @param dsl {@link DSLContext} transactional context.
     */
    public EntityReservedInstanceMappingStore(@Nonnull final DSLContext dsl) {
        this.dsl = dsl;
    }

    /**
     * Input a list of {@link EntityRICoverageUpload}, store them into the database table.
     *
     * @param context {@link DSLContext} transactional context (configured by client).
     * @param entityReservedInstanceCoverages a list of {@link EntityRICoverageUpload}.
     */
    public void updateEntityReservedInstanceMapping(@Nonnull final DSLContext context,
            @Nonnull final List<EntityRICoverageUpload> entityReservedInstanceCoverages) {
        final LocalDateTime currentTime = LocalDateTime.now(ZoneOffset.UTC);

        final List<EntityToReservedInstanceMappingRecord> records =
                entityReservedInstanceCoverages.stream()
                        .filter(entityCoverage -> !entityCoverage.getCoverageList().isEmpty())
                        .map(entityCoverage -> createEntityToRIMappingRecords(
                                context, currentTime,
                                entityCoverage.getEntityId(),
                                entityCoverage.getCoverageList()))
                        .flatMap(List::stream)
                        .collect(Collectors.toList());

        // Replace table with the latest RI mapping records.
        final int countDel = context.deleteFrom(ENTITY_TO_RESERVED_INSTANCE_MAPPING).execute();
        Lists.partition(records, chunkSize).forEach(
                entitiesChunk -> context.batchInsert(entitiesChunk).execute());
        logger.info("SE-RI-Mapping: Count of deleted records: {}, updated: {}",
                countDel, records.size());
    }

    /**
     * Inserts RI coverage records into historical Entity RI Coverage mapping table.
     *
     * @param context {@link DSLContext} transactional context (configured by client).
     * @param seRIHistCoverageList list of protobuf structures (one structure per SE) with a summary
     *        of RI coverage per SE.
     */
    public void updateHistEntityRICoverageMappings(@Nonnull final DSLContext context,
            @Nonnull final List<EntityRICoverageUpload> seRIHistCoverageList) {
        final List<UpdatableRecord<?>> records = new ArrayList<>();

        final LocalDateTime currentTime = LocalDateTime.now(ZoneOffset.UTC);
        for (EntityRICoverageUpload seRIUpload : seRIHistCoverageList) {
            final Long entityOid = seRIUpload.getEntityId();
            final List<EntityRICoverageUpload.Coverage> riCoverageList = seRIUpload.getCoverageList();

            // Table of covered coupons per entity/RI pair _combined_ by coverage source (i.e supplemental + billing)
            final Table<Long, Long, Double> riCombinedCoupons = HashBasedTable.create();
            // Start and end usage tables (per entity/RI pair)
            final Table<Long, Long, Long> riStartTime = HashBasedTable.create();
            final Table<Long, Long, Long> riEndTime = HashBasedTable.create();
            for (EntityRICoverageUpload.Coverage riCov : riCoverageList) {
                Long riId = riCov.getReservedInstanceId();
                Double coveredCoupons = riCombinedCoupons.get(entityOid, riId);
                if (coveredCoupons != null) {
                    coveredCoupons += riCov.getCoveredCoupons();
                    riCombinedCoupons.put(entityOid, riId, coveredCoupons);
                } else {
                    riCombinedCoupons.put(entityOid, riId, riCov.getCoveredCoupons());
                }
                riStartTime.put(entityOid, riId, riCov.getUsageStartTimestamp());
                riEndTime.put(entityOid, riId, riCov.getUsageEndTimestamp());
            }

            for (Table.Cell<Long, Long, Double> cell : riCombinedCoupons.cellSet()) {
                final Long reservedInstanceId = cell.getColumnKey();
                long usageStartTime = riStartTime.get(entityOid, reservedInstanceId);
                final LocalDateTime rawUsageStart = LocalDateTime.ofInstant(
                        // Add full minute to avoid zero timestamp (i.e '1970-01-01 00:00:00') which is out of range
                        // for MySQL JDBC driver.
                        Instant.ofEpochMilli(usageStartTime + 1000 * 60),
                        ZoneId.from(ZoneOffset.UTC));

                // RI coverage Usage Start time is a PK column and we do not want timestamp with precision of seconds
                // and nanoseconds to be part of PK value (to avoid inserting potential duplicate rows if API returns
                // slightly different timestamp next time around).
                // Seconds and nanoseconds play negligible role in Buy RI analysis and related recommendations.
                final LocalDateTime usageStart = LocalDateTime.of(rawUsageStart.getYear(),
                        rawUsageStart.getMonth(), rawUsageStart.getDayOfMonth(),
                        rawUsageStart.getHour(), rawUsageStart.getMinute());

                // Usage end.
                long usageEndTime = riEndTime.get(entityOid, reservedInstanceId);
                final LocalDateTime usageEnd = LocalDateTime.ofInstant(
                        // Again, add a minute to the usage end time.
                        Instant.ofEpochMilli(usageEndTime + 1000 * 60),
                        ZoneId.from(ZoneOffset.UTC));
                logger.info("Usage start/end for Entity/RI pair '{}:{}' = [{} .. {}], covered coupons = {}",
                        entityOid, reservedInstanceId, usageStart.toString(), usageEnd.toString(), cell.getValue());

                records.add(context.newRecord(HIST_ENTITY_RESERVED_INSTANCE_MAPPING,
                        new HistEntityReservedInstanceMappingRecord(
                                currentTime,
                                entityOid,
                                reservedInstanceId,
                                usageStart,
                                usageEnd)));
            }
        }

        // Note that we delete only specific historical coverage records.
        final int[] countDel = context.batchDelete(records).execute();
        Lists.partition(records, chunkSize).forEach(
                entitiesChunk -> context.batchInsert(entitiesChunk).execute());
        logger.info("Hist-SE-RI-Mapping: Count of deleted records: {}, updated: {}",
                countUpdatedRecords(countDel), records.size());
    }

    private int countUpdatedRecords(final int[] rcUpdated) {
        int recCount = 0;
        for (int i = 0; i < rcUpdated.length; i++) {
            recCount += rcUpdated[i];
        }
        return recCount;
    }

    /**
     * Delete historical Entity RI coverage records for a list of Entities.
     *
     * @param entityIds list of Entities whose RI coverage should be deleted.
     *
     * @return count of deleted rows.
     */
    public int deleteHistEntityRICoverageMappings(List<Long> entityIds) {
        logger.info("Deleting data from HistEntityRICoverage table for entities count: " + entityIds.size());
        final int rowsDeleted = dsl.deleteFrom(HIST_ENTITY_RESERVED_INSTANCE_MAPPING)
                    .where(HIST_ENTITY_RESERVED_INSTANCE_MAPPING.ENTITY_ID
                                .in(entityIds)).execute();
        return rowsDeleted;
    }

    /**
     * Get the sum count of used coupons for each reserved instance.
     *
     * @param context {@link DSLContext} transactional context.
     * @return a map which key is reserved instance id, value is the sum of used coupons.
     */
    public Map<Long, Double> getReservedInstanceUsedCouponsMap(@Nonnull final DSLContext context) {
        final Map<Long, Double> retMap = new HashMap<>();
        context.select(ENTITY_TO_RESERVED_INSTANCE_MAPPING.RESERVED_INSTANCE_ID,
                (sum(ENTITY_TO_RESERVED_INSTANCE_MAPPING.USED_COUPONS)).as(RI_SUM_COUPONS))
            .from(ENTITY_TO_RESERVED_INSTANCE_MAPPING)
            .groupBy(ENTITY_TO_RESERVED_INSTANCE_MAPPING.RESERVED_INSTANCE_ID)
            .fetch()
            .forEach(record -> retMap.put(record.value1(), record.value2().doubleValue()));
        return retMap;
    }

    /**
     * Get the sum count of used coupons for each reserved instance in the filter.
     *
     * @param context {@link DSLContext} transactional context.
     * @param filter {@link EntityReservedInstanceMappingFilter} filter for scoping to required Resrved Instance oids.
     * @return a map which key is reserved instance id, value is the sum of used coupons.
     */
    public Map<Long, Double> getReservedInstanceUsedCouponsMapWithFilter(@Nonnull final DSLContext context, EntityReservedInstanceMappingFilter filter) {
        final Map<Long, Double> retMap = new HashMap<>();
        context.select(ENTITY_TO_RESERVED_INSTANCE_MAPPING.RESERVED_INSTANCE_ID,
            (sum(ENTITY_TO_RESERVED_INSTANCE_MAPPING.USED_COUPONS)))
            .from(ENTITY_TO_RESERVED_INSTANCE_MAPPING)
            .where(filter.getConditions())
            .groupBy(ENTITY_TO_RESERVED_INSTANCE_MAPPING.RESERVED_INSTANCE_ID)
            .fetch()
            .forEach(record -> retMap.put(record.value1(), record.value2().doubleValue()));
        return retMap;
    }

    /**
     * Get the sum count of used coupons for each reserved instance in the filter.
     *
     * @param filter EntityReservedInstanceMappingFilter which contains scoped Reserved Instance oids.
     * @return a map which key is reserved instance id, value is the sum of used coupons.
     */
    public Map<Long, Double> getReservedInstanceUsedCouponsMapByFilter(
                    @Nonnull final EntityReservedInstanceMappingFilter filter) {
        return getReservedInstanceUsedCouponsMapWithFilter(dsl, filter);
    }

    /**
     * Get the RI coverage of all entities.
     *
     * @return A map from entity ID to {@link EntityReservedInstanceCoverage} for that entity.
     */
    public Map<Long, EntityReservedInstanceCoverage> getEntityRiCoverage() {
        final Map<Long, EntityReservedInstanceCoverage> retMap = new HashMap<>();

        final List<EntityToReservedInstanceMapping> riCoverageRows = getEntityRICoverageFromDB();

        EntityReservedInstanceCoverage.Builder curEntityCoverageBldr = null;
        for (final EntityToReservedInstanceMapping reCoverageRow : riCoverageRows) {
            logger.debug("EntityToReservedInstanceMapping retrieved from Database: {}", reCoverageRow::toString);
            final Long entityId = reCoverageRow.getEntityId();
            if (curEntityCoverageBldr == null) {
                // This should only be true for the first entry.
                curEntityCoverageBldr = EntityReservedInstanceCoverage.newBuilder()
                        .setEntityId(entityId);
            } else if (curEntityCoverageBldr.getEntityId() != entityId) {
                // Because of the group-by, this means there are no longer any records for the
                // entity whose coverage we've been building so far. We can "finalize" it by
                // and put it in the return map.
                retMap.put(curEntityCoverageBldr.getEntityId(), curEntityCoverageBldr.build());
                curEntityCoverageBldr = EntityReservedInstanceCoverage.newBuilder()
                        .setEntityId(entityId);
            }
            // When we have a case where an entity can be covered by multiple RIs, either through the bill or RI Allocator,
            // we need to aggregate them when updating the Coupons Covered by RI.
            if (reCoverageRow.getUsedCoupons() == null) {
                logger.error("Unable to get Used coupons for Entity {} with Reservation {}. Please check values in entity_to_reserved_instance_mapping table of cost db.",
                        reCoverageRow.getEntityId(), reCoverageRow.getReservedInstanceId());
                continue;
            }
            final double usedCoupons = reCoverageRow.getUsedCoupons();
            final double couponsCoveredByRiOrDefault = curEntityCoverageBldr.getCouponsCoveredByRiOrDefault(reCoverageRow.getReservedInstanceId(), 0.0);
            curEntityCoverageBldr.putCouponsCoveredByRi(reCoverageRow.getReservedInstanceId(), usedCoupons + couponsCoveredByRiOrDefault);
            logger.debug("Current Entity Coverage Builder: {}", curEntityCoverageBldr.getCouponsCoveredByRiMap()::toString);
        }
        // End of results = end of records for the last entity being built.
        if (curEntityCoverageBldr != null) {
            retMap.put(curEntityCoverageBldr.getEntityId(), curEntityCoverageBldr.build());
        }
        return retMap;
    }

    /**
     * Gets the RI coverage by entity ID.
     *
     * @return A {@link Map} of Entity OID to a {@link Set} of {@link Coverage} entries
     */
    public Map<Long, Set<Coverage>> getRICoverageByEntity() {
        return getRICoverageByEntity(entityReservedInstanceMappingFilter);
    }

    /**
     * Gets the RI coverage by entity ID.
     *
     * @param filter - entity reserved instance mapping filter.
     * @return A {@link Map} of Entity OID to a {@link Set} of {@link Coverage} entries
     */
    public Map<Long, Set<Coverage>> getRICoverageByEntity(EntityReservedInstanceMappingFilter filter) {
        final Map<Long, Set<Coverage>> riCoverageByEntity = new HashMap<>();

        getEntityRICoverageFromDB(filter).forEach(entityRIMapping -> {
            riCoverageByEntity.computeIfAbsent(
                    entityRIMapping.getEntityId(),
                    entityId -> new HashSet<Coverage>())
            .add(Coverage.newBuilder()
                    .setReservedInstanceId(entityRIMapping.getReservedInstanceId())
                    .setCoveredCoupons(entityRIMapping.getUsedCoupons())
                    .setRiCoverageSource(RICoverageSource.valueOf(
                            entityRIMapping.getRiSourceCoverage().toString()))
                    .build());
        });

        return riCoverageByEntity;
    }

    /**
     * Create a list of {@link EntityToReservedInstanceMappingRecord}.
     *
     * @param context {@link DSLContext} transactional context.
     * @param currentTime the current time.
     * @param entityId the entity id.
     * @param riCoverageList a list of {@link EntityRICoverageUpload.Coverage}.
     * @return a list of {@link EntityToReservedInstanceMappingRecord}.
     */
    private List<EntityToReservedInstanceMappingRecord> createEntityToRIMappingRecords(
            @Nonnull final DSLContext context,
            @Nonnull final LocalDateTime currentTime,
            final long entityId,
            @Nonnull final List<EntityRICoverageUpload.Coverage> riCoverageList) {

        // If the provider has multiple entries for the (RI OID, Coverage Source) tuple, we reduce
        // them to a single entry and sum the covered coupons to conform to the table key constraints
        final Table<Long, RICoverageSource, Double> riCoverageBySource = riCoverageList.stream()
                .collect(ImmutableTable.toImmutableTable(
                        Coverage::getReservedInstanceId,
                        Coverage::getRiCoverageSource,
                        Coverage::getCoveredCoupons,
                        Double::sum
                ));

        return riCoverageBySource.cellSet().stream()
                .map(riCoverage ->
                        context.newRecord(ENTITY_TO_RESERVED_INSTANCE_MAPPING,
                                new EntityToReservedInstanceMappingRecord(
                                        currentTime,
                                        entityId,
                                        // reserved_instance_id bigint
                                        // This ID will already be the Cost component assigned OID.
                                        // For billing coverage, this is updated in RIAndExpenseUploadRpcService.
                                        // For supplemental coverage, it is created with the correct OID.
                                        riCoverage.getRowKey(),
                                        // used_coupons float
                                        riCoverage.getValue(),
                                        // ri_source_coverage enum('BILLING','SUPPLEMENTAL_COVERAGE_ALLOCATION')
                                        EntityToReservedInstanceMappingRiSourceCoverage.valueOf(
                                                riCoverage.getColumnKey().toString()))))
                .collect(Collectors.toList());
    }

    private List<EntityToReservedInstanceMapping> getEntityRICoverageFromDB() {
        final List<EntityToReservedInstanceMapping> riCoverageRows =
                // There should only be one set of RI coverage in the table at a time, so
                // we can just get everything from the table.
                dsl.selectFrom(ENTITY_TO_RESERVED_INSTANCE_MAPPING)
                        // This is important - lets us process one entity completely before
                        // moving on to the next one.
                        .orderBy(ENTITY_TO_RESERVED_INSTANCE_MAPPING.ENTITY_ID)
                        .fetch()
                        .into(EntityToReservedInstanceMapping.class);
        return riCoverageRows;
    }

    private List<EntityToReservedInstanceMapping> getEntityRICoverageFromDB(EntityReservedInstanceMappingFilter filter) {
        final List<EntityToReservedInstanceMapping> riCoverageRows =
                // There should only be one set of RI coverage in the table at a time, so
                // we can just get everything from the table.
                dsl.selectFrom(ENTITY_TO_RESERVED_INSTANCE_MAPPING)
                        // This is important - lets us process one entity completely before
                        // moving on to the next one.
                        .where(filter.getConditions())
                        .orderBy(ENTITY_TO_RESERVED_INSTANCE_MAPPING.ENTITY_ID)
                        .fetch()
                        .into(EntityToReservedInstanceMapping.class);
        return riCoverageRows;
    }

    /**
     * Logs the entity-RI coverage information for the list of entities passed from swagger API. If list
     * is empty, all entity-ri coverage information found in the db table entity_to_reserved_instance_mapping
     * are logged.
     *
     * @param entityIds List of entityID obtained from the api request.
     */
    public void logEntityCoverage(@Nonnull List<Long> entityIds) {
        // Get the values from DB
        final Collection<EntityToReservedInstanceMapping> entityRICoverageFromDB = getEntityRICoverageFromDB();
        // Organize data in a multiValuedMap
        final MultiValuedMap<Long, EntityToReservedInstanceMapping> entityRIMultiMap = new HashSetValuedHashMap<>();
        for (EntityToReservedInstanceMapping entityToReservedInstanceMapping : entityRICoverageFromDB) {
            entityRIMultiMap.put(entityToReservedInstanceMapping.getEntityId(), entityToReservedInstanceMapping);
        }

        // if entityIds is empty, log all entity-RI coverage info else only log for requested set of entities.
        if (entityIds.isEmpty()) {
            logAllEntities(entityRIMultiMap);
        } else {
            logFilteredEntities(entityIds, entityRIMultiMap);
        }
    }

    /**
     * Logs the RI-entity coverage information for the list of RIs passed from swagger API. If list
     * is empty, all RI-entity coverage information found in the db table entity_to_reserved_instance_mapping
     * are logged.
     *
     * @param riIds List of riID obtained from the api request.
     */
    public void logRICoverage(@Nonnull List<Long> riIds) {
        //Get the values from DB
        final Collection<EntityToReservedInstanceMapping> entityRICoverageFromDB = getEntityRICoverageFromDB();
        // Organize data in a multiValuedMap
        final MultiValuedMap<Long, EntityToReservedInstanceMapping> riEntityMultiMap = new HashSetValuedHashMap<>();
        for (EntityToReservedInstanceMapping entityToReservedInstanceMapping : entityRICoverageFromDB) {
            riEntityMultiMap.put(entityToReservedInstanceMapping.getReservedInstanceId(), entityToReservedInstanceMapping);
        }

        // if riIds is empty, log all RI-entity coverage info else only log for requested set of RIs.
        if (riIds.isEmpty()) {
            logAllReservedInstances(riEntityMultiMap);
        } else {
            logFilteredReservedInstances(riIds, riEntityMultiMap);
        }
    }

    /**
     * Logs all the Entity - RI Coverage information when the list of entity ids obtained from the api request is empty.
     *
     * @param entitiesRIMultiMap MultiValuedMap where key is of type Long and value is a collection of EntityToReservedInstanceMapping.
     */
    private void logAllEntities(@Nonnull MultiValuedMap<Long, EntityToReservedInstanceMapping> entitiesRIMultiMap) {
        // if entityIdList is empty, log info for all entities
        final Set<Long> entityKeySet = entitiesRIMultiMap.keySet();
        for (Long entityId : entityKeySet) {
            final Collection<EntityToReservedInstanceMapping> entityToRIMappings = entitiesRIMultiMap.get(entityId);
            logEntityRIMapping(entityToRIMappings, entityId);
        }
    }

    /**
     * Logs Entity - RI Coverage information for entity IDs listed in the api request.
     *
     * @param entityIdList List of entityID obtained from the api request.
     * @param entitiesRIMultiMap MultiValuedMap containing the Entity - RI Coverage mapping.
     */
    private void logFilteredEntities(@Nonnull List<Long> entityIdList,
                                    @Nonnull MultiValuedMap<Long, EntityToReservedInstanceMapping> entitiesRIMultiMap) {
        // Log data as part of the incoming list
        for (Long entityId : entityIdList) {
            final Collection<EntityToReservedInstanceMapping> entityToReservedInstanceMappings = entitiesRIMultiMap.get(entityId);
            if (!entityToReservedInstanceMappings.isEmpty()) {
                logEntityRIMapping(entityToReservedInstanceMappings, entityId);
            } else {
                StringBuilder strBuilder = new StringBuilder("\n");
                strBuilder.append(new ST(ENTITY_RI_COVERAGE_LOGGING_TEMPLATE_ENTITY_INFO)
                                .add("entityId", entityId).render());
                strBuilder.append(new ST(ENTITY_RI_COVERAGE_LOGGING_TEMPLATE_COVERAGE_INFO)
                                .add("riId", "-")
                                .add("usedCoupons", 0)
                                .add("coverageSource", "-")
                                .render());
                strBuilder.append(new ST(LOGGING_TEMPLATE_TERMINATE).render());
                logger.info(strBuilder.toString());
            }
        }
    }

    /**
     * Logs information about an Entity and all the RIs used to cover it including the Coverage Source and Used Coupons.
     * | ================== NEW MAPPING =====================
     * | | Entity ID: 73122743345226
     * | ============== COVERAGE INFORMATION ================
     * | | Reserved Instance ID: 706441439968464
     * | | Used Coupons : 14.0
     * | | Coverage Source : BILLING
     * | ============== COVERAGE INFORMATION ================
     * | | Reserved Instance ID: 706441439968160
     * | | Used Coupons : 1.33333
     * | | Coverage Source : BILLING
     * | ====================================================
     *
     * @param entityToReservedInstanceMappings Collection of type EntityToReservedInstanceMapping containing the EntityRICoverage information.
     * @param entityId Entity ID whose information needs to be logged.
     */
    private void logEntityRIMapping(@Nonnull Collection<EntityToReservedInstanceMapping> entityToReservedInstanceMappings,
                    @Nonnull Long entityId) {
        if (!entityToReservedInstanceMappings.isEmpty()) {
            StringBuilder strBuilder = new StringBuilder("\n");
            strBuilder.append(new ST(ENTITY_RI_COVERAGE_LOGGING_TEMPLATE_ENTITY_INFO)
                    .add("entityId", entityId).render());
            for (EntityToReservedInstanceMapping entityToReservedInstanceMapping : entityToReservedInstanceMappings) {
                strBuilder.append(new ST(ENTITY_RI_COVERAGE_LOGGING_TEMPLATE_COVERAGE_INFO)
                        .add("riId", entityToReservedInstanceMapping.getReservedInstanceId())
                        .add("usedCoupons", entityToReservedInstanceMapping.getUsedCoupons())
                        .add("coverageSource", entityToReservedInstanceMapping.getRiSourceCoverage())
                        .render());
            }
            strBuilder.append(new ST(LOGGING_TEMPLATE_TERMINATE).render());
            logger.info(strBuilder.toString());
        }
    }

    /**
     * Logs all the RI - Entity Coverage information when the list of RI ids obtained from the api request is empty.
     *
     * @param riEntitiesMultiMap MultiValuedMap where key is of type Long and value is a collection of EntityToReservedInstanceMapping.
     */
    private void logAllReservedInstances(@Nonnull MultiValuedMap<Long, EntityToReservedInstanceMapping> riEntitiesMultiMap) {
        // If no RI List was sent from swagger, log info of all RIs.
        final Set<Long> riKeySet = riEntitiesMultiMap.keySet();
        for (Long riId : riKeySet) {
            final Collection<EntityToReservedInstanceMapping> entityToRIMappings = riEntitiesMultiMap.get(riId);
            logRIEntityMapping(entityToRIMappings, riId);
        }
    }

    /**
     * Logs RI - Entity Coverage information for entity IDs listed in the api request.
     *
     * @param riIdList List of riID obtained from the api request.
     * @param riEntitiesMultiMap MultiValuedMap where key is of type Long and value is a collection of EntityToReservedInstanceMapping.
     */
    private void logFilteredReservedInstances(@Nonnull List<Long> riIdList,
                    @Nonnull MultiValuedMap<Long, EntityToReservedInstanceMapping> riEntitiesMultiMap) {
        for (Long riId : riIdList) {
            final Collection<EntityToReservedInstanceMapping> entityToReservedInstanceMappings = riEntitiesMultiMap.get(riId);
            if (!entityToReservedInstanceMappings.isEmpty()) {
                logRIEntityMapping(entityToReservedInstanceMappings, riId);
            } else {
                StringBuilder stringBuilder = new StringBuilder("\n");
                stringBuilder.append(new ST(RI_ENTITY_LOGGING_TEMPLATE_ENTITY_INFO)
                                .add("riId", riId).render());
                stringBuilder.append(new ST(RI_ENTITY_COVERAGE_LOGGING_TEMPLATE_COVERAGE_INFO)
                                .add("entityId", "-")
                                .add("usedCoupons", 0)
                                .add("coverageSource", "-")
                                .render());
                stringBuilder.append(new ST(LOGGING_TEMPLATE_TERMINATE).render());
                logger.info(stringBuilder.toString());
            }
        }
    }

    /**
     * Logs information about an RI and all the entities it covers including the Coverage Source and Used Coupons.
     * | ================== NEW MAPPING =====================
     * | | Reserved Instance ID: 706441439968160
     * | ============== COVERAGE INFORMATION ================
     * | | Entity ID: 73122741996605
     * | | Used Coupons : 14.0
     * | | Coverage Source : BILLING
     * | ============== COVERAGE INFORMATION ================
     * | | Entity ID: 73122743345226
     * | | Used Coupons : 1.33333
     * | | Coverage Source : BILLING
     * | ====================================================
     *
     * @param riToEntityMappings Collection of type EntityToReservedInstanceMapping containing the EntityRICoverage information.
     * @param riId Reserved Instance ID whose information needs to be logged.
     */
    private void logRIEntityMapping(@Nonnull Collection<EntityToReservedInstanceMapping> riToEntityMappings,
                    @Nonnull Long riId) {
        if (!riToEntityMappings.isEmpty()) {
            StringBuilder strBuilder = new StringBuilder("\n");
            strBuilder.append(new ST(RI_ENTITY_LOGGING_TEMPLATE_ENTITY_INFO)
                    .add("riId", riId).render());
            for (EntityToReservedInstanceMapping entityToReservedInstanceMapping : riToEntityMappings) {
                strBuilder.append(new ST(RI_ENTITY_COVERAGE_LOGGING_TEMPLATE_COVERAGE_INFO)
                        .add("entityId", entityToReservedInstanceMapping.getEntityId())
                        .add("usedCoupons", entityToReservedInstanceMapping.getUsedCoupons())
                        .add("coverageSource", entityToReservedInstanceMapping.getRiSourceCoverage())
                        .render());
            }
            strBuilder.append(new ST(LOGGING_TEMPLATE_TERMINATE).render());
            logger.info(strBuilder.toString());
        }
    }

    @Override
    public void restoreDiags(@Nonnull final List<String> collectedDiags) throws DiagnosticsException {
        // TODO to be implemented as part of OM-58627
    }

    @Override
    public void collectDiags(@Nonnull final DiagnosticsAppender appender) throws DiagnosticsException {
        dsl.transaction(transactionContext -> {
            final DSLContext transaction = DSL.using(transactionContext);
            Stream<EntityToReservedInstanceMappingRecord> latestRecords = transaction.selectFrom(Tables.ENTITY_TO_RESERVED_INSTANCE_MAPPING).stream();
            latestRecords.forEach(s -> {
                try {
                    appender.appendString(s.formatJSON());
                } catch (DiagnosticsException e) {
                    logger.error("Exception encountered while appending entity to RI mapping records" +
                            " to the diags dump", e);
                }
            });
        });
    }

    @Nonnull
    @Override
    public String getFileName() {
        return entityReservedInstanceMappingFile;
    }

    /**
     * Retrieves RI Coverage records per each SE given in the list argument.
     * Empty argument list will produce RI Coverage records for all SEs in DB.
     *
     * @param entityIDs list of entity IDs for which we are interested to get historical RI coverage data.
     *
     * @return Map of SEs to list of RI Coverage data records.
     */
    public Map<Long, List<EntityHistRIMappingItem>> getHistEntityRICoverageMappings(final Collection<Long> entityIDs) {
        Map<Long, List<EntityHistRIMappingItem>> riCoverageMap = new HashMap<>();

        List<HistEntityReservedInstanceMappingRecord> records = dsl.selectFrom(HIST_ENTITY_RESERVED_INSTANCE_MAPPING)
                .where(filterByOidsCondition(entityIDs)).fetch();
        for (HistEntityReservedInstanceMappingRecord rec : records) {
            final Long entityId = rec.getEntityId();
            final Long riId = rec.getReservedInstanceId();
            final LocalDateTime usageStart = rec.getStartUsageTime();
            final LocalDateTime usageEnd = rec.getEndUsageTime();

            List<EntityHistRIMappingItem> riCoverageItems = riCoverageMap.get(entityId);
            if (riCoverageItems == null) {
                riCoverageItems = new ArrayList<>();
                riCoverageMap.put(entityId, riCoverageItems);
            }
            riCoverageItems.add(new EntityHistRIMappingItem(entityId, riId, usageStart, usageEnd));
        }

        return riCoverageMap;
    }

    /**
     * Condition to filter the Entity Historical RI Coverage table by the list of entity IDs.
     *
     * @param oids The entity IDs.
     * @return The condition.
     */
    private Condition filterByOidsCondition(final Collection<Long> oids) {
        return oids.isEmpty() ? DSL.trueCondition() : HIST_ENTITY_RESERVED_INSTANCE_MAPPING.ENTITY_ID.in(oids);
    }

    /**
     * Class representing item of account RI coverage.
     */
    protected class EntityHistRIMappingItem {
        private final Long entityId;
        private final Long reservedInstanceId;
        private final LocalDateTime usageStart;
        private final LocalDateTime usageEnd;

        public EntityHistRIMappingItem(Long entityId, Long reservedInstanceId, LocalDateTime usageStart,
                LocalDateTime usageEnd) {
            super();
            this.entityId = entityId;
            this.reservedInstanceId = reservedInstanceId;
            this.usageStart = usageStart;
            this.usageEnd = usageEnd;
        }

        public Long getEntityId() {
            return entityId;
        }

        public Long getReservedInstanceId() {
            return reservedInstanceId;
        }

        public LocalDateTime getUsageStart() {
            return usageStart;
        }

        public LocalDateTime getUsageEnd() {
            return usageEnd;
        }

        @Override
        public String toString() {
            return "EntityHistRICoverageItem [entityId=" + entityId + ", reservedInstanceId=" + reservedInstanceId
                    + ", usageStart=" + usageStart + ", usageEnd=" + usageEnd + "]";
        }
    }
}
