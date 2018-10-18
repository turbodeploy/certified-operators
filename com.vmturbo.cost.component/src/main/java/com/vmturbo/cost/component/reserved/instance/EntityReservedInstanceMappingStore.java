package com.vmturbo.cost.component.reserved.instance;

import static org.jooq.impl.DSL.sum;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.Record2;
import org.jooq.Result;

import com.google.common.collect.Lists;

import com.vmturbo.common.protobuf.cost.Cost.EntityReservedInstanceCoverage;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.UploadRIDataRequest.EntityRICoverageUpload;
import com.vmturbo.cost.component.db.Tables;
import com.vmturbo.cost.component.db.tables.pojos.EntityToReservedInstanceMapping;
import com.vmturbo.cost.component.db.tables.records.EntityToReservedInstanceMappingRecord;
import com.vmturbo.cost.component.reserved.instance.filter.ReservedInstanceBoughtFilter;

/**
 * This class responsible for storing the mapping relation between entity with reserved instance about
 * coupons coverage information. And the data is only comes from billing topology. For example:
 * VM1 use RI1 10 coupons, VM1 use RI2 20 coupons, VM2 use RI3 5 coupons.
 */
public class EntityReservedInstanceMappingStore {

    private final static Logger logger = LogManager.getLogger();

    private final static String RI_SUM_COUPONS = "RI_SUM_COUPONS";

    private final static String ENTITY_SUM_COUPONS = "ENTITY_SUM_COUPONS";

    //TODO: set this chunk config through consul.
    private final static int chunkSize = 1000;

    private final DSLContext dsl;

    private final ReservedInstanceBoughtStore reservedInstanceBoughtStore;

    public EntityReservedInstanceMappingStore(
            @Nonnull final DSLContext dsl,
            @Nonnull final ReservedInstanceBoughtStore reservedInstanceBoughtStore) {
        this.dsl = dsl;
        this.reservedInstanceBoughtStore = reservedInstanceBoughtStore;
    }

    /**
     * Input a list of {@link EntityRICoverageUpload}, store them into the database table.
     *
     * @param context {@link DSLContext} transactional context.
     * @param entityReservedInstanceCoverages a list of {@link EntityRICoverageUpload}.
     */
    public void updateEntityReservedInstanceMapping(
            @Nonnull final DSLContext context,
            @Nonnull final List<EntityRICoverageUpload> entityReservedInstanceCoverages) {
        final LocalDateTime currentTime = LocalDateTime.now(ZoneOffset.UTC);
        final ReservedInstanceBoughtFilter filter = ReservedInstanceBoughtFilter.newBuilder().build();
        final List<ReservedInstanceBought> reservedInstancesBought =
                reservedInstanceBoughtStore.getReservedInstanceBoughtByFilterWithContext(context, filter);
        // it will not have conflict, because the probe reserved instance id is also unique.
        final Map<String, Long> probeStrIdToIdMap = reservedInstancesBought.stream()
                .collect(Collectors.toMap(
                        ri -> ri.getReservedInstanceBoughtInfo().getProbeReservedInstanceId(),
                        ReservedInstanceBought::getId));
        final List<EntityToReservedInstanceMappingRecord> records =
                entityReservedInstanceCoverages.stream()
                        .filter(entityCoverage -> !entityCoverage.getCoverageList().isEmpty())
                        .map(entityCoverage -> createEntityToRIMappingRecords(
                                context, currentTime,
                                entityCoverage.getEntityId(),
                                entityCoverage.getCoverageList(), probeStrIdToIdMap))
                        .flatMap(List::stream)
                        .collect(Collectors.toList());

        // replace table with the latest mapping record.
        context.deleteFrom(Tables.ENTITY_TO_RESERVED_INSTANCE_MAPPING).execute();
        Lists.partition(records, chunkSize).forEach(entityChunk -> context.batchInsert(records).execute());
    }

    /**
     * Get the sum count of used coupons for each reserved instance.
     *
     * @param context {@link DSLContext} transactional context.
     * @return a map which key is reserved instance id, value is the sum of used coupons.
     */
    public Map<Long, Double> getReservedInstanceUsedCouponsMap(@Nonnull final DSLContext context) {
        final Result<Record2<Long, Double>> riUsedCouponsMap =
                context.select(Tables.ENTITY_TO_RESERVED_INSTANCE_MAPPING.RESERVED_INSTANCE_ID,
                        (sum(Tables.ENTITY_TO_RESERVED_INSTANCE_MAPPING.USED_COUPONS))
                                .cast(Double.class).as(RI_SUM_COUPONS))
                        .from(Tables.ENTITY_TO_RESERVED_INSTANCE_MAPPING)
                        .groupBy(Tables.ENTITY_TO_RESERVED_INSTANCE_MAPPING.RESERVED_INSTANCE_ID)
                        .fetch();
        return riUsedCouponsMap.intoMap(
                riUsedCouponsMap.field(Tables.ENTITY_TO_RESERVED_INSTANCE_MAPPING.RESERVED_INSTANCE_ID),
                riUsedCouponsMap.field(RI_SUM_COUPONS).cast(Double.class));
    }

    /**
     * Get the RI coverage of all entities.
     *
     * @return A map from entity ID to {@link EntityReservedInstanceCoverage} for that entity.
     */
    public Map<Long, EntityReservedInstanceCoverage> getEntityRiCoverage() {
        final Map<Long, EntityReservedInstanceCoverage> retMap = new HashMap<>();

        final List<EntityToReservedInstanceMapping> riCoverageRows =
            // There should only be one set of RI coverage in the table at a time, so
            // we can just get everything from the table.
            dsl.selectFrom(Tables.ENTITY_TO_RESERVED_INSTANCE_MAPPING)
                    // This is important - lets us process one entity completely before
                    // moving on to the next one.
                    .orderBy(Tables.ENTITY_TO_RESERVED_INSTANCE_MAPPING.ENTITY_ID)
                    .fetch()
                    .into(EntityToReservedInstanceMapping.class);

        EntityReservedInstanceCoverage.Builder curEntityCoverageBldr = null;
        for (final EntityToReservedInstanceMapping reCoverageRow : riCoverageRows) {
            if (curEntityCoverageBldr == null) {
                // This should only be true for the first entry.
                curEntityCoverageBldr = EntityReservedInstanceCoverage.newBuilder()
                        .setEntityId(reCoverageRow.getEntityId());
            } else if (curEntityCoverageBldr.getEntityId() != reCoverageRow.getEntityId()) {
                // Because of the group-by, this means there are no longer any records for the
                // entity whose coverage we've been building so far. We can "finalize" it by
                // and put it in the return map.
                retMap.put(curEntityCoverageBldr.getEntityId(), curEntityCoverageBldr.build());
                curEntityCoverageBldr = EntityReservedInstanceCoverage.newBuilder()
                        .setEntityId(reCoverageRow.getEntityId());
            }
            curEntityCoverageBldr.putCouponsCoveredByRi(reCoverageRow.getReservedInstanceId(),
                    reCoverageRow.getUsedCoupons());
        }
        // End of results = end of records for the last entity being built.
        if (curEntityCoverageBldr != null) {
            retMap.put(curEntityCoverageBldr.getEntityId(), curEntityCoverageBldr.build());
        }
        return retMap;
    }

    /**
     * Create a list of {@link EntityToReservedInstanceMappingRecord}.
     *
     * @param context {@link DSLContext} transactional context.
     * @param currentTime the current time.
     * @param entityId the entity id.
     * @param riCoverageList a list of {@link EntityRICoverageUpload.Coverage}.
     * @param probeStrIdToIdMap a map which key is probe reserved instance id, value is the real id
     *                          reserved instance.
     * @return a list of {@link EntityToReservedInstanceMappingRecord}.
     */
    private List<EntityToReservedInstanceMappingRecord> createEntityToRIMappingRecords(
            @Nonnull final DSLContext context,
            @Nonnull final LocalDateTime currentTime,
            final long entityId,
            @Nonnull final List<EntityRICoverageUpload.Coverage> riCoverageList,
            @Nonnull Map<String, Long> probeStrIdToIdMap) {
        return riCoverageList.stream()
                .filter(riCoverage -> probeStrIdToIdMap.containsKey(riCoverage.getProbeReservedInstanceId()))
                .map(riCoverage ->
                        context.newRecord(Tables.ENTITY_TO_RESERVED_INSTANCE_MAPPING,
                                new EntityToReservedInstanceMappingRecord(currentTime, entityId,
                                        probeStrIdToIdMap.get(riCoverage.getProbeReservedInstanceId()),
                                        riCoverage.getCoveredCoupons())))
                .collect(Collectors.toList());
    }
}
