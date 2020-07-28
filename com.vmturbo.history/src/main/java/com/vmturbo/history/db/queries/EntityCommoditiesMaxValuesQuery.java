package com.vmturbo.history.db.queries;

import static com.vmturbo.common.protobuf.utils.StringConstants.COMMODITY_KEY;
import static com.vmturbo.common.protobuf.utils.StringConstants.MAX_VALUE;
import static com.vmturbo.common.protobuf.utils.StringConstants.PRODUCER_UUID;
import static com.vmturbo.common.protobuf.utils.StringConstants.PROPERTY_SUBTYPE;
import static com.vmturbo.common.protobuf.utils.StringConstants.PROPERTY_TYPE;
import static com.vmturbo.common.protobuf.utils.StringConstants.RELATION;
import static com.vmturbo.common.protobuf.utils.StringConstants.SNAPSHOT_TIME;
import static com.vmturbo.common.protobuf.utils.StringConstants.UUID;
import static com.vmturbo.history.db.jooq.JooqUtils.getDoubleField;
import static com.vmturbo.history.db.jooq.JooqUtils.getRelationTypeField;
import static com.vmturbo.history.db.jooq.JooqUtils.getTimestampField;
import static com.vmturbo.history.db.jooq.JooqUtils.getStringField;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Optional;

import javax.annotation.Nonnull;

import io.grpc.stub.StreamObserver;

import org.apache.commons.collections4.CollectionUtils;
import org.jooq.Field;
import org.jooq.JoinType;
import org.jooq.Table;
import org.jooq.impl.DSL;
import org.springframework.lang.Nullable;

import com.vmturbo.common.protobuf.stats.Stats.GetEntityCommoditiesMaxValuesRequest;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.history.db.QueryBase;
import com.vmturbo.history.schema.RelationType;
import com.vmturbo.history.stats.PropertySubType;
import com.vmturbo.history.stats.StatsHistoryRpcService;

/**
 * This class builds queries to determine max values for sold commodities for every combination
 * of entity id, commodity type, and commodity key appearing in the monthly stats table for a
 * given entity type.
 *
 * <p>This query is used in the implementation of the
 * {@link StatsHistoryRpcService#getEntityCommoditiesMaxValues(GetEntityCommoditiesMaxValuesRequest, StreamObserver)}
 * service method.
 * </p>
 */
public class EntityCommoditiesMaxValuesQuery extends QueryBase {

    private static final String FORCE_INDEX = "property_type";

    /**
     * Create a new default query.
     *
     * @param table Entity
     * @param comms The list of commodities to get the max value for.
     * @param lookbackDays Look back days for querying max used value.
     */
    public EntityCommoditiesMaxValuesQuery(@Nonnull Table<?> table, @Nonnull List<String> comms,
        @Nonnull int lookbackDays) {
        getEntityCommoditiesMaxValuesQuery(table, comms, lookbackDays, false, null, Optional.empty());
    }

    /**
     * Creates a new query considering whether a commodity is bought, and a specified set of uuids
     * in addition to the original arguments considered.
     *
     * @param table the stats table for which stats are being queried
     * @param comms the commodities for which to compute a historical max
     * @param lookbackDays the number of days to look back
     * @param commodityBought whether we're interested in bought commodites - if false, consider
     * sold commodities
     * @param uuids the entity uuids for which stats should be gathered - if null or empty, consider all
     * @param tempTableName the name of the optional temp table to use
     */
    public EntityCommoditiesMaxValuesQuery(@Nonnull Table<?> table, @Nonnull List<String> comms,
            @Nonnull int lookbackDays, boolean commodityBought, @Nonnull List<Long> uuids, Optional<String> tempTableName) {
        getEntityCommoditiesMaxValuesQuery(table, comms, lookbackDays, commodityBought, uuids, tempTableName);
    }

    /**
     * Builds the query to execute.
     *
     * @param table the stats table for which stats are being queried
     * @param comms the commodities for which to compute a historical max
     * @param lookbackDays the number of days to look back
     * @param commodityBought whether we're interested in bought commodites - if false, consider
     * sold commodities
     * @param uuids the entity uuids for which stats should be gathered - if null or empty, consider all
     * @param tempTableNameOptional the name of the optional temp table to use
     */
    public void getEntityCommoditiesMaxValuesQuery(
            @Nonnull final Table<?> table,
            @Nonnull final List<String> comms,
            @Nonnull int lookbackDays,
            final boolean commodityBought,
            @Nullable final List<Long> uuids,
            Optional<String> tempTableNameOptional) {
        final Field<String> propertyTypeField = getStringField(table, PROPERTY_TYPE);
        final Field<String> propertySubtypeField = getStringField(table, PROPERTY_SUBTYPE);
        final Field<RelationType> relationField = getRelationTypeField(table, RELATION);
        final Field<String> uuidField = getStringField(table, UUID);
        final Field<String> producerUuidField = getStringField(table, PRODUCER_UUID);
        final Field<String> commodityKeyField = getStringField(table, COMMODITY_KEY);
        final Field<Double> maxValueField = getDoubleField(table, MAX_VALUE);
        final Field<Timestamp> snapshotTime = getTimestampField(table, SNAPSHOT_TIME);


        if (commodityBought) {
            addSelectFields(uuidField, propertyTypeField, commodityKeyField, DSL.max(maxValueField), producerUuidField);
            groupBy(uuidField, propertyTypeField, commodityKeyField, producerUuidField);
        } else {
            addSelectFields(uuidField, propertyTypeField, commodityKeyField, DSL.max(maxValueField));
            groupBy(uuidField, propertyTypeField, commodityKeyField);
        }
        addTable(table);
        table.getIndexes().stream()
                .filter(idx -> idx.getName().toLowerCase().equals(FORCE_INDEX))
                .findFirst().ifPresent(idx -> this.forceIndex(table, FORCE_INDEX));

        addConditions(
                snapshotTime.greaterOrEqual(Timestamp.from(Instant.now().minus(lookbackDays, ChronoUnit.DAYS))),
                propertyTypeField.in(comms),
                propertySubtypeField.eq(PropertySubType.Used.getApiParameterName()),
                relationField.eq(commodityBought ? RelationType.COMMODITIESBOUGHT : RelationType.COMMODITIES));

        boolean useTempTable = tempTableNameOptional.isPresent();
        if (useTempTable) {
            final String tempTableName = tempTableNameOptional.get();
            final Table<?> tempTable = DSL.table(tempTableName);
            addTable(tempTable, null, JoinType.JOIN, getStringField(table, UUID).eq(
                    DSL.field(DSL.name(tempTableName, StringConstants.TARGET_OBJECT_UUID), String.class)
            ));

        } else if (CollectionUtils.isNotEmpty(uuids) && !useTempTable) {
            addConditions(uuidField.in(uuids));
        }
    }
}
