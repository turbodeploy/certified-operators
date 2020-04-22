package com.vmturbo.history.db.queries;

import static com.vmturbo.common.protobuf.utils.StringConstants.CAPACITY;
import static com.vmturbo.common.protobuf.utils.StringConstants.COMMODITY_KEY;
import static com.vmturbo.common.protobuf.utils.StringConstants.PROPERTY_TYPE;
import static com.vmturbo.common.protobuf.utils.StringConstants.SNAPSHOT_TIME;
import static com.vmturbo.common.protobuf.utils.StringConstants.UUID;
import static com.vmturbo.history.db.jooq.JooqUtils.getDoubleField;
import static com.vmturbo.history.db.jooq.JooqUtils.getStringField;
import static com.vmturbo.history.db.jooq.JooqUtils.getTimestampField;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;

import javax.annotation.Nonnull;

import org.jooq.Field;
import org.jooq.Table;
import org.jooq.impl.DSL;

import com.vmturbo.history.db.QueryBase;

/**
 * This class builds queries to determine max capacity value in the last 7 days for sold commodities
 * for every combination of entity id, commodity type, and commodity key appearing in the daily stats
 * table for a given entity type.
 *
 */
public class EntityCommoditiesCapacityValuesQuery extends QueryBase {

    /**
     * Create a new query.
     *
     * @param table Entity
     */
    public EntityCommoditiesCapacityValuesQuery(@Nonnull Table<?> table, List<String> uuids,
                                                String commodityType) {
        final Field<String> uuidField = getStringField(table, UUID);
        final Field<String> propertyTypeField = getStringField(table, PROPERTY_TYPE);
        final Field<String> commodityKeyField = getStringField(table, COMMODITY_KEY);
        final Field<Double> capacity = getDoubleField(table, CAPACITY);
        final Field<Timestamp> snapshotTime = getTimestampField(table, SNAPSHOT_TIME);
        addSelectFields(uuidField, propertyTypeField, commodityKeyField, DSL.max(capacity));
        addTable(table);
        addConditions(
                uuidField.in(uuids),
                propertyTypeField.eq(commodityType),
                snapshotTime.greaterOrEqual(Timestamp.from(Instant.now().minus(7, ChronoUnit.DAYS))));
        groupBy(uuidField, propertyTypeField, commodityKeyField);
    }
}