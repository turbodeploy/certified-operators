package com.vmturbo.history.db.queries;

import static com.vmturbo.common.protobuf.utils.StringConstants.CAPACITY;
import static com.vmturbo.common.protobuf.utils.StringConstants.COMMODITY_KEY;
import static com.vmturbo.common.protobuf.utils.StringConstants.PROPERTY_TYPE;
import static com.vmturbo.common.protobuf.utils.StringConstants.RELATION;
import static com.vmturbo.common.protobuf.utils.StringConstants.SNAPSHOT_TIME;
import static com.vmturbo.common.protobuf.utils.StringConstants.UUID;
import static com.vmturbo.history.db.jooq.JooqUtils.getDoubleField;
import static com.vmturbo.history.db.jooq.JooqUtils.getRelationTypeField;
import static com.vmturbo.history.db.jooq.JooqUtils.getStringField;
import static com.vmturbo.history.db.jooq.JooqUtils.getTimestampField;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Set;

import javax.annotation.Nonnull;

import org.jooq.Field;
import org.jooq.Table;
import org.jooq.impl.DSL;

import com.vmturbo.history.db.QueryBase;
import com.vmturbo.history.schema.RelationType;

/**
 * This class builds queries to determine max capacity value in the last 7 days for sold commodities
 * for every combination of entity id, commodity type, and commodity key appearing in the daily stats
 * table for a given entity type.
 *
 */
public class EntityCommoditiesCapacityValuesQuery extends QueryBase {

    private static final String FORCE_INDEX = "property_type";

    /**
     * Creating a query to get the max capacity of all entities in this table over the last 7 days.
     *
     * @param table destination for the query
     * @param commodities The types of commodities we want.
     */
    public EntityCommoditiesCapacityValuesQuery(@Nonnull Table<?> table,
                                                Set<String> commodities) {
        final Field<String> uuidField = getStringField(table, UUID);
        final Field<String> propertyTypeField = getStringField(table, PROPERTY_TYPE);
        final Field<String> commodityKeyField = getStringField(table, COMMODITY_KEY);
        final Field<Double> capacityField = getDoubleField(table, CAPACITY);
        final Field<Timestamp> timeField = getTimestampField(table, SNAPSHOT_TIME);
        final Field<RelationType> relationField = getRelationTypeField(table, RELATION);

        table.getIndexes().stream()
            .filter(idx -> idx.getName().toLowerCase().equals(FORCE_INDEX))
            .findFirst().ifPresent(idx -> this.forceIndex(table, FORCE_INDEX));

        addSelectFields(uuidField, commodityKeyField, propertyTypeField, DSL.max(capacityField));
        addTable(table);
        addConditions(propertyTypeField.in(commodities),
                relationField.eq(RelationType.COMMODITIES),
                timeField.greaterOrEqual(Timestamp.from(Instant.now().minus(7, ChronoUnit.DAYS))));
        groupBy(uuidField, propertyTypeField, commodityKeyField);
    }
}