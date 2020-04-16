package com.vmturbo.history.db.queries;

import static com.vmturbo.common.protobuf.utils.StringConstants.COMMODITY_KEY;
import static com.vmturbo.common.protobuf.utils.StringConstants.MAX_VALUE;
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

import javax.annotation.Nonnull;

import io.grpc.stub.StreamObserver;

import org.jooq.Field;
import org.jooq.Table;
import org.jooq.impl.DSL;

import com.vmturbo.common.protobuf.stats.Stats.GetEntityCommoditiesMaxValuesRequest;
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
     * Create a new query.
     *
     * @param table Entity
     * @param comms The list of commodities to get the max value for.
     * @param maxUsedLookbackDays Look back days for querying max used value.
     */
    public EntityCommoditiesMaxValuesQuery(@Nonnull Table<?> table, @Nonnull List<String> comms,
        @Nonnull int maxUsedLookbackDays) {
        final Field<String> propertyTypeField = getStringField(table, PROPERTY_TYPE);
        final Field<String> propertySubtypeField = getStringField(table, PROPERTY_SUBTYPE);
        final Field<RelationType> relationField = getRelationTypeField(table, RELATION);
        final Field<String> uuidField = getStringField(table, UUID);
        final Field<String> commodityKeyField = getStringField(table, COMMODITY_KEY);
        final Field<Double> maxValueField = getDoubleField(table, MAX_VALUE);
        final Field<Timestamp> snapshotTime = getTimestampField(table, SNAPSHOT_TIME);

        addSelectFields(uuidField, propertyTypeField, commodityKeyField, DSL.max(maxValueField));
        addTable(table);
        table.getIndexes().stream()
            .filter(idx -> idx.getName().toLowerCase().equals(FORCE_INDEX))
            .findFirst().ifPresent(idx -> this.forceIndex(table, FORCE_INDEX));
        addConditions(
            snapshotTime.greaterOrEqual(Timestamp.from(Instant.now().minus(maxUsedLookbackDays, ChronoUnit.DAYS))),
            propertyTypeField.in(comms),
            propertySubtypeField.eq(PropertySubType.Used.getApiParameterName()),
            relationField.eq(RelationType.COMMODITIES));
        groupBy(uuidField, propertyTypeField, commodityKeyField);
    }
}
