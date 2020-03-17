package com.vmturbo.history.db.queries;

import static com.vmturbo.components.common.utils.StringConstants.COMMODITY_KEY;
import static com.vmturbo.components.common.utils.StringConstants.MAX_VALUE;
import static com.vmturbo.components.common.utils.StringConstants.PROPERTY_SUBTYPE;
import static com.vmturbo.components.common.utils.StringConstants.PROPERTY_TYPE;
import static com.vmturbo.components.common.utils.StringConstants.RELATION;
import static com.vmturbo.components.common.utils.StringConstants.UUID;
import static com.vmturbo.history.db.jooq.JooqUtils.getDoubleField;
import static com.vmturbo.history.db.jooq.JooqUtils.getRelationTypeField;
import static com.vmturbo.history.db.jooq.JooqUtils.getStringField;

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

    /**
     * Create a new query.
     *
     * @param table Entity
     */
    public EntityCommoditiesMaxValuesQuery(@Nonnull Table<?> table) {
        final Field<String> propertyTypeField = getStringField(table, PROPERTY_TYPE);
        final Field<String> propertySubtypeField = getStringField(table, PROPERTY_SUBTYPE);
        final Field<RelationType> relationField = getRelationTypeField(table, RELATION);
        final Field<String> uuidField = getStringField(table, UUID);
        final Field<String> commodityKeyField = getStringField(table, COMMODITY_KEY);
        final Field<Double> maxValueField = getDoubleField(table, MAX_VALUE);

        addSelectFields(uuidField, propertyTypeField, commodityKeyField, DSL.max(maxValueField));
        addTable(table);
        addConditions(
                propertySubtypeField.eq(PropertySubType.Used.getApiParameterName()),
                relationField.eq(RelationType.COMMODITIES));
        groupBy(uuidField, propertyTypeField, commodityKeyField);
    }
}
