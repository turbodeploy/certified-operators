package com.vmturbo.history.db.queries;

import java.sql.Timestamp;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.jooq.Field;
import org.jooq.Table;
import org.jooq.impl.DSL;

import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.history.db.QueryBase;
import com.vmturbo.history.db.jooq.JooqUtils;

/**
 * Query that fetches stats records for clusters.
 */
public class ClusterStatsQuery extends QueryBase {
    /**
     * Create a cluster stats query.
     *
     * @param table      table to fetch data from
     * @param startDate  optional starting time
     * @param endDate    optional ending time
     * @param fields     property types of interest
     * @param clusterIds clusters of interest
     *                   if empty, retrieve stats for all clusters in the table
     */
    public ClusterStatsQuery(@Nonnull Table<?> table,
                             @Nonnull Optional<Timestamp> startDate,
                             @Nonnull Optional<Timestamp> endDate,
                             @Nonnull Set<String> fields,
                             @Nonnull Set<Long> clusterIds) {
        addTable(table);
        final Field<Timestamp> recordedOn = JooqUtils.getTimestampField(table, StringConstants.RECORDED_ON);
        final Field<String> propertyType = JooqUtils.getStringField(table, StringConstants.PROPERTY_TYPE);
        final Field<String> id = JooqUtils.getStringField(table, StringConstants.INTERNAL_NAME);

        startDate.ifPresent(t -> addConditions(recordedOn.ge(t)));
        endDate.ifPresent(t -> addConditions(recordedOn.le(t)));

        if (!fields.isEmpty()) {
            addConditions(DSL.or(fields.stream()
                                        .map(propertyType::equalIgnoreCase)
                                        .collect(Collectors.toList())));
        }

        if (!clusterIds.isEmpty()) {
            addConditions(id.in(clusterIds));
        }
    }
}
