/*
 * (C) Turbonomic 2020.
 */

package com.vmturbo.history.stats.snapshots;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableTable;
import com.google.common.collect.Table;

import org.jooq.Field;
import org.jooq.Record;

import com.vmturbo.common.protobuf.stats.Stats.StatsFilter.CommodityRequest;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.history.schema.abstraction.tables.HistUtilization;
import com.vmturbo.history.schema.abstraction.tables.VmStatsLatest;
import com.vmturbo.history.schema.abstraction.tables.records.HistUtilizationRecord;

/**
 * {@link AbstractRecordsAggregator} provides a common behavior for {@link RecordsAggregator}
 * implementations.
 *
 * @param <R> type of the record that should be aggregated.
 */
public abstract class AbstractRecordsAggregator<R extends Record> implements RecordsAggregator<R> {
    /**
     * Random stats table. All stats tables has the same structure
     */
    private static final VmStatsLatest STATS_TABLE = VmStatsLatest.VM_STATS_LATEST;
    private static final String RECORD_KEY_DELIMITER = "-";
    /*
     map from groupBy string provided by API to the corresponding table field name in DB
     virtualDisk id is saved as commodity key in db, thus mapped to commodity key column
    */
    private static final Map<String, Field<?>> API_GROUP_BY_TO_TABLE_FIELDS =
                                        createApiGroupByToFields();
    private static final Table<Class<? extends Record>, String, Field<?>>
                    API_GROUP_BY_TO_OVERRIDDEN_FIELDS =
                                        ImmutableTable.of(HistUtilizationRecord .class, StringConstants.RELATED_ENTITY,
                                                        HistUtilization.HIST_UTILIZATION.PRODUCER_OID);

    private final Map<String, Collection<String>> commodityNameToGroupByIds;

    /**
     * Creates {@link AbstractRecordsAggregator} instance.
     *
     * @param commodityRequests which contains information that manages aggregation process.
     */
    protected AbstractRecordsAggregator(@Nonnull Collection<CommodityRequest> commodityRequests) {
        // collect groupBy fields for different commodity type and convert to table field names
        this.commodityNameToGroupByIds =
                        commodityRequests.stream().filter(CommodityRequest::hasCommodityName)
                                        .collect(Collectors
                                                        .toMap(CommodityRequest::getCommodityName,
                                                                        r -> new HashSet<>(
                                                                                        r.getGroupByList())));
    }

    private static Map<String, Field<?>> createApiGroupByToFields() {
        final ImmutableMap.Builder<String, Field<?>> result = ImmutableMap.builder();
        result.put(StringConstants.RELATED_ENTITY, STATS_TABLE.PRODUCER_UUID);
        result.put(StringConstants.KEY, STATS_TABLE.COMMODITY_KEY);
        result.put(StringConstants.VIRTUAL_DISK, STATS_TABLE.COMMODITY_KEY);
        return result.build();
    }

    /**
     * Creates record key, based on the requests from API.
     *
     * @param record for which we need to create a key.
     * @return record key that will be used for records grouping process.
     */
    @Nonnull
    protected String createRecordKey(@Nonnull R record) {
        // Need to separate commodity bought and sold as some commodities are both bought
        // and sold in the same entity, e.g., StorageAccess in Storage entity.

        final String commodityName = getPropertyType(record);
        // See the enum RelationType in com.vmturbo.history.db.
        // Commodities, CommoditiesBought, and CommoditiesFromAttributes
        // (e.g., priceIndex, numVCPUs, etc.)
        final Collection<String> recordKeyItems = new LinkedList<>();
        recordKeyItems.add(commodityName);
        final String relation = getRelation(record);
        recordKeyItems.add(relation);
        // collect groupBy fields for different commodity type and convert to table field names
        final Collection<Field<?>> groupByFields = commodityNameToGroupByIds
                        .getOrDefault(commodityName, Collections.emptySet()).stream()
                        .map(groupById -> getGroupByField(record, groupById))
                        .collect(Collectors.toSet());
        recordKeyItems.add(createGroupByKey(record, groupByFields));
        // Need to separate commodities by entity_type if the field exists, as some
        // commodities (e.g. CPUAllocation) are sold by more than one entity type
        // (VirtualDataCenter and PhysicalMachine)
        if (record.field(StringConstants.ENTITY_TYPE) != null) {
            recordKeyItems.add(record.getValue(StringConstants.ENTITY_TYPE, String.class));
        }
        return String.join(RECORD_KEY_DELIMITER, recordKeyItems);
    }

    @Nonnull
    private String createGroupByKey(@Nonnull R record,
                    @Nonnull Collection<Field<?>> groupByFields) {
        return groupByFields.stream().filter(fieldName -> record.field(fieldName) != null)
                        .map(fieldName -> record.getValue(fieldName, String.class))
                        .collect(Collectors.joining(RECORD_KEY_DELIMITER));
    }

    @Nullable
    private Field<?> getGroupByField(@Nonnull R record, @Nonnull String groupById) {
        final Field<?> field = API_GROUP_BY_TO_OVERRIDDEN_FIELDS.get(record.getClass(), groupById);
        if (field == null) {
            return API_GROUP_BY_TO_TABLE_FIELDS.get(groupById);
        }
        return field;
    }

    /**
     * Returns relation type literal from DB record.
     *
     * @param record which relation type we want to know.
     * @return relation type literal of the record.
     */
    @Nonnull
    protected abstract String getRelation(@Nonnull R record);

    /**
     * Returns property type of the DB record.
     *
     * @param record which property type we want to know.
     * @return property type of the DB record.
     */
    @Nonnull
    protected abstract String getPropertyType(@Nonnull R record);
}
