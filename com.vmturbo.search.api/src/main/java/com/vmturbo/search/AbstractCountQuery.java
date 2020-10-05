package com.vmturbo.search;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.annotations.VisibleForTesting;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.RecordMapper;
import org.jooq.Result;
import org.jooq.Select;
import org.jooq.impl.DSL;

import com.vmturbo.api.dto.searchquery.FieldApiDTO;
import com.vmturbo.api.dto.searchquery.SearchCountRecordApiDTO;
import com.vmturbo.api.dto.searchquery.SearchQueryRecordApiDTO;

/**
 * A representation of a single API count query, mapped to a SQL query.
 */
public abstract class AbstractCountQuery extends AbstractQuery {

    private static final Logger logger = LogManager.getLogger();

    /**
     * The set of fields to select and group by, lazy-loaded.
     */
    private Set<Field> groupByFields;

    /**
     * Creates a query to retrieve entity or group counts from the search db.
     *
     * @param readOnlyDSLContext a context for making read-only database queries
     */
    protected AbstractCountQuery(@NonNull final DSLContext readOnlyDSLContext) {
        super(readOnlyDSLContext);
    }

    /**
     * Count the entities or groups in the dataset, grouped by the criteria defined in the request.
     *
     * @return a list of counts along with the fields they are grouped by
     */
    protected List<SearchCountRecordApiDTO> count() {
        // We select the same fields that we want to group by so that we can have meaningful results,
        // that is to say labels along with the counts.
        final Set<Field> groupByFields = buildSelectFields();
        Select<Record> query = getReadOnlyDSLContext()
            .select(groupByFields)
            .select(DSL.count())
            // no need to join search_entity_action table for now as no action data involved
            .from(SEARCH_ENTITY_TABLE)
            .where(buildWhereClause())
            .groupBy(groupByFields);

        //Decouple fetch from query chain for testing purposes
        Result<Record> records = getReadOnlyDSLContext().fetch(query);
        logger.debug("COUNT_RECORDS: \n" + records.formatCSV());
        List<SearchCountRecordApiDTO> results = records.map(countRecordMapper());
        return results;
    }

    @Override
    protected Set<Field> buildSelectFields() {
        if (groupByFields == null) {
            groupByFields = getGroupByFields()
                .map(this::buildAndTrackSelectFieldFromEntityType)
                .collect(Collectors.toSet());
        }
        return groupByFields;
    }

    /**
     * Maps fetched {@link Record}s into {@link SearchQueryRecordApiDTO}.
     *
     * @return mapper to process records from Search DB
     */
    @VisibleForTesting
    RecordMapper<Record, SearchCountRecordApiDTO> countRecordMapper() {
        return record -> {
            Integer count = record.get(DSL.count());
            return SearchCountRecordApiDTO.entityCount(mapValues(record), count);
        };
    }

    protected abstract Stream<FieldApiDTO> getGroupByFields();

    protected abstract Condition buildWhereClause();
}
