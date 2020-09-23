package com.vmturbo.search;

import static org.jooq.impl.DSL.coalesce;
import static org.jooq.impl.DSL.inline;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.jetbrains.annotations.NotNull;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.Select;
import org.jooq.SelectSeekStepN;
import org.jooq.SortField;
import org.jooq.SortOrder;
import org.jooq.impl.DSL;

import com.vmturbo.api.dto.searchquery.BooleanConditionApiDTO;
import com.vmturbo.api.dto.searchquery.ConditionApiDTO;
import com.vmturbo.api.dto.searchquery.ConditionApiDTO.Operator;
import com.vmturbo.api.dto.searchquery.FieldApiDTO;
import com.vmturbo.api.dto.searchquery.FieldValueApiDTO.Type;
import com.vmturbo.api.dto.searchquery.InclusionConditionApiDTO;
import com.vmturbo.api.dto.searchquery.IntegerConditionApiDTO;
import com.vmturbo.api.dto.searchquery.NumberConditionApiDTO;
import com.vmturbo.api.dto.searchquery.OrderByApiDTO;
import com.vmturbo.api.dto.searchquery.PaginationApiDTO;
import com.vmturbo.api.dto.searchquery.PrimitiveFieldApiDTO;
import com.vmturbo.api.dto.searchquery.SearchQueryRecordApiDTO;
import com.vmturbo.api.dto.searchquery.TextConditionApiDTO;
import com.vmturbo.api.dto.searchquery.WhereApiDTO;
import com.vmturbo.api.pagination.searchquery.SearchQueryPaginationResponse;
import com.vmturbo.common.api.mappers.EnumMapper;
import com.vmturbo.search.mappers.EntitySeverityMapper;
import com.vmturbo.search.mappers.EntityStateMapper;
import com.vmturbo.search.mappers.EntityTypeMapper;
import com.vmturbo.search.mappers.EnvironmentTypeMapper;
import com.vmturbo.search.mappers.GroupTypeMapper;
import com.vmturbo.search.metadata.SearchEntityMetadata;
import com.vmturbo.search.metadata.SearchGroupMetadata;
import com.vmturbo.search.metadata.SearchMetadataMapping;

/**
 * A representation of a single API search query, mapped to a SQL query.
 */
public abstract class AbstractSearchQuery extends AbstractQuery {

    /**
     * Provides a mapping from EntityType(string) -> SearchEntityMetadata.
     */
    public static final EnumMapper<SearchEntityMetadata> SEARCH_ENTITY_METADATA_ENUM_MAPPER =
        new EnumMapper<>(SearchEntityMetadata.class);
    /**
     * Provides a mapping from GroupType(string) -> SearchEntityMetadata.
     */
    public static final EnumMapper<SearchGroupMetadata> SEARCH_GROUP_METADATA_ENUM_MAPPER =
        new EnumMapper<>(SearchGroupMetadata.class);
    /**
     * Default limit of results to return in search queries when not specified.
     */
    private int defaultLimit;

    /**
     * Max number of results allowed to be returned in search queries.
     */
    private int maxLimit;

    /**
     * Select clause parsed from user request.
     */
    private Set<Field> selectClause = null;

    /**
     * Where conditions parsed from user request.
     */
    private List<Condition> whereClause = null;

    /**
     * SortBy fields parsed from user request.  LinkedHashSet preserves insertion order
     */
    protected LinkedHashSet<SortField<?>> orderBy = null;

    /**
     * Tracks sorted on columns, used to create cursor conditions and read results.
     *
     * <p>{@link LinkedHashMap} used to maintain insertion order</p>
     */
    @VisibleForTesting
    @Nonnull
    Set<SortedOnColumn> sortedOnColumns = new LinkedHashSet<>();

    private static final String NULL = "null";

    private static final Set<PrimitiveFieldApiDTO> ENUMERATED_PRIMITIVE_FIELDS = new HashSet<PrimitiveFieldApiDTO>() {
        {
            add(PrimitiveFieldApiDTO.entityType());
            add(PrimitiveFieldApiDTO.severity());
            add(PrimitiveFieldApiDTO.entityState());
            add(PrimitiveFieldApiDTO.environmentType());
        }
    };

    private static final Map<PrimitiveFieldApiDTO, Function> ENUM_FIELD_API_TO_JOOQ_MAPPER = new HashMap<PrimitiveFieldApiDTO, Function>() {{
        put(PrimitiveFieldApiDTO.entityType(), EntityTypeMapper.fromApiToSearchSchemaFunction);
        put(PrimitiveFieldApiDTO.groupType(), GroupTypeMapper.fromApiToSearchSchemaFunction);
        put(PrimitiveFieldApiDTO.severity(), EntitySeverityMapper.fromApiToSearchSchemaFunction);
        put(PrimitiveFieldApiDTO.entityState(), EntityStateMapper.fromApiToSearchSchemaFunction);
        put(PrimitiveFieldApiDTO.environmentType(), EnvironmentTypeMapper.fromApiToSearchSchemaFunction);
    }};

    /**
     * Creates a query to retrieve data from the search db.
     * @param readOnlyDSLContext a context for making read-only database queries
     * @param defaultLimit default limit of results to return
     * @param maxLimit max number of results to return
     */
    protected AbstractSearchQuery(@NonNull final DSLContext readOnlyDSLContext,
            final int defaultLimit, final int maxLimit) {
        super(readOnlyDSLContext);
        this.defaultLimit = defaultLimit;
        this.maxLimit = maxLimit;
    }

    /**
     * Retrieve selected fields for entities or groups, filtered and sorted based on the criteria
     * defined in the request.
     *
     * @return a paginated list of selected fields for entities or groups
     * @throws SearchQueryFailedException problems processing request
     */
    public SearchQueryPaginationResponse<SearchQueryRecordApiDTO> readQueryAndExecute()
            throws SearchQueryFailedException {

        final int totalRecordCount = fetchTotalRecordCount();

        final Select<Record> paginatedQuery = buildCompleteQuery();

        //Decouple fetch from query chain for testing purposes
        Result<Record> records = getReadOnlyDSLContext().fetch(paginatedQuery);

        //We apply different parsing on records.  PreviousCursor based queries require records
        //to be reversed as well as other logic of which records to use for cursor and results
        return this.cursorBasedQuery() && this.previousCursorBasedQuery()
                ? this.paginatePreviousResponse(records, totalRecordCount)
                : this.paginateNextCursorResponse(records, totalRecordCount);
    }

    /**
     * Runs query to get total record count.
     * @return total record count from query
     */
    private int fetchTotalRecordCount() {
        //TODO Performance improvement for total counts
        return getReadOnlyDSLContext()
                .fetchCount(
                        DSL.selectFrom(getSearchTable()).where(buildWhereClauses())
                );
    }

    @Override
    protected Set<Field> buildSelectFields() {
        if (this.selectClause == null) {
            Set<Field> tableFields = buildCommonFields();
            tableFields.addAll(buildNonCommonFields());
            //We add the OrderByApiDtos to select clause
            //Their values are required to create pagination cursors
            tableFields.addAll(buildSelectFieldsFromOrderBys());
            this.selectClause = Collections.unmodifiableSet(tableFields);
        }
        return this.selectClause;
    }

    /**
     * We need to add all sort on columns to the select clause.
     *
     * <p>There values will be read separately and used to create paginated cursors</p>
     * @return list of {@link Field} created user configured {@link OrderByApiDTO}
     */
    @NotNull
    private List<Field> buildSelectFieldsFromOrderBys() {
        List<OrderByApiDTO> orderBys = getOrderBy();
        final List<Field> fields = new LinkedList<>();
        if (orderBys.isEmpty()) {
            //default orderBy Name and OID will be applied,  those fields always added to
            //selectClause as part of buildCommonFields()
            return fields;
        }

        //User defined sorts provided
        fields.addAll(orderBys.stream()
                .map(orderby -> buildSelectFieldFromOrderBy(orderby.getField()))
                .collect(Collectors.toList()));
        return fields;
    }

    /**
     * Returns collection of {@link Field}s to always include in query fetch.
     *
     * <p>We always return these fields regardless of user requesting them or not</p>
     *
     * @return collection of {@link Field}s fields
     */
    protected Set<Field> buildCommonFields() {
        return new HashSet<Field>() {{
            add(buildAndTrackSelectFieldFromEntityType(PrimitiveFieldApiDTO.oid()));
            add(buildAndTrackSelectFieldFromEntityType(PrimitiveFieldApiDTO.name()));
        }};
    }

    @Override
    protected abstract Map<FieldApiDTO, SearchMetadataMapping> lookupMetadataMapping();

    protected abstract List<Condition> buildTypeSpecificConditions();

    @Nonnull
    protected abstract List<FieldApiDTO> getSelectedFields();

    @Nullable
    protected abstract WhereApiDTO getWhere();

    @Nullable
    protected abstract PaginationApiDTO getPaginationApiDto();

    /**
     * Gets {@link OrderByApiDTO} from Api request.
     * @return list of {@link OrderByApiDTO}
     */
    @NonNull
    protected List<OrderByApiDTO> getOrderBy() {
        PaginationApiDTO pag = getPaginationApiDto();
        return pag == null ? Collections.emptyList() : pag.getOrderBy();
    }

    /**
     * Build where {@link Condition}s.
     *
     * @return a list of {@link Condition}
     */
    @VisibleForTesting
    List<Condition> buildWhereClauses() {
        if (this.whereClause == null) {
            List<Condition> conditions = new LinkedList<>();
            conditions.addAll(buildTypeSpecificConditions());
            conditions.addAll(buildGenericConditions());
            whereClause = Collections.unmodifiableList(conditions);
        }

        return whereClause;
    }

    /**
     * Builds query applying all clauses.
     * @return Complete query translation from user configuration
     * @throws SearchQueryFailedException thrown if error processing
     */
    @VisibleForTesting
    Select<Record> buildCompleteQuery()
            throws SearchQueryFailedException {
        SelectSeekStepN baseQuery = getReadOnlyDSLContext()
                .select(buildSelectFields())
                .from(getSearchTable())
                .where(buildWhereClauses())
                .orderBy(buildOrderByFields());

        return this.cursorBasedQuery()
                ? baseQuery.seek(buildSeek()).limit(limit())
                : baseQuery.limit(limit());
    }

    /**
     * Returns true if query has cursor configured.
     * @return true if cursor based query
     */
    private boolean cursorBasedQuery() {
        PaginationApiDTO paginationApiDTO = this.getPaginationApiDto();
        return paginationApiDTO != null && paginationApiDTO.hasCursor();
    }

    /**
     * Indicates if current request is based on nextCursor.
     * @return boolean if nextCursors to be applied
     */
    private boolean nextCursorBasedQuery() {
        return cursorBasedQuery()
                && SearchPaginationUtil.isNextCursor(this.getPaginationApiDto().getCursor());
    }

    /**
     * Indicates if current request is based on previousCursor.
     * @return boolean if nextCursors to be applied
     */
    private boolean previousCursorBasedQuery() {
        return cursorBasedQuery()
                && SearchPaginationUtil.isPreviousCursor(this.getPaginationApiDto().getCursor());
    }

    /**
     * Returns limit based on user request or default limits.
     * @return limit of request
     */
    private int limit() {
        PaginationApiDTO pagDTO = getPaginationApiDto();

        int limit;

        if (pagDTO != null && pagDTO.hasLimit()) {
            int requestLimit = pagDTO.getLimit();
            limit = requestLimit > maxLimit ? maxLimit : requestLimit;
        } else {
            limit = this.defaultLimit;
        }

        return limit + 1; //We add extra limit for nextCursor
    }

    @NonNull
    @VisibleForTesting
    protected LinkedHashSet<SortField<?>> buildOrderByFields() {
        if (this.orderBy == null) {
            //LinkedHashSet to maintain order of insertions
            final LinkedHashSet<SortField<?>> sortFields = new LinkedHashSet<>();

            if (getOrderBy().isEmpty()) {
                sortFields.add(buildAndTrackOrderByFields(PrimitiveFieldApiDTO.name(), SortOrder.ASC));
            } else {
                getOrderBy().forEach(orderby -> {
                    SortField<?> sortField = buildAndTrackOrderByFields(orderby.getField(), orderby.isAscending() ? SortOrder.ASC : SortOrder.DESC);
                    sortFields.add(sortField);
                });
            }
            sortFields.add(buildAndTrackOrderByFields(PrimitiveFieldApiDTO.oid(), SortOrder.ASC));
            this.orderBy = sortFields;
        }
        return this.orderBy;
    }


    @VisibleForTesting
    Object[] buildSeek() throws SearchQueryFailedException {
        String nextCursor = this.getPaginationApiDto().getCursor();

        List<String> cursorValues = SearchPaginationUtil.getCursorWithoutPrefix(nextCursor);

        if (this.sortedOnColumns.size() != cursorValues.size()) {
            throw new IllegalArgumentException("Cursor values not matching orderby fields");
        }

        Object[] seekValues = new Object[cursorValues.size()];

        List<SortedOnColumn> sortedOnColumnArray = new ArrayList<>(sortedOnColumns);

        for (int i = 0; i < sortedOnColumnArray.size(); i++) {
            SortedOnColumn sortedOnColumn = sortedOnColumnArray.get(i);
            String value  = cursorValues.get(i);
            Type dataType = sortedOnColumn.getSearchMetadataMapping().getApiDatatype();
            seekValues[i] = cursorValueIsNull(value)
                    ? getDefaultFieldValueWhenNull(dataType) : getDataSpecificValue(value, dataType);

        }
        return seekValues;
    }

    /**
     * Returns default value to use for sorting when value are null.
     * @param dataType data type
     * @return default value for dataType
     */
    private Object getDefaultFieldValueWhenNull(Type dataType) {
        switch (dataType) {
            case NUMBER:
                return -Double.MAX_VALUE;
            case INTEGER:
                return Long.valueOf(Integer.MIN_VALUE);
            case BOOLEAN:
                return false;
            default:
                return "";
        }
    }

    /**
     * Returns data converted value based on dataType.
     * @param value to be converted based on dataType
     * @param dataType object type value is saved as in db
     * @return converted value
     * @throws SearchQueryFailedException if dataType is unsupported
     */
    private Object getDataSpecificValue(String value, Type dataType) throws SearchQueryFailedException {
        switch (dataType) {
            case TEXT:
            case ENUM:
            case MULTI_TEXT:
                return value;
            case NUMBER:
                return Double.valueOf(value);
            case INTEGER:
                return Long.valueOf(value);
            case BOOLEAN:
                return Boolean.valueOf(value);
            default:
                throw new SearchQueryFailedException("Unsupported dataType: "
                        + dataType.name());
        }

    }

    /**
     * Returns true if cursor value is 'null'.
     * @param value to be evaluated
     * @return true if 'null'
     */
    private boolean cursorValueIsNull(final String value) {
        return value.equals(NULL);
    }

    /**
     * Returns collection of {@link Field}s mapped from {@link PrimitiveFieldApiDTO}.
     *
     * <p>Excludes basic fields i.e, oid, name, type</p>
     *
     * @return collection of {@link Field}s fields
     */
    @VisibleForTesting
    Set<Field> buildNonCommonFields() {
        return getSelectedFields()
            .stream()
            .map(entityField -> this.buildAndTrackSelectFieldFromEntityType(entityField) )
            .collect(Collectors.toSet());
    }

    /**
     *Returns a {@link SearchQueryPaginationResponse} containing pagination results.
     *
     * @param records returned from query
     * @param totalRecordCount total number of records available
     * @return {@link SearchQueryPaginationResponse}
     */
    @VisibleForTesting
    SearchQueryPaginationResponse<SearchQueryRecordApiDTO> paginatePreviousResponse(
        @Nonnull final Result<Record> records,
        @Nonnull Integer totalRecordCount) {

        final int recordsSize = records.size();

        //Map all the results and reverse the order
        final List<SearchQueryRecordApiDTO> allResults = records.map(this.recordMapper());
        Collections.reverse(allResults);

        final List<SearchQueryRecordApiDTO> resultsExcludingCursorRows;
        final String previousCursor;
        final String nextCursor;
        //Previous Cursor based resulted are in reverse order because of the way we apply
        //orderBy and get seek() working.  After mapping we need to reverse the collections.
        if (recordsSize < limit()) {
            resultsExcludingCursorRows = allResults;
            previousCursor = null;
            nextCursor = buildNextCursor(records.get(0));
        } else {
            //Start with index 1,  index 0 is just the additional record that tells use there
            //is a previousCursor to be set
            resultsExcludingCursorRows = allResults.subList(1, recordsSize);
            //The cursor does not point to the last row indicating where next page should start,
            //It points to the last entry in the current page
            previousCursor = buildPreviousCursor(records.get(recordsSize - 2));
            //Will use the first record for nextCursor, this is really the last record of the page
            // results as we reverse the results
            nextCursor = buildNextCursor(records.get(0));
        }

        return new SearchQueryPaginationResponse<>(resultsExcludingCursorRows, previousCursor, nextCursor,
                totalRecordCount);
    }

    /**
     *Returns a {@link SearchQueryPaginationResponse} containing pagination results.
     *
     * @param records returned from query
     * @param totalRecordCount total number of records available
     * @return {@link SearchQueryPaginationResponse}
     */
    @VisibleForTesting
    SearchQueryPaginationResponse<SearchQueryRecordApiDTO> paginateNextCursorResponse(
            @Nonnull final Result<Record> records,
            @Nonnull Integer totalRecordCount) {
        final int recordsSize = records.size();
        final List<SearchQueryRecordApiDTO> allResults = records.map(this.recordMapper());
        final List<SearchQueryRecordApiDTO> resultsExcludingCursorRows;
        final String nextCursor;

        if (recordsSize < limit()) {
            resultsExcludingCursorRows = allResults;
            nextCursor = null;
        } else {
            resultsExcludingCursorRows = allResults.subList(0, recordsSize - 1);
            //The cursor does not point to the last row indicating where next page should start,
            //It points to the last entry in the current page
            nextCursor = buildNextCursor(records.get(recordsSize - 2));
        }

        //First page run will not have next cursor configured
        final String previousCursor = this.nextCursorBasedQuery()
                ? buildPreviousCursor(records.get(0)) : null;

        return new SearchQueryPaginationResponse<>(resultsExcludingCursorRows, previousCursor, nextCursor,
                totalRecordCount);
    }



    /**
     * Will create a nextCursor marker.
     * @param record Record's values used to create nextCursor
     * @return nextCursor
     */
    private String buildNextCursor(Record record) {
        List<String> cursorValues = getCursorValues(record);
        return SearchPaginationUtil.constructNextCursor(cursorValues);
    }

    /**
     * Returns cursor values of given record based on sortedOnColumns.
     * @param record to be read and provide cursor values
     * @return Collection of cursor values
     */
    @NotNull
    private List<String> getCursorValues(Record record) {
        return this.sortedOnColumns.stream().map(sortedOnColumn -> {
            SearchMetadataMapping columnMetadata = sortedOnColumn.getSearchMetadataMapping();
            final String columnAlias =
                    getColumnAlias(columnMetadata.getColumnName(), columnMetadata.getJsonKeyName());
            final Object value = getValueFromRecord(record, columnAlias);
            return value == null ? null : value.toString();
        }).collect(Collectors.toList());
    }

    /**
     * Will create a previousCursor marker.
     * @param record Record's values used to create nextCursor
     * @return nextCursor
     */
    private String buildPreviousCursor(Record record) {
        //Iterate through sortbyColumns
        List<String> cursorValues = getCursorValues(record);
        return SearchPaginationUtil.constructPreviousCursor(cursorValues);
    }

    protected List<Condition> buildGenericConditions() {
        final WhereApiDTO whereEntity = getWhere();
        if (whereEntity == null) {
            return Collections.EMPTY_LIST;
        }
        final List<Condition> conditions = new LinkedList<>();
        for (ConditionApiDTO condition: whereEntity.getConditions()) {
            Field field = this.buildFieldForApiField(condition.getField());

            if (condition instanceof TextConditionApiDTO) {
                conditions.add(parseCondition((TextConditionApiDTO)condition, field));
            } else if (condition instanceof InclusionConditionApiDTO) {
                conditions.add(parseCondition((InclusionConditionApiDTO)condition, field));
            } else if (condition instanceof NumberConditionApiDTO) {
                conditions.add(parseCondition((NumberConditionApiDTO)condition, field));
            } else if (condition instanceof IntegerConditionApiDTO) {
                conditions.add(parseCondition((IntegerConditionApiDTO)condition, field));
            } else if (condition instanceof BooleanConditionApiDTO) {
                conditions.add(parseCondition((BooleanConditionApiDTO)condition, field));
            }
        }
        return conditions;
    }

    private boolean isEnumeratedField(FieldApiDTO field) {
        return ENUMERATED_PRIMITIVE_FIELDS.contains(field);
    }

    /**
     * Builds {@link Condition} from {@link TextConditionApiDTO}.
     *
     * @param entityCondition to build from
     * @param field the field to compare against the provided condition
     * @return constructed {@link Condition}
     */
    private Condition parseCondition(@Nonnull TextConditionApiDTO entityCondition, @Nonnull Field field) {
        String value = entityCondition.getValue();
        FieldApiDTO fieldApiDTO = entityCondition.getField();
        List<String> parsedValues = parseTextAndInclusionConditions(fieldApiDTO, Collections.singletonList(value));
        String caseInsensitiveRegex = "(?i)";
        return isEnumeratedField(fieldApiDTO) ? field.eq(parsedValues.get(0)) : field.likeRegex(caseInsensitiveRegex.concat(parsedValues.get(0)));
    }

    /**
     * Builds {@link Condition} from {@link InclusionConditionApiDTO}.
     *
     * @param entityCondition to build from
     * @param field the field to compare against the provided condition
     * @return constructed {@link Condition}
     */
    private Condition parseCondition(@Nonnull InclusionConditionApiDTO entityCondition, @Nonnull Field field) {
        final List<String> requestedValues = entityCondition.getValue(); //List of enumLiterals only
        FieldApiDTO fieldApiDTO = entityCondition.getField();
        final List<String> queryValues = parseTextAndInclusionConditions(fieldApiDTO, requestedValues);
        return createInclusionDTOBasedConditions(queryValues, fieldApiDTO, field);
    }

    /**
     * Builds {@link Condition} from {@link NumberConditionApiDTO}.
     *
     * @param entityCondition to build from
     * @param field the field to compare against the provided condition
     * @return constructed {@link Condition}
     */
    @Nonnull
    private Condition parseCondition(@Nonnull NumberConditionApiDTO entityCondition, @Nonnull Field field) {
        final Double value = entityCondition.getValue();
        return applyNumericOperators(field, entityCondition.getOperator(), value);
    }

    /**
     * Builds {@link Condition} from {@link IntegerConditionApiDTO}.
     *
     * @param entityCondition to build from
     * @param field the field to compare against the provided condition
     * @return constructed {@link Condition}
     */
    @NonNull
    private Condition parseCondition(@Nonnull IntegerConditionApiDTO entityCondition, @Nonnull Field field) {
        Long value = entityCondition.getValue();
        return applyNumericOperators(field, entityCondition.getOperator(), value);
    }

    /**
     * Builds {@link Condition} from {@link BooleanConditionApiDTO}.
     *
     * @param entityCondition to build from
     * @param field the field to compare against the provided condition
     * @return constructed {@link Condition}
     */
    private Condition parseCondition(@Nonnull BooleanConditionApiDTO entityCondition, @Nonnull Field field) {
        boolean value = entityCondition.getValue();
        return field.eq(value);
    }

    /**
     * Construct conditions for inclusionDTO.
     *
     * @param queryValues the string values to use in query, if enum, already mapped
     * @param fieldApiDTO corresponding {@link FieldApiDTO} of configured condition.
     * @param field field the field to compare against the provided condition
     * @return query conditions
     */
    private Condition createInclusionDTOBasedConditions(final List<String> queryValues, final FieldApiDTO fieldApiDTO, @Nonnull Field field) {
        SearchMetadataMapping mapping = getMetadataMapping().get(fieldApiDTO);
        if (!mapping.getApiDatatype().equals(Type.MULTI_TEXT)) {
            return field.in(queryValues);
        }
        //For multiText columns we cannot apply in clause.
        //JsonB multitext columns save values as strings
        //Example: attrs_column = {colKey ["123", "456"]}
        //         User request inclusion where colKey in "123"
        //         PSQL translation -> where colkey in ("123")
        //         Because they are strings this equates to:  "123" = '["123","456"]' with inclusion
        //         so no match will be made.  We can use like conditions to get desired results


        List<String> valuesWithSurroundingQuotes = queryValues.stream()
                .map(AbstractSearchQuery::mapToStringLiteral).collect(
                Collectors.toList());

        String queryValue = String.join("|", valuesWithSurroundingQuotes);
        return field.likeRegex(queryValue);

    }

    /**
     * Maps query value to string literal to be used in regex matching.
     * @param queryValue string value to be matched on
     * @return string literal of queryValue
     */
    @VisibleForTesting
    static String mapToStringLiteral(String queryValue) {
        String sanitized = queryValue.replaceAll("[-.\\+*?\\[^\\]$(){}=!<>|:\\\\]", "\\\\$0");
        //Surrounding in quotes, this is specific to values in multitext columns
        return "\"" + sanitized + "\"";
    }

    /**
     * If {@link Type#ENUM} conditions this maps api enums to jooq equivalents.
     *
     * @param fieldApiDto Corresponding {@link FieldApiDTO} of configured condition.
     * @param values request api values, enums will be converted to jooq enums
     * @return jooq enum references or original values returned
     */
    private List<String> parseTextAndInclusionConditions(FieldApiDTO fieldApiDto, List<String> values ) {
        SearchMetadataMapping mapping = getMetadataMapping().get(fieldApiDto);

        if (!ENUM_FIELD_API_TO_JOOQ_MAPPER.containsKey(fieldApiDto)) {
            return values;
        }

        if (fieldApiDto.equals(PrimitiveFieldApiDTO.entityType())) {
            throw new IllegalArgumentException("EntityType condition should only exist on Select Query");
        }

        final EnumMapper apiEnumMapper = new EnumMapper(mapping.getEnumClass());
        final Function enumMappingFunction = ENUM_FIELD_API_TO_JOOQ_MAPPER.get(fieldApiDto);

        return values.stream()
                .map(apiEnumLiteral -> apiEnumMapper.valueOf(apiEnumLiteral).get())
                .map(obj -> enumMappingFunction.apply(obj).toString())
                .collect(Collectors.toList());
    }

    @Nonnull
    private Condition applyNumericOperators(@Nonnull Field field, @Nonnull Operator operator, @NonNull Object value ) {
        Condition condition = null;
        switch (operator) {
            case EQ:
                return field.eq(value);
            case NEQ:
                return field.ne(value);
            case GT:
                return field.gt(value);
            case LT:
                return field.lt(value);
            case GE:
                return field.ge(value);
            case LE:
                return field.le(value);
        }
        return condition;
    }

    /**
     * Get {@link Field} configuration for entityField from mappings.
     *
     * @param apiField {@link FieldApiDTO} to parse into select query {@link Field}
     * @return Field configuration based on {@link FieldApiDTO}
     */
    @VisibleForTesting
    Field<?> buildSelectFieldFromOrderBy(FieldApiDTO apiField) {
        return buildFieldForApiField(apiField, true);
    }

    /**
     * Get {@link Field} configuration for entityField from mappings.
     *
     * @param apiField {@link FieldApiDTO} to parse into select query {@link Field}
     * @param sortOrder {@link SortOrder} sortOrder request on field
     * @return Field configuration based on {@link FieldApiDTO}
     */
    @VisibleForTesting
    SortField<?> buildAndTrackOrderByFields(final FieldApiDTO apiField, final SortOrder sortOrder) {
        SearchMetadataMapping columnMetadata = getMetadataMapping().get(apiField);
        Field<?> field = buildFieldForApiField(apiField, false);

        //We use default values to get correct sorting when values null
        field = coalesce(field, inline(getDefaultFieldValueWhenNull(columnMetadata.getApiDatatype())));

        SortOrder cursorConsideredSortOrder = getSortOrderBasedOnCursor(sortOrder);
        trackSortedOnFields(columnMetadata, field, cursorConsideredSortOrder);
        SortField<?> sortField = field.sort(cursorConsideredSortOrder);
        //Sorting descending order, we want null values to appear last
        return cursorConsideredSortOrder.equals(SortOrder.DESC)
                ? sortField.nullsLast() : sortField.nullsFirst();
    }

    /**
     * Returns {@link SortOrder} considering cursor selection.
     *
     * <p>If previousCursor is used we need to reverse the sortOrder provided</p>
     * @param sortOrder current sort order
     * @return the sortOrder to apply to field
     */
    private SortOrder getSortOrderBasedOnCursor(SortOrder sortOrder) {
        if (!cursorBasedQuery() || (cursorBasedQuery() && nextCursorBasedQuery()) ) {
            return sortOrder;
        }

        return sortOrder.equals(SortOrder.ASC)
                ? sortOrder.DESC : sortOrder.ASC;
    }


    /**
     * This will track user request and common fields, used for reading results.
     *
     * @param columnMetadata metadata of field being tracked
     * @param field jooq configured field being tracked
     * @param cursorConsideredSortOrder sortOrder applied to field
     */
    protected void trackSortedOnFields(@Nonnull final SearchMetadataMapping columnMetadata, @Nonnull final Field<?> field, @Nonnull final SortOrder cursorConsideredSortOrder) {
        //Even if duplicates are added, order is maintained from time first inserted
        sortedOnColumns.add(new SortedOnColumn(columnMetadata, field, cursorConsideredSortOrder));
    }

    /**
     * Collects the various configurations of a Field to be sorted on.
     */
    @VisibleForTesting
    class SortedOnColumn {

        @Nonnull
        private SearchMetadataMapping searchMetadataMapping;

        @Nonnull
        private Field<?> field;

        @Nonnull
        private SortOrder cursorConsideredSortOrder;

        protected SortedOnColumn(@Nonnull SearchMetadataMapping searchMetadataMapping, @Nonnull Field<?> field,
                @Nonnull SortOrder cursorConsideredSortOrder) {
            this.searchMetadataMapping = Objects.requireNonNull(searchMetadataMapping);
            this.field = Objects.requireNonNull(field);
            this.cursorConsideredSortOrder = Objects.requireNonNull(cursorConsideredSortOrder);
        }

        public Field getField() {
            return this.field;
        }

        public SortOrder getCursorConsideredSortOrder() {
            return this.cursorConsideredSortOrder;
        }

        public SearchMetadataMapping getSearchMetadataMapping() {
            return this.searchMetadataMapping;
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof SortedOnColumn
                    && Objects.equals(searchMetadataMapping, ((SortedOnColumn)other).searchMetadataMapping)
                    && Objects.equals(field, ((SortedOnColumn)other).field)
                    && Objects.equals(cursorConsideredSortOrder, ((SortedOnColumn)other).cursorConsideredSortOrder);
        }

        @Override
        public int hashCode() {
            return Objects.hash(searchMetadataMapping, field, cursorConsideredSortOrder);
        }
    }
}
