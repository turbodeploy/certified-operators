package com.vmturbo.search;

import java.util.Collections;
import java.util.HashSet;
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
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.Select;
import org.jooq.SortField;
import org.jooq.SortOrder;

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
import com.vmturbo.api.enums.EntitySeverity;
import com.vmturbo.api.enums.EntityState;
import com.vmturbo.api.enums.EnvironmentType;
import com.vmturbo.api.pagination.searchquery.SearchQueryPaginationResponse;
import com.vmturbo.common.api.mappers.EnumMapper;
import com.vmturbo.extractor.schema.tables.SearchEntity;
import com.vmturbo.search.mappers.EntitySeverityMapper;
import com.vmturbo.search.mappers.EntityStateMapper;
import com.vmturbo.search.mappers.EnvironmentTypeMapper;
import com.vmturbo.search.metadata.SearchMetadataMapping;

/**
 * A representation of a single API search query, mapped to a SQL query.
 */
public abstract class AbstractSearchQuery extends AbstractQuery {

    private static final Set<PrimitiveFieldApiDTO> ENUMERATED_PRIMITIVE_FIELDS = new HashSet<PrimitiveFieldApiDTO>() {
        {
            add(PrimitiveFieldApiDTO.entityType());
            add(PrimitiveFieldApiDTO.severity());
            add(PrimitiveFieldApiDTO.entityState());
            add(PrimitiveFieldApiDTO.environmentType());
        }
    };

    /**
     * String representing request type {@link com.vmturbo.api.enums.GroupType} or {@link com.vmturbo.api.enums.EntityType}.
     */
    private final String type;

    /**
     * Creates a query to retrieve data from the search db.
     *
     * @param type the entity or group type associated with this query
     * @param readOnlyDSLContext a context for making read-only database queries
     */
    protected AbstractSearchQuery(@NonNull String type, @NonNull final DSLContext readOnlyDSLContext) {
        super(readOnlyDSLContext);
        this.type = Objects.requireNonNull(type);
    }

    /**
     * Retrieve selected fields for entities or groups, filtered and sorted based on the criteria
     * defined in the request.
     *
     * @return a paginated list of selected fields for entities or groups
     */
    public SearchQueryPaginationResponse<SearchQueryRecordApiDTO> readQueryAndExecute() {
        Select<Record> query = getReadOnlyDSLContext()
            .select(this.buildSelectFields())
            .from(getSearchTable())
            .where(this.buildWhereClauses())
            .orderBy(this.buildOrderByFields())
            .limit(20);

        //Decouple fetch from query chain for testing purposes
        Result<Record> records = getReadOnlyDSLContext().fetch(query);
        List<SearchQueryRecordApiDTO> results = records.map(this.recordMapper());

        final int totalRecordCount = 0; // Adding mock data
        return paginateResponse(results, totalRecordCount);
    }

    @Override
    protected Set<Field> buildSelectFields() {
        Set<Field> tableFields = buildCommonFields();
        tableFields.addAll(buildNonCommonFields());
        return tableFields;
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
    protected Map<FieldApiDTO, SearchMetadataMapping> lookupMetadataMapping() {
        return lookupMetadataMapping(getType());
    }

    /**
     * Retrieves a metadata mapping based on a metadata key from the API request.
     *
     * @param type the entity or group type associated with this query
     * @return a mapping of FieldApiDTO -> metadata mapping describing that field
     */
    protected abstract Map<FieldApiDTO, SearchMetadataMapping> lookupMetadataMapping(String type);

    protected abstract List<Condition> buildTypeSpecificConditions();

    protected abstract List<FieldApiDTO> getSelectedFields();

    protected abstract WhereApiDTO getWhere();

    @Nullable
    protected abstract PaginationApiDTO getPaginationApiDto();

    protected abstract List<OrderByApiDTO> getOrderBy();

    /**
     * Build where {@link Condition}s.
     *
     * @return a list of {@link Condition}
     */
    @VisibleForTesting
    List<Condition> buildWhereClauses() {
        List<Condition> conditions = new LinkedList<>();
        conditions.addAll(buildTypeSpecificConditions());
        conditions.addAll(buildGenericConditions());
        return conditions;
    }

    @VisibleForTesting
    List<SortField<?>> buildOrderByFields() {
        List<OrderByApiDTO> orderBys = getOrderBy();
        if (orderBys.isEmpty()) {
            return Collections.singletonList(defaultOrderByName());
        }

        return orderBys.stream()
            .map(orderby -> {
                Field<?> field = buildFieldForApiField(orderby.getField());
                return orderby.isAscending() ? field.sort(SortOrder.ASC) : field.sort(SortOrder.DESC);
            })
            .collect(Collectors.toList());
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
     * @param results to be returned in paginated response
     * @param totalRecordCount total number of records available
     * @return {@link SearchQueryPaginationResponse}
     */
    @VisibleForTesting
    SearchQueryPaginationResponse<SearchQueryRecordApiDTO> paginateResponse(
        @Nonnull List<SearchQueryRecordApiDTO> results,
        @Nonnull Integer totalRecordCount) {

        String nextCursor = null;
        // If results available nextCursor will point to some designation
        //        of last record in results
        return new SearchQueryPaginationResponse<>(results, nextCursor, totalRecordCount);
    }

    private List<Condition> buildGenericConditions() {
        final WhereApiDTO whereEntity = getWhere();
        if (Objects.isNull(whereEntity )) {
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
        List<String> values = entityCondition.getValue(); //List of enumLiterals only
        List<String> jooqEnumValues = parseTextAndInclusionConditions(entityCondition.getField(), values);
        //InclusionCondition can only be configured for IN operator
        return field.in(jooqEnumValues);
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
        Double value = entityCondition.getValue();
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
     * If {@link Type#ENUM} conditions this maps api enums to jooq equivalents.
     *
     * @param fieldApiDto Corresponding {@link FieldApiDTO} of configered condition.
     * @param values request api values, enums will be converted to jooq enums
     * @return jooq enum references.
     *         If Field is not {@link Type#ENUM} original values returned
     */
    private List<String> parseTextAndInclusionConditions(FieldApiDTO fieldApiDto, List<String> values ) {
        SearchMetadataMapping mapping = getMetadataMapping().get(fieldApiDto);

        if (!mapping.getApiDatatype().equals(Type.ENUM)) {
            return values;
        }

        final EnumMapper apiEnumMapper;
        final Function enumMappingFunction;

        if (fieldApiDto.equals(PrimitiveFieldApiDTO.entityType())) {
            throw new IllegalArgumentException("EntityType condition should only exist on Select Query");
        } else if (fieldApiDto.equals(PrimitiveFieldApiDTO.severity())) {
            apiEnumMapper = new EnumMapper(EntitySeverity.class);
            enumMappingFunction = EntitySeverityMapper.fromApiToSearchSchemaFunction;
        } else if (fieldApiDto.equals(PrimitiveFieldApiDTO.entityState())) {
            apiEnumMapper = new EnumMapper(EntityState.class);
            enumMappingFunction = EntityStateMapper.fromApiToSearchSchemaFunction;
        } else if (fieldApiDto.equals(PrimitiveFieldApiDTO.environmentType())) {
            apiEnumMapper = new EnumMapper(EnvironmentType.class);
            enumMappingFunction = EnvironmentTypeMapper.fromApiToSearchSchemaFunction;
        } else {
            return values;
        }

        return values.stream()
            .map(apiEnumLiteral -> apiEnumMapper.valueOf(apiEnumLiteral).get())
            .map(obj -> enumMappingFunction.apply(obj).toString())
            .collect(Collectors.toList());
    }

    /**
     * Returns default sort order by name.
     *
     * @return {@link SortField} by name
     */
    private SortField<String> defaultOrderByName() {
        return SearchEntity.SEARCH_ENTITY.NAME.desc();
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

    private String getType() {
        return type;
    }
}
