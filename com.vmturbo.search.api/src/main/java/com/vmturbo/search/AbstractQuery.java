package com.vmturbo.search;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.jooq.DSLContext;
import org.jooq.DataType;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.RecordMapper;
import org.jooq.Table;
import org.jooq.TableField;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;

import com.vmturbo.api.dto.searchquery.EntityQueryApiDTO;
import com.vmturbo.api.dto.searchquery.FieldApiDTO;
import com.vmturbo.api.dto.searchquery.FieldValueApiDTO;
import com.vmturbo.api.dto.searchquery.PrimitiveFieldApiDTO;
import com.vmturbo.api.dto.searchquery.RelatedActionFieldApiDTO;
import com.vmturbo.api.dto.searchquery.SearchQueryRecordApiDTO;
import com.vmturbo.extractor.schema.tables.SearchEntity;
import com.vmturbo.extractor.schema.tables.records.SearchEntityRecord;
import com.vmturbo.search.mappers.EntitySeverityMapper;
import com.vmturbo.search.mappers.EntityStateMapper;
import com.vmturbo.search.mappers.EntityTypeMapper;
import com.vmturbo.search.mappers.EnvironmentTypeMapper;
import com.vmturbo.search.mappers.GroupTypeMapper;
import com.vmturbo.search.mappers.TypeMapper;
import com.vmturbo.search.metadata.SearchMetadataMapping;

/**
 * A representation of a single API query, mapped to a SQL query.
 */
public abstract class AbstractQuery {

    /**
     * Const field used by searchAll to handle entity/groupType parsing.
     */
    protected static final PrimitiveFieldApiDTO PRIMITIVE_TYPE = PrimitiveFieldApiDTO.primitive("EntityOrGroupType");

    private static final Logger logger = LogManager.getLogger();

    /**
     * The JOOQ table to use for search queries.
     */
    protected static final Table<SearchEntityRecord> searchTable = SearchEntity.SEARCH_ENTITY;

    /**
     * Mapping of common dtos to database columns.
     *
     * <p>TODO: Seems like this mapping should ideally be part of the search metadata.
     * Meaning, it should be included in {@link SearchMetadataMapping}.</p>
     */
    private static Map<FieldApiDTO, Field> primaryTableColumns =
        new HashMap<FieldApiDTO, Field>() {{
            put(PrimitiveFieldApiDTO.oid(), SearchEntity.SEARCH_ENTITY.OID);
            put(PRIMITIVE_TYPE, SearchEntity.SEARCH_ENTITY.TYPE);
            put(PrimitiveFieldApiDTO.entityType(), SearchEntity.SEARCH_ENTITY.TYPE);
            put(PrimitiveFieldApiDTO.groupType(), SearchEntity.SEARCH_ENTITY.TYPE);
            put(PrimitiveFieldApiDTO.name(), SearchEntity.SEARCH_ENTITY.NAME);
            put(PrimitiveFieldApiDTO.severity(), SearchEntity.SEARCH_ENTITY.SEVERITY);
            put(PrimitiveFieldApiDTO.entityState(), SearchEntity.SEARCH_ENTITY.STATE);
            put(PrimitiveFieldApiDTO.environmentType(), SearchEntity.SEARCH_ENTITY.ENVIRONMENT);
            put(RelatedActionFieldApiDTO.actionCount(), SearchEntity.SEARCH_ENTITY.NUM_ACTIONS);
        }};

    private static final Map<PrimitiveFieldApiDTO, Function> ENUM_FIELD_JOOQ_TO_API_MAPPER = new HashMap<PrimitiveFieldApiDTO, Function>() {{
        put(PrimitiveFieldApiDTO.entityType(), EntityTypeMapper.fromSearchSchemaToApiFunction);
        put(PrimitiveFieldApiDTO.groupType(), GroupTypeMapper.fromSearchSchemaToApiFunction);
        put(PrimitiveFieldApiDTO.severity(), EntitySeverityMapper.fromSearchSchemaToApiFunction);
        put(PrimitiveFieldApiDTO.entityState(), EntityStateMapper.fromSearchSchemaToApiFunction);
        put(PrimitiveFieldApiDTO.environmentType(), EnvironmentTypeMapper.fromSearchSchemaToApiFunction);
        put(PRIMITIVE_TYPE, TypeMapper.fromSearchSchemaToApiFunction); // For Search All
    }};

    /**
     * Provides functionality for reading Json.
     */
    private static ObjectMapper objectMapper = new ObjectMapper();

    /**
     * A context for making read-only database queries.
     */
    private final DSLContext readOnlyDSLContext;

    /**
     * A mapping of FieldApiDTO -> metadata mapping describing that field.
     */
    @VisibleForTesting
    Map<FieldApiDTO, SearchMetadataMapping> metadataMapping;

    /**
     * Tracks requested columns, used to know which information to read when building response.
     */
    private Map<SearchMetadataMapping, FieldApiDTO> requestedColumns = new HashMap<>();


    /**
     * Creates a query to retrieve data from the search db.
     *
     * @param readOnlyDSLContext a context for making read-only database queries
     */
    protected AbstractQuery(@NonNull final DSLContext readOnlyDSLContext) {
        this.readOnlyDSLContext = Objects.requireNonNull(readOnlyDSLContext);
    }

    protected Map<FieldApiDTO, SearchMetadataMapping> getMetadataMapping() {
        if (metadataMapping == null) {
            metadataMapping = lookupMetadataMapping();
        }
        return metadataMapping;
    }

    /**
     * Retrieves a metadata mapping based on a metadata key from the API request.
     *
     * @return a mapping of FieldApiDTO -> metadata mapping describing that field
     */
    protected abstract Map<FieldApiDTO, SearchMetadataMapping> lookupMetadataMapping();

    /**
     * Maps fetched {@link Record}s into {@link SearchQueryRecordApiDTO}.
     *
     * @return mapper to process records from Search DB
     */
    @VisibleForTesting
    RecordMapper<Record, SearchQueryRecordApiDTO> recordMapper() {
        return record -> {
            Long oid = record.get(SearchEntity.SEARCH_ENTITY.OID);
            return SearchQueryRecordApiDTO.entityOrGroupResult(oid)
                .values(mapValues(record))
                .build();
        };
    }

    List<FieldValueApiDTO> mapValues(Record record) {
        List<FieldValueApiDTO> entityFieldValues = new LinkedList<>();
        requestedColumns.entrySet()
            .forEach(entry -> {
                mapRecordToValue(record, entry.getKey(), entry.getValue())
                    .ifPresent(entityFieldValues::add);
            });
        return entityFieldValues;
    }

    /**
     * Maps {@link Record} to {@link FieldValueApiDTO}.
     *
     * @param record containing the db result for parsing
     * @param columnMetadata metadata for column which we will be reading from record
     * @param fieldApiDto  fieldDto to couple with value for {@link FieldValueApiDTO}
     * @return FieldValue
     */
    @VisibleForTesting
    Optional<FieldValueApiDTO> mapRecordToValue(@Nonnull Record record, @Nonnull SearchMetadataMapping columnMetadata, @Nonnull FieldApiDTO fieldApiDto) {
        final String columnAlias = getColumnAlias(columnMetadata.getColumnName(), columnMetadata.getJsonKeyName());

        //OID field is added at base-level of response dto, rather than a value in list
        if (fieldApiDto.equals(PrimitiveFieldApiDTO.oid())) {
            return Optional.empty();
        }

        final Object value = this.getValueFromRecord(record, columnAlias);

        // Do not try to map a missing field
        if (value == null) {
            return Optional.empty();
        }

        FieldValueApiDTO fieldValue = null;
        switch (columnMetadata.getApiDatatype()) {
            case TEXT:
            case ENUM:
                if (ENUM_FIELD_JOOQ_TO_API_MAPPER.containsKey(fieldApiDto)) {
                    Function jooqToApiEnumMapper = ENUM_FIELD_JOOQ_TO_API_MAPPER.get(fieldApiDto);
                    fieldValue = fieldApiDto.enumValue(
                            readEnumRecordAndMap(record, columnAlias, jooqToApiEnumMapper));
                } else {
                    fieldValue = fieldApiDto.value((String)value);
                }
                break;
            case NUMBER:
                fieldValue = fieldApiDto.value((double)value, columnMetadata.getUnitsString());
                break;
            case INTEGER:
                if (fieldApiDto.equals(RelatedActionFieldApiDTO.actionCount())) {
                    fieldValue = fieldApiDto.value((int)value);
                } else {
                    fieldValue = fieldApiDto.value((Long)value, columnMetadata.getUnitsString());
                }
                break;
            case BOOLEAN:
                fieldValue = fieldApiDto.value((boolean)value);
                break;
            case MULTI_TEXT:
                try {
                    fieldValue = fieldApiDto.values(objectMapper.readValue((String)value, String[].class));
                } catch (JsonProcessingException e) {
                    logger.error("Error parsing multi-text results");
                }
                break;
            default:
                throw new IllegalArgumentException("Column parsing not implemented:" + columnMetadata.getApiDatatype());
        }

        return Objects.isNull(fieldValue) ? Optional.empty() : Optional.of(fieldValue);
    }

    private <T extends Enum<T>, R extends Enum<R>> R readEnumRecordAndMap(Record record, String columnAlias, Function<T, R> enumMapper) {
        T enumValue =  (T)record.get(columnAlias);
        return enumMapper.apply(enumValue);
    }

    /**
     * Build list of tableField from {@link EntityQueryApiDTO}.
     *
     * @return List of expected {@link TableField}
     */
    @VisibleForTesting
    protected abstract Set<Field> buildSelectFields();

    /**
     * Get {@link Field} configuration for entityField from mappings.
     *
     * @param apiField {@link FieldApiDTO} to parse into select query {@link Field}
     * @return Field configuration based on {@link FieldApiDTO}
     */
    @VisibleForTesting
    Field buildAndTrackSelectFieldFromEntityType(FieldApiDTO apiField) {
        SearchMetadataMapping columnMetadata = getMetadataMapping().get(apiField);
        trackUserRequestedFields(columnMetadata, apiField);
        return buildFieldForApiField(apiField, true);
    }

    /**
     * This will track user request and common fields, used for reading results.
     *
     * @param columnMetadata metadata of field being tracked
     * @param apiField the field being tracked
     */
    protected void trackUserRequestedFields(final SearchMetadataMapping columnMetadata, final FieldApiDTO apiField) {
        requestedColumns.put(columnMetadata, apiField);
    }

    /**
     * Builds a {@link Field} object from {@link SearchMetadataMapping}.
     *
     * @param apiField {@link FieldApiDTO} to parse into select query {@link Field}
     * @return Field
     */
    @VisibleForTesting
    Field<?> buildFieldForApiField(@Nonnull FieldApiDTO apiField) {
        return buildFieldForApiField(apiField, false);
    }

    /**
     * Builds a {@link Field} object from {@link SearchMetadataMapping}.
     *
     * @param apiField {@link FieldApiDTO} to parse into select query {@link Field}
     * @param aliasColumn if field name should contain an alias
     * @return Field
     */
    @VisibleForTesting
    Field<?> buildFieldForApiField(@Nonnull FieldApiDTO apiField, @Nonnull boolean aliasColumn) {
        final SearchMetadataMapping mapping = getMetadataMapping().get(apiField);
        if (mapping == null) {
            throw new IllegalArgumentException("Field " + apiField.toString()
                    + " does not apply to selected type. ");
        }

        final Field<?> field;
        if (this.primaryTableColumns.containsKey(apiField)) {
            //For Primary Columns we use the jooq generated Fields
            field = this.primaryTableColumns.get(apiField);
        } else {
            final String columnName = mapping.getColumnName();
            final String jsonKey = mapping.getJsonKeyName();
            final Field unCastfield = Objects.isNull(jsonKey)
                    ? DSL.field(columnName, getColumnDataType(mapping))
                    : DSL.field(buildSqlStringForJsonBColumns(columnName, jsonKey), getColumnDataType(mapping));
            DataType dataType = getColumnDataType(mapping);
            field = unCastfield.cast(dataType); //Required for proper handling of where and sorting
        }

        return aliasColumn ? addColumnAliasToField(field, mapping) : field;
    }

    /**
     * Adds columnAlias by parsing metadata information.
     *
     * @param unaliasedField Field to attach alias to.
     * @param mapping Metadata mapping for the field
     * @return aliased field
     */
    protected Field<?> addColumnAliasToField(Field<?> unaliasedField, SearchMetadataMapping mapping) {
        final String columnName = mapping.getColumnName();
        final String jsonKey = mapping.getJsonKeyName();
        final String columnAlias = getColumnAlias(columnName, jsonKey);
        return unaliasedField.as(columnAlias);
    }

    private DataType<?> getColumnDataType(SearchMetadataMapping columnMapping) {
        switch (columnMapping.getApiDatatype()) {
            case NUMBER:
                return SQLDataType.DOUBLE;
            case INTEGER:
                return SQLDataType.BIGINT;
            case BOOLEAN:
                return SQLDataType.BOOLEAN;
            case TEXT:
            case ENUM:
            case MULTI_TEXT:
            default:
                return SQLDataType.LONGNVARCHAR;
        }
    }

    private String buildSqlStringForJsonBColumns(@Nonnull String columnName, @Nonnull String jsonKey) {
        return String.format(columnName + "->>'%s'", jsonKey);
    }

    /**
     * Returns alias for querying column.
     *
     * @param columnName column alias name to use if jsonKey is null
     * @param jsonKey column alias name used if present
     * @return alias name for column
     */
    @VisibleForTesting
    @Nonnull
    String getColumnAlias(@Nonnull String columnName, @Nullable String jsonKey) {
        return Objects.isNull(jsonKey) ? columnName : jsonKey;
    }

    protected DSLContext getReadOnlyDSLContext() {
        return readOnlyDSLContext;
    }

    protected Table<SearchEntityRecord> getSearchTable() {
        return searchTable;
    }

    /**
     * Will read the value from record based on columnAlias.
     *
     * @param record to be read
     * @param columnAlias the column to read value from
     * @return value, null if error occurred or column not set
     */
    Object getValueFromRecord(@Nonnull final Record record, @Nonnull final String columnAlias) {
        try {
            return record.get(columnAlias);
        } catch (IllegalArgumentException e) {
            //TODO:  Specify exception,  test processEntities is cause
            //This will only happen during testing. End to end test we may not include all the
            //primary column data.
            logger.info("Record does not contain column " + columnAlias );
            return null;
        }
    }
}
