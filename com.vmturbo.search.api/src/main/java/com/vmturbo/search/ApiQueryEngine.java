package com.vmturbo.search;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.GroupField;
import org.jooq.OrderField;
import org.jooq.Record;
import org.jooq.RecordMapper;
import org.jooq.Result;
import org.jooq.Select;
import org.jooq.Table;
import org.jooq.TableField;
import org.jooq.impl.DSL;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;

import com.vmturbo.api.dto.searchquery.EntityQueryApiDTO;
import com.vmturbo.api.dto.searchquery.FieldApiDTO;
import com.vmturbo.api.dto.searchquery.FieldValueApiDTO;
import com.vmturbo.api.dto.searchquery.PrimitiveFieldApiDTO;
import com.vmturbo.api.dto.searchquery.RelatedActionFieldApiDTO;
import com.vmturbo.api.dto.searchquery.SearchQueryRecordApiDTO;
import com.vmturbo.api.dto.searchquery.SelectEntityApiDTO;
import com.vmturbo.api.enums.EntityType;
import com.vmturbo.api.pagination.searchquery.SearchQueryPaginationResponse;
import com.vmturbo.common.api.mappers.EnumMapper;
import com.vmturbo.extractor.schema.tables.SearchEntity;
import com.vmturbo.extractor.schema.tables.records.SearchEntityRecord;
import com.vmturbo.search.mappers.EntitySeverityMapper;
import com.vmturbo.search.mappers.EntityStateMapper;
import com.vmturbo.search.mappers.EntityTypeMapper;
import com.vmturbo.search.mappers.EnvironmentTypeMapper;
import com.vmturbo.search.metadata.SearchEntityMetadata;
import com.vmturbo.search.metadata.SearchEntityMetadataMapping;
import com.vmturbo.sql.utils.DbEndpoint;

/**
 * Read only from search(extractor) database.
 *
 * <p>Responsible for mapping {@link EntityQueryApiDTO} request into queries, fetching data,
 *    and mapping back into response objects i.e {@link SearchQueryRecordApiDTO} for entities </p>
 **/
public class ApiQueryEngine implements IApiQueryEngine {

    private static final Table<SearchEntityRecord>
            searchEntityTable = SearchEntity.SEARCH_ENTITY;

    private static final Logger logger = LogManager.getLogger();

    private DbEndpoint readonlyDbEndpoint;

    private final boolean enableSearchApi;

    private DSLContext readOnlyDSLContext;

    private EntityQueryApiDTO entityQueryApiDTO;

    private ObjectMapper objectMapper = new ObjectMapper();

    private Map<FieldApiDTO, Field> primaryTableColumns =
            new HashMap<FieldApiDTO, Field>() {{
                put(PrimitiveFieldApiDTO.oid(), SearchEntity.SEARCH_ENTITY.OID);
                put(PrimitiveFieldApiDTO.entityType(), SearchEntity.SEARCH_ENTITY.TYPE);
                put(PrimitiveFieldApiDTO.name(), SearchEntity.SEARCH_ENTITY.NAME);
                put(PrimitiveFieldApiDTO.entitySeverity(), SearchEntity.SEARCH_ENTITY.SEVERITY);
                put(PrimitiveFieldApiDTO.entityState(), SearchEntity.SEARCH_ENTITY.STATE);
                put(PrimitiveFieldApiDTO.environmentType(), SearchEntity.SEARCH_ENTITY.ENVIRONMENT);
                put(RelatedActionFieldApiDTO.actionCount(), SearchEntity.SEARCH_ENTITY.NUM_ACTIONS);
            }};

    @VisibleForTesting
    Map<FieldApiDTO, SearchEntityMetadataMapping> entityMetadata;

    //Tracks requested columns, used to know which information to read when building response
    private Map<SearchEntityMetadataMapping, FieldApiDTO> requestedColumns = new HashMap<>();

    public ApiQueryEngine(DbEndpoint readonlyDbEndpoint, boolean enableSearchApi) {
        this.readonlyDbEndpoint = readonlyDbEndpoint;
        this.enableSearchApi = enableSearchApi;
    }

    /**
     * Initializes DSLContext from {@link DbEndpoint}.
     *
     * @throws Exception if access to DB has not been established
     */
    private void dslContextInitilization()
            throws Exception {
        if (this.readOnlyDSLContext == null && enableSearchApi) {
            this.readOnlyDSLContext = this.readonlyDbEndpoint.dslContext();
        }
    }

    /**
     * Reads {@link EntityQueryApiDTO}, fetching DB data and returning paginated response.
     *
     * @param request query request
     * @return Collection of SearchQueryRecordApiDTO mapped from DB response
     * @throws Exception thrown if error occurs gaining DB access
     */
    @Override
    public SearchQueryPaginationResponse processEntityQuery(@Nonnull final EntityQueryApiDTO request)
            throws Exception {
        if (!enableSearchApi) {
            throw new UnsupportedOperationException("Search API is not yet enabled!");
        }
        logger.info("SearchQueryPaginationResponse processEntityQuery");
            setMetaDataMapping(request);
            this.dslContextInitilization();
            return readQueryAndExecute();
    }

    private SearchQueryPaginationResponse<SearchQueryRecordApiDTO> readQueryAndExecute() {
        logger.info("SearchQueryRecordApiDTO QUERYING");
        Select<Record> query = this.readOnlyDSLContext
                .select(this.buildSelectFields())
                .from(searchEntityTable)
                //                .where(this.buildWhereClauses())
                .limit(20);

        //Decouple fetch from query chain for testing purposes
        Result<Record> records = this.readOnlyDSLContext.fetch(query);

        logger.info("SEARCHDB  4 \n" + records.formatCSV());

        List<SearchQueryRecordApiDTO> results = records.map(this.recordMapper());

        final int totalRecordCount = 0; // Adding mock data
        return paginateResponse(results, totalRecordCount);
    }

    /**
     * Sets metadata mapping based on requesting entityType and entityQueryApiDTO.
     *
     * @param request User request
     */
    @VisibleForTesting
    void setMetaDataMapping(EntityQueryApiDTO request) {
        this.entityQueryApiDTO = request;
        EntityType entityType = request.getSelect().getEntityType();
        EnumMapper<SearchEntityMetadata> searchEntityMetadataEnumMapper = new EnumMapper<>(SearchEntityMetadata.class);
        this.entityMetadata = searchEntityMetadataEnumMapper.valueOf(entityType.name())
                .map(SearchEntityMetadata::getMetadataMappingMap)
                .orElseThrow(() -> new IllegalArgumentException(
                        "No data for entityType " + entityType.name()));
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

        String nextCursor = null; // If results available nextCursor will point to some designation
//        of last record in results
        return new SearchQueryPaginationResponse<>(results, nextCursor, totalRecordCount);
    }

    /**
     * Maps fetched {@link Record}s into {@link SearchQueryRecordApiDTO}.
     *
     * @return mapper to process records from Search DB
     */
    @VisibleForTesting
    RecordMapper<Record, SearchQueryRecordApiDTO> recordMapper() {
        return new RecordMapper<Record, SearchQueryRecordApiDTO>() {
            @Override
            public SearchQueryRecordApiDTO map(@Nonnull final Record record) {
                Long oid = record.get(SearchEntity.SEARCH_ENTITY.OID);
                return SearchQueryRecordApiDTO.entityOrGroupResult(oid)
                        .values(mapValues(record))
                        .build();
            }
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
    Optional<FieldValueApiDTO> mapRecordToValue(@Nonnull Record record, @Nonnull SearchEntityMetadataMapping columnMetadata, @Nonnull FieldApiDTO fieldApiDto) {
        final String columnAlias = getColumnAlias(columnMetadata.getColumnName(), columnMetadata.getJsonKeyName());

        //OID field is added at base-level of response dto, rather than a value in list
        if (fieldApiDto.equals(PrimitiveFieldApiDTO.oid())) {
            return Optional.empty();
        }

        FieldValueApiDTO fieldValue = null;
        final Object value;

        try {
            value = record.get(columnAlias);
        } catch (Exception e) {
            //TODO:  Specify exception,  test processEntities is cause
            //This will only happen during testing. End to end test we may not include all the
            //primary column data.
            logger.info("Record does not contain column " + columnAlias );
            return Optional.empty();
        }


        switch (columnMetadata.getApiDatatype()) {
                case TEXT:
                    fieldValue = fieldApiDto.value((String)value);
                    break;
                case ENUM:
                    if (fieldApiDto.equals(PrimitiveFieldApiDTO.environmentType())) {
                        fieldValue = fieldApiDto.enumValue(
                                readEnumRecordAndMap(record, columnAlias, EnvironmentTypeMapper.fromSearchSchemaToApiFunction));
                    } else if (fieldApiDto.equals(PrimitiveFieldApiDTO.entitySeverity())) {
                        fieldValue = fieldApiDto.enumValue(readEnumRecordAndMap(record,
                                columnAlias,
                                EntitySeverityMapper.fromSearchSchemaToApiFunction));
                    } else if (fieldApiDto.equals(PrimitiveFieldApiDTO.entityState())) {
                        fieldValue = fieldApiDto.enumValue(readEnumRecordAndMap(record,
                                columnAlias,
                                EntityStateMapper.fromSearchSchemaToApiFunction));
                    } else if (fieldApiDto.equals(PrimitiveFieldApiDTO.entityType())) {
                        fieldValue = fieldApiDto.enumValue(readEnumRecordAndMap(record,
                                columnAlias,
                                EntityTypeMapper.fromSearchSchemaToApiFunction));
                    } else {
                        fieldValue = fieldApiDto.value((String)value);
                    }
                    break;
                case NUMBER:
                    fieldValue = fieldApiDto.value(Double.valueOf((String)value));
                    break;
                case INTEGER:
                    if (fieldApiDto.equals(RelatedActionFieldApiDTO.actionCount())) {
                        fieldValue = fieldApiDto.value((Integer)value);
                    } else {
                        fieldValue = fieldApiDto.value(Integer.valueOf((String)value));
                    }

                    break;
                case BOOLEAN:
                    fieldValue = fieldApiDto.value(Boolean.valueOf((String)value));
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

//    Pass the record, get the value, cast it to enum
//    and then do all this

    /**
     * Build list of tableField from {@link EntityQueryApiDTO}.
     *
     * @return List of expected {@link TableField}
     */
    @VisibleForTesting
    Set<Field> buildSelectFields() {
        SelectEntityApiDTO selectEntity = this.entityQueryApiDTO.getSelect();
        Set<Field> tableFields = buildCommonFields();
        tableFields.addAll(buildNonCommonFields(selectEntity));
        return tableFields;
    }

    /**
     * Returns collection of {@link Field}s mapped from {@link PrimitiveFieldApiDTO}.
     *
     * <p>Excludes basic fields i.e, oid, name, type</p>
     *
     * @return collection of {@link Field}s fields
     */
    @VisibleForTesting
    Set<Field> buildNonCommonFields(SelectEntityApiDTO selectEntity) {
        return selectEntity.getFields()
                .stream()
                .filter(entityField -> entityMetadata.containsKey(entityField))
                .map(entityField -> this.buildAndTrackSelectFieldFromEntityType(entityField) )
                .collect(Collectors.toSet());
    }

    /**
     * Returns collection of {@link Field}s to always include in query fetch.
     *
     * <p>We always return these fields regardless of user requesting them or not</p>
     *
     * @return collection of {@link Field}s fields
     */
    private Set<Field> buildCommonFields() {
       return new HashSet<Field>() {{
           add(buildAndTrackSelectFieldFromEntityType(PrimitiveFieldApiDTO.oid()));
           add(buildAndTrackSelectFieldFromEntityType(PrimitiveFieldApiDTO.name()));
           add(buildAndTrackSelectFieldFromEntityType(PrimitiveFieldApiDTO.entityType()));
           add(buildAndTrackSelectFieldFromEntityType(PrimitiveFieldApiDTO.entityState()));
           add(buildAndTrackSelectFieldFromEntityType(PrimitiveFieldApiDTO.environmentType()));
           add(buildAndTrackSelectFieldFromEntityType(PrimitiveFieldApiDTO.entitySeverity()));
        }};
    }

    /**
     * Get {@link Field} configuration for entityField from mappings.
     *
     * @param apiField {@link FieldApiDTO} to parse into select query {@link Field}
     * @return Field configuration based on {@link FieldApiDTO}
     */
    @VisibleForTesting
    Field buildAndTrackSelectFieldFromEntityType(FieldApiDTO apiField) {
        SearchEntityMetadataMapping columnMetadata = this.entityMetadata.get(apiField);
        requestedColumns.put(columnMetadata, apiField);
        return buildFieldForEntityField(apiField, true);
    }

    /**
     * Builds a {@link Field} object from {@link SearchEntityMetadataMapping}.
     *
     * @param apiField {@link FieldApiDTO} to parse into select query {@link Field}
     * @return Field
     */
    @VisibleForTesting
    Field buildFieldForEntityField(@Nonnull FieldApiDTO apiField) {
        return buildFieldForEntityField(apiField, false);
    }

    /**
     * Builds a {@link Field} object from {@link SearchEntityMetadataMapping}.
     *
     * @param apiField {@link FieldApiDTO} to parse into select query {@link Field}
     * @param aliasColumn if field name should contain an alias
     * @return Field
     */
    @VisibleForTesting
    Field buildFieldForEntityField(@Nonnull FieldApiDTO apiField, @Nonnull boolean aliasColumn) {
        final SearchEntityMetadataMapping mapping = this.entityMetadata.get(apiField);
        final String columnName = mapping.getColumnName();
        final String jsonKey = mapping.getJsonKeyName();
        final String columnAlias = getColumnAlias(columnName, jsonKey);
        final Field field;
        if (this.primaryTableColumns.containsKey(apiField)) {
            //For Primary Columns we use the jooq generated Fields
            field = this.primaryTableColumns.get(apiField);
        } else {
            field = Objects.isNull(jsonKey) ? DSL.field(columnName) : DSL.field(buildSqlStringForJsonBColumns(columnName, jsonKey));
        }

        return aliasColumn ? field.as(columnAlias) : field;
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

//    private Field getColumnNameForCommodityFields(CommodityField field) {
//        String columnName = this.searchEntityQueryMetaData.getCommodityFields().get(field).columnName;
//        DataType dataType = this.searchEntityQueryMetaData.getCommodityFields().get(field).type;
//        return createFieldWithColumnNameAndDataType(columnName, dataType);
//    }
//
//    private Field createFieldWithColumnNameAndDataType(String columnName, DataType dataType) {
//        String formatedColumnName = String.format("fields->'%s'", columnName);
//        return DSL.field(formatedColumnName, dataType);
//    }
//
//    private List<Field> getCommodityFields(SelectEntity selectEntity) {
//        return selectEntity.getCommodities()
//                .stream()
//                .map(commodityField -> this.getColumnNameForCommodityFields(commodityField))
//                .collect(Collectors.toList());
//    }

    private List<Condition> buildWhereClauses() {

        return Collections.emptyList();
    }

    private Collection<? extends OrderField<?>> buildOrderByClauses() {
        return Collections.emptyList();
    }

    private List<GroupField> buildGroupByClauses() {
        return Collections.emptyList();
    }

}
