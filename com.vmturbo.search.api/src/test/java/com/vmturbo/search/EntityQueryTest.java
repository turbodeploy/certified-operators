package com.vmturbo.search;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;

import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Record2;
import org.jooq.Record4;
import org.jooq.Result;
import org.jooq.SQLDialect;
import org.jooq.Select;
import org.jooq.SortField;
import org.jooq.impl.DSL;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.http.ResponseEntity;

import com.vmturbo.api.dto.searchquery.CommodityFieldApiDTO;
import com.vmturbo.api.dto.searchquery.EntityQueryApiDTO;
import com.vmturbo.api.dto.searchquery.EnumFieldValueApiDTO;
import com.vmturbo.api.dto.searchquery.FieldApiDTO;
import com.vmturbo.api.dto.searchquery.FieldValueApiDTO;
import com.vmturbo.api.dto.searchquery.FieldValueApiDTO.Type;
import com.vmturbo.api.dto.searchquery.InclusionConditionApiDTO;
import com.vmturbo.api.dto.searchquery.IntegerConditionApiDTO;
import com.vmturbo.api.dto.searchquery.MultiTextFieldValueApiDTO;
import com.vmturbo.api.dto.searchquery.NumberConditionApiDTO;
import com.vmturbo.api.dto.searchquery.NumberFieldValueApiDTO;
import com.vmturbo.api.dto.searchquery.OrderByApiDTO;
import com.vmturbo.api.dto.searchquery.PaginationApiDTO;
import com.vmturbo.api.dto.searchquery.PrimitiveFieldApiDTO;
import com.vmturbo.api.dto.searchquery.RelatedEntityFieldApiDTO;
import com.vmturbo.api.dto.searchquery.SearchQueryRecordApiDTO;
import com.vmturbo.api.dto.searchquery.SelectEntityApiDTO;
import com.vmturbo.api.dto.searchquery.TextConditionApiDTO;
import com.vmturbo.api.dto.searchquery.TextFieldValueApiDTO;
import com.vmturbo.api.dto.searchquery.WhereApiDTO;
import com.vmturbo.api.enums.CommodityType;
import com.vmturbo.api.enums.EntityType;
import com.vmturbo.api.pagination.searchquery.SearchQueryPaginationResponse;
import com.vmturbo.extractor.schema.enums.EntitySeverity;
import com.vmturbo.extractor.schema.tables.SearchEntity;
import com.vmturbo.search.AbstractSearchQuery.SortedOnColumn;
import com.vmturbo.search.mappers.EntityTypeMapper;
import com.vmturbo.search.metadata.SearchEntityMetadata;
import com.vmturbo.search.metadata.SearchMetadataMapping;

/**
 * Tests for EntityQuery.
 */
public class EntityQueryTest {

    private static final SearchMetadataMapping oidPrimitive = SearchMetadataMapping.PRIMITIVE_OID;

    private static final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * A fake database context.
     */
    private DSLContext dSLContextSpy;

    /**
     * Set up for test.
     *
     * @throws Exception thrown if db access fails to succeed.
     */
    @Before
    public void setup() throws Exception {
        this.dSLContextSpy = spy(DSL.using(SQLDialect.POSTGRES));
    }

    private EntityQuery entityQuery(final EntityQueryApiDTO entityQueryApiDTO) {
        return new EntityQuery(entityQueryApiDTO, dSLContextSpy, 100, 100);
    }

    /**
     * Creates {@link EntityQueryApiDTO} configured for requesting data for entityType.
     *
     * @param entityType The requested entityType of interest
     * @return {@link EntityQueryApiDTO} configured to entityType
     */
    public static EntityQueryApiDTO basicRequestForEntityType(EntityType entityType) {
        SelectEntityApiDTO selectEntity =
                SelectEntityApiDTO.selectEntity(EntityType.VIRTUAL_MACHINE).build();
        return EntityQueryApiDTO.queryEntity(selectEntity);
    }

    /**
     * Build basic fields absent of what {@link SelectEntityApiDTO} requests.
     */
    @Test
    public void buildSelectFieldsWithNoExtraFieldsSpecified() {
        //GIVEN
        final EntityQueryApiDTO request = basicRequestForEntityType(EntityType.VIRTUAL_MACHINE);
        EntityQuery query = entityQuery(request);

        Map<FieldApiDTO, SearchMetadataMapping> mappings = SearchEntityMetadata.VIRTUAL_MACHINE.getMetadataMappingMap();
        //WHEN
        Set<String> fields =
                query.buildSelectFields().stream().map(Field::getName).collect(Collectors.toSet());

        //THEN
        assertTrue(fields.contains(mappings.get(PrimitiveFieldApiDTO.oid()).getColumnName()));
        assertTrue(fields.contains(mappings.get(PrimitiveFieldApiDTO.name()).getColumnName()));
        assertTrue(fields.contains(mappings.get(PrimitiveFieldApiDTO.entityType()).getColumnName()));
    }

    /**
     * Expect return empty set when {@link SelectEntityApiDTO#getFields()} empty.
     */
    @Test
    public void getPrimitiveFieldsWithEmptyFields() {
        //GIVEN
        final EntityQueryApiDTO request = basicRequestForEntityType(EntityType.VIRTUAL_MACHINE);
        EntityQuery query = entityQuery(request);

        //WHEN
        Set<Field> fields = query.buildNonCommonFields();

        //THEN
        assertTrue(fields.isEmpty());
    }

    /**
     * Expect return set when {@link SelectEntityApiDTO#getFields()} empty.
     */
    @Test
    public void getFieldsWithPrimitiveTextFields() {
        //GIVEN
        final EntityType type = EntityType.VIRTUAL_MACHINE;
        final FieldApiDTO primitiveEntityField = getAnyEntityKeyField(type, PrimitiveFieldApiDTO.class, null);

        SelectEntityApiDTO selectEntity = SelectEntityApiDTO.selectEntity(EntityType.VIRTUAL_MACHINE)
                .fields(primitiveEntityField, primitiveEntityField).build();


        final EntityQueryApiDTO request = EntityQueryApiDTO.queryEntity(selectEntity);
        EntityQuery query = entityQuery(request);

        //WHEN
        Set<Field> fields = query.buildNonCommonFields();

        //THEN
        Field primiField = query.buildFieldForApiField(primitiveEntityField, true);
        assertTrue("Duplicates should have been filtered out", fields.size() == 1);
        assertTrue("Field created from FieldApiDTO", fields.contains(primiField));
    }

    /**
     * Expect no duplicates in results.
     */
    @Test
    public void getFieldsNoDuplicatesReturned() {
        //GIVEN
        PrimitiveFieldApiDTO severityField = PrimitiveFieldApiDTO.severity();

        SelectEntityApiDTO selectEntity = SelectEntityApiDTO.selectEntity(EntityType.VIRTUAL_MACHINE)
                .fields(severityField, severityField, // Test to make sure duplicate removed
                        severityField // Test to make sure duplicate removed
                )
                .build();

        final EntityQueryApiDTO request = EntityQueryApiDTO.queryEntity(selectEntity);
        EntityQuery query = entityQuery(request);

        //WHEN
        Set<Field> fields = query.buildNonCommonFields();

        //THEN
        assertTrue(fields.size() == 1);
    }

    /**
     * Return {@link EntityType} representative of existing key for SearchEntityMetadata.
     *
     * @param entityType       entityType mappings to target
     * @param expectedKeyClass the key class match wanted
     * @param apiDatatype      {@link Type} match wanted
     * @return the field representative of existing key for SearchEntityMetadata
     */
    private FieldApiDTO getAnyEntityKeyField(@Nonnull EntityType entityType, @Nonnull Class<? extends FieldApiDTO> expectedKeyClass,
            @Nullable Type apiDatatype) {
        return SearchEntityMetadata.valueOf(entityType.name())
                .getMetadataMappingMap()
                .entrySet()
                .stream()
                .filter(entry -> {
                    final FieldApiDTO key = entry.getKey();
                    final SearchMetadataMapping value = entry.getValue();
                    final boolean sameType = Objects.isNull(apiDatatype) ? true :
                            value.getApiDatatype().equals(apiDatatype);
                    final boolean sameKey = !key.equals(this.oidPrimitive) && key.getClass().equals(expectedKeyClass);
                    return sameKey && sameType;
                })
                .findAny()
                .get()
                .getKey();
    }

    /**
     * Expect creates {@link Field} from {@link CommodityFieldApiDTO}.
     */
    @Test
    public void getPrimitiveFieldsWithCommodityFields() {
        //GIVEN
        final EntityType type = EntityType.VIRTUAL_MACHINE;
        final FieldApiDTO commodityField = getAnyEntityKeyField(type, CommodityFieldApiDTO.class, null);
        final SelectEntityApiDTO selectEntity =
                SelectEntityApiDTO.selectEntity(type).fields(commodityField).build();

        final EntityQueryApiDTO request = EntityQueryApiDTO.queryEntity(selectEntity);
        EntityQuery query = entityQuery(request);

        //WHEN
        final Set<Field> fields = query.buildNonCommonFields();

        //THEN
        assertFalse(fields.isEmpty());
        Field comField = query.buildFieldForApiField(commodityField, true);
        assertTrue(fields.contains(comField));
    }

    /**
     * Expect creates {@link Field} from {@link RelatedEntityFieldApiDTO}.
     */
    @Test
    public void getPrimitiveFieldsWithRelatedEntityFields() {
        //GIVEN
        final EntityType type = EntityType.VIRTUAL_MACHINE;
        final FieldApiDTO relatedEntityField = getAnyEntityKeyField(type, RelatedEntityFieldApiDTO.class, null);
        final SelectEntityApiDTO selectEntity =
                SelectEntityApiDTO.selectEntity(type).fields(relatedEntityField).build();

        final EntityQueryApiDTO request = EntityQueryApiDTO.queryEntity(selectEntity);
        EntityQuery query = entityQuery(request);

        //WHEN
        final Set<Field> fields = query.buildNonCommonFields();

        //THEN
        assertFalse(fields.isEmpty());
        Field relField = query.buildFieldForApiField(relatedEntityField, true);
        assertTrue(fields.contains(relField));
    }

    /**
     * Expect result to generate correct response dtos.
     *
     * <p>This is an end to end test of the class.  The query results are mocked and
     * test focus on expected {@link SearchQueryRecordApiDTO}</p>
     *
     * @throws SearchQueryFailedException problems processing request
     */
    @Test
    public void processEntityQuery() throws SearchQueryFailedException {
        //GIVEN
        final EntityType type = EntityType.VIRTUAL_MACHINE;
        final FieldApiDTO primitiveOid = PrimitiveFieldApiDTO.oid();
        final FieldApiDTO primitiveTextField = PrimitiveFieldApiDTO.primitive("guestOsType");
        final FieldApiDTO commodityNumericField = getAnyEntityKeyField(type, CommodityFieldApiDTO.class, Type.NUMBER);
        final FieldApiDTO relatedEntityMultiTextField = getAnyEntityKeyField(type, RelatedEntityFieldApiDTO.class, Type.MULTI_TEXT);

        final SelectEntityApiDTO selectEntity = SelectEntityApiDTO.selectEntity(EntityType.VIRTUAL_MACHINE)
                .fields(primitiveOid, primitiveTextField, commodityNumericField,
                        // Test to make sure duplicate removed
                        relatedEntityMultiTextField)
                .build();
        EntityQueryApiDTO request = EntityQueryApiDTO.queryEntity(selectEntity);
        EntityQuery query = entityQuery(request);

        //Jooq Fields for building results
        final Field oidField = query.buildAndTrackSelectFieldFromEntityType(primitiveOid);
        final Field primitive = query.buildAndTrackSelectFieldFromEntityType(primitiveTextField);
        final Field commodity = query.buildAndTrackSelectFieldFromEntityType(commodityNumericField);
        final Field relateEntity = query.buildAndTrackSelectFieldFromEntityType(relatedEntityMultiTextField);
        //Values for jooq results
        final Long oidValue = 123L;
        final String primitiveTextValue = "primitiveTextValue";
        final String commodityNumericValue = "123.456";
        final String relatedEntityMultiTextValue = "[\"relatedEntityMultiTextValue\"]";

        Result<Record4> result = dSLContextSpy.newResult(oidField, primitive, commodity, relateEntity);
        result.add(dSLContextSpy.newRecord(oidField, primitive, commodity, relateEntity)
                .values(oidValue, primitiveTextValue, commodityNumericValue,
                        relatedEntityMultiTextValue));

        doReturn(result).when(dSLContextSpy).fetch(any(Select.class));
        doReturn(12).when(dSLContextSpy).fetchCount(any(Select.class));

        //WHEN
        SearchQueryPaginationResponse<SearchQueryRecordApiDTO> paginationResponse = query.readQueryAndExecute();
        SearchQueryRecordApiDTO dtoResult = paginationResponse.getRestResponse().getBody().get(0);

        //THEN
        List<FieldValueApiDTO> resultValues = dtoResult.getValues();
        assertTrue(resultValues.size() == 3);
        resultValues.forEach(resultValue -> {
            switch (resultValue.getField().getFieldType()) {
                case PRIMITIVE:
                    assertTrue(((TextFieldValueApiDTO)resultValue).getValue().equals(primitiveTextValue));
                    break;
                case COMMODITY:
                    assertTrue(((NumberFieldValueApiDTO)resultValue).getValue() == (Double.valueOf(
                            commodityNumericValue)));
                    break;
                case RELATED_ENTITY:
                    try {
                        assertTrue(((MultiTextFieldValueApiDTO)resultValue).getValue()
                                .get(0)
                                .equals(objectMapper.readValue(relatedEntityMultiTextValue, String[].class)[0]));
                    } catch (JsonProcessingException e) {
                        Assert.fail("Unable to map multitextField");
                    }
                    break;
                default:
                    Assert.fail("Unexpected Value");
            }
        });
    }

    /**
     * Expect correct translation of {@link WhereApiDTO} clause for entityType.
     */
    @Test
    public void buildWhereClauseEntityType() {
        //GIVEN
        final EntityType type = EntityType.VIRTUAL_MACHINE;
        final FieldApiDTO relatedEntityField = getAnyEntityKeyField(type, RelatedEntityFieldApiDTO.class, null);
        final SelectEntityApiDTO selectEntity =
                SelectEntityApiDTO.selectEntity(type).fields(relatedEntityField).build();

        final EntityQueryApiDTO request = EntityQueryApiDTO.queryEntity(selectEntity);
        EntityQuery query = entityQuery(request);
        //WHEN
        Condition condition = query.buildWhereClauses().get(0);

        //THEN
        String expectedCondition = "\"extractor\".\"search_entity\".\"type\" = 'VIRTUAL_MACHINE'";
        assertTrue(condition.toString().equals(expectedCondition));
    }

    /**
     * Expect correct translation of {@link WhereApiDTO} clause for {@link TextConditionApiDTO} a
     * primary column.
     *
     * <p>TextCondition against Enum uses an in operator in sql.</p>
     */
    @Test
    public void buildWhereClauseTextConditionEnum() {
        //GIVEN
        TextConditionApiDTO enumCondition =
                PrimitiveFieldApiDTO.severity().like(EntitySeverity.CRITICAL.getLiteral());

        final EntityType type = EntityType.VIRTUAL_MACHINE;
        final FieldApiDTO relatedEntityField = getAnyEntityKeyField(type, RelatedEntityFieldApiDTO.class, null);
        final WhereApiDTO where = WhereApiDTO.where().and(enumCondition).build();
        final SelectEntityApiDTO selectEntity =
                SelectEntityApiDTO.selectEntity(type).fields(relatedEntityField).build();

        EntityQueryApiDTO request = EntityQueryApiDTO.queryEntity(selectEntity, where);
        EntityQuery query = entityQuery(request);

        //WHEN
        List<Condition> conditions = query.buildWhereClauses();

        //THEN
        assertTrue(conditions.size() == 2);
        // Case-insensitive regex search
        String expectedCondition = "\"extractor\".\"search_entity\".\"severity\" = 'CRITICAL'";
        assertTrue(containsCondition(conditions, expectedCondition));
    }

    /**
     * Expect correct translation of {@link WhereApiDTO} clause for {@link TextConditionApiDTO} of
     * non enum value.
     */
    @Test
    public void buildWhereClauseTextConditionNonEnum() {
        //GIVEN
        TextConditionApiDTO enumCondition =
                PrimitiveFieldApiDTO.primitive("guestOsType").like("foobar");

        final EntityType type = EntityType.VIRTUAL_MACHINE;
        final FieldApiDTO relatedEntityField = getAnyEntityKeyField(type, RelatedEntityFieldApiDTO.class, null);
        final WhereApiDTO where = WhereApiDTO.where().and(enumCondition).build();
        final SelectEntityApiDTO selectEntity =
                SelectEntityApiDTO.selectEntity(type).fields(relatedEntityField).build();

        EntityQueryApiDTO request = EntityQueryApiDTO.queryEntity(selectEntity, where);
        EntityQuery query = entityQuery(request);

        //WHEN
        List<Condition> conditions = query.buildWhereClauses();

        //THEN
        assertTrue(conditions.size() == 2);
        // Case-insensitive regex search
        String expectedCondition = "(cast(attrs->>'guest_os_type' as longnvarchar) like_regex '(?i)foobar')";
        assertTrue(containsCondition(conditions, expectedCondition));
    }

    /**
     * Expect correct translation of {@link WhereApiDTO} clause for {@link InclusionConditionApiDTO}
     * of enum value.
     */
    @Test
    public void buildWhereClauseInclusionCondition() {
        //GIVEN
        String[] states = {"ACTIVE", "IDLE"};
        InclusionConditionApiDTO enumCondition = PrimitiveFieldApiDTO.entityState().in(states);

        final EntityType type = EntityType.VIRTUAL_MACHINE;
        final WhereApiDTO where = WhereApiDTO.where().and(enumCondition).build();
        final SelectEntityApiDTO selectEntity = SelectEntityApiDTO.selectEntity(type)
                .fields(PrimitiveFieldApiDTO.entityState())
                .build();

        EntityQueryApiDTO request = EntityQueryApiDTO.queryEntity(selectEntity, where);
        EntityQuery query = entityQuery(request);

        //WHEN
        List<Condition> conditions = query.buildWhereClauses();

        //THEN
        assertTrue(conditions.size() == 2);

        String expectedCondition = "\"extractor\".\"search_entity\".\"state\" in (\n  " + "'POWERED_ON', 'POWERED_OFF'\n)";
        assertTrue(containsCondition(conditions, expectedCondition));
    }

    /**
     * Expect correct translation of {@link WhereApiDTO} clause for {@link NumberConditionApiDTO}.
     */
    @Test
    public void buildWhereClauseNumberCondition() {
        //GIVEN
        final EntityType type = EntityType.VIRTUAL_MACHINE;
        final FieldApiDTO commodityField = CommodityFieldApiDTO.used(CommodityType.VCPU);
        Double doubleValue = 98.89;
        NumberConditionApiDTO numberConditionApiDTO = commodityField.eq(doubleValue);
        final WhereApiDTO where = WhereApiDTO.where().and(numberConditionApiDTO).build();
        final SelectEntityApiDTO selectEntity =
                SelectEntityApiDTO.selectEntity(type).fields(commodityField).build();

        EntityQueryApiDTO request = EntityQueryApiDTO.queryEntity(selectEntity, where);
        EntityQuery query = entityQuery(request);

        //WHEN
        List<Condition> conditions = query.buildWhereClauses();

        //THEN
        assertTrue(conditions.size() == 2);
        String expectedCondition = "cast(attrs->>'vcpu_used' as double) = 98.89";
        assertTrue(containsCondition(conditions, expectedCondition));
    }

    /**
     * Expect correct translation of {@link WhereApiDTO} clause for {@link IntegerConditionApiDTO}.
     */
    @Test
    public void buildWhereClauseIntegerCondition() {
        //GIVEN
        final EntityType type = EntityType.PHYSICAL_MACHINE;
        final FieldApiDTO commodityField = RelatedEntityFieldApiDTO.entityCount(EntityType.VIRTUAL_MACHINE);
        Long longValue = 98L;
        IntegerConditionApiDTO integerConditionApiDTO = commodityField.eq(longValue);
        final WhereApiDTO where = WhereApiDTO.where().and(integerConditionApiDTO).build();
        final SelectEntityApiDTO selectEntity =
                SelectEntityApiDTO.selectEntity(type).fields(commodityField).build();

        EntityQueryApiDTO request = EntityQueryApiDTO.queryEntity(selectEntity, where);
        EntityQuery query = entityQuery(request);

        //WHEN
        List<Condition> conditions = query.buildWhereClauses();

        //THEN
        assertTrue(conditions.size() == 2);

        String expectedCondition = "cast(attrs->>'num_vms' as bigint) = 98";
        assertTrue(containsCondition(conditions, expectedCondition));
    }

    private boolean containsCondition(final List<Condition> conditions, final String expectedCondition) {
        return conditions.stream().map(Object::toString).anyMatch(s -> expectedCondition.equals(s));
    }

    private boolean containsSort(final LinkedHashSet<SortField<?>> sortFields, final String expectedSort) {
        return sortFields.stream().map(Object::toString).anyMatch(s -> expectedSort.equals(s));
    }

    private boolean containsSelectField(final List<Field> sortFields, final String expectedSelectField) {
        return sortFields.stream().map(Object::toString).anyMatch(s -> expectedSelectField.equals(s));
    }

    /**
     * Expect units to be returned for {@link CommodityFieldApiDTO}.
     */
    @Test
    public void mapRecordToValueReturningUnits() {
        //GIVEN
        SearchMetadataMapping columnMetadata = SearchMetadataMapping.COMMODITY_CPU_USED;
        FieldApiDTO fieldApiDto = CommodityFieldApiDTO.used(CommodityType.CPU);

        final EntityQueryApiDTO request = basicRequestForEntityType(EntityType.PHYSICAL_MACHINE);
        EntityQuery querySpy = spy(entityQuery(request));
        querySpy.metadataMapping = mock(Map.class);

        doReturn(columnMetadata).when(querySpy.metadataMapping).get(any());
        final Field commodityField = querySpy.buildAndTrackSelectFieldFromEntityType(fieldApiDto);
        Record record = dSLContextSpy.newRecord(commodityField).values("45");

        //WHEN
        NumberFieldValueApiDTO value =
                (NumberFieldValueApiDTO)querySpy.mapRecordToValue(record, columnMetadata,
                        fieldApiDto).get();

        //THEN
        assertNotNull(value.getUnits());
        assertTrue(value.getUnits().equals(columnMetadata.getUnitsString()));
    }

    /**
     * Expect error to be thrown on non valid field in select statement on metadata
     */
    @Test(expected = IllegalArgumentException.class)
    public void testErrorThrownOnInvalidSelectFieldDtoRequest() {
        //GIVEN
        final EntityType type = EntityType.VIRTUAL_MACHINE;
        final FieldApiDTO nonVmField = PrimitiveFieldApiDTO.primitive("Cant touch this");
        final SelectEntityApiDTO selectEntity =
                SelectEntityApiDTO.selectEntity(type).fields(nonVmField).build();
        final EntityQueryApiDTO request = EntityQueryApiDTO.queryEntity(selectEntity);
        EntityQuery query = entityQuery(request);

        //WHEN
        query.buildSelectFields();
    }

    /**
     * Expect error to be thrown on non valid condition.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testErrorThrownOnInvalidWhereFieldDtoRequest() {
        //GIVEN
        final EntityType type = EntityType.VIRTUAL_MACHINE;
        final TextConditionApiDTO invalidTextCondition =
                PrimitiveFieldApiDTO.primitive("Cant touch this").like("foo");
        final WhereApiDTO whereEntity = WhereApiDTO.where().and(invalidTextCondition).build();
        final EntityQueryApiDTO request =
                EntityQueryApiDTO.queryEntity(SelectEntityApiDTO.selectEntity(type).build(),
                        whereEntity);
        EntityQuery query = entityQuery(request);

        //WHEN
        query.buildWhereClauses();
    }

    //TODO:  Add test for thrown error on order by a field unsupported for metadata configured

    /**
     * Expect correct mapping of {@link com.vmturbo.extractor.schema.enums.EntityType} to {@link EntityType}
     */
    @Test
    public void mapRecordToValueReturnsEntityTypeApiEnum() {
        //GIVEN
        final EntityQueryApiDTO request = basicRequestForEntityType(EntityType.VIRTUAL_MACHINE);
        EntityQuery query = entityQuery(request);

        com.vmturbo.extractor.schema.enums.EntityType recordValue = com.vmturbo.extractor.schema.enums.EntityType.VIRTUAL_MACHINE;
        Record record = dSLContextSpy.newRecord(SearchEntity.SEARCH_ENTITY.TYPE).values(recordValue);
        PrimitiveFieldApiDTO entityTypeFieldDto = PrimitiveFieldApiDTO.entityType();
        //WHEN
        EnumFieldValueApiDTO
                value = (EnumFieldValueApiDTO) query.mapRecordToValue(record, SearchMetadataMapping.PRIMITIVE_ENTITY_TYPE, entityTypeFieldDto).get();

        //THEN
        assertTrue(value.getValue().equals(EntityTypeMapper.fromSearchSchemaToApi(recordValue).toString()));
    }

    /**
     * Tests defaultSorting on name applied when no sort order given/
     */
    @Test
    public void defaultSortApplied() {
        //GIVEN
        SelectEntityApiDTO selectEntity =
                SelectEntityApiDTO.selectEntity(EntityType.PHYSICAL_MACHINE).build();
        final EntityQueryApiDTO request = EntityQueryApiDTO.queryEntity(selectEntity);
        EntityQuery query = entityQuery(request);

        //WHEN
        LinkedHashSet<SortField<?>> sortFields = query.buildOrderByFields();

        //THEN
        assertTrue(sortFields.size() == 2);
        final String nameSort = "\"extractor\".\"search_entity\".\"name\" asc";
        final String oidSort = "\"extractor\".\"search_entity\".\"oid\" asc";
        assertTrue(containsSort(sortFields, nameSort));
        assertTrue(containsSort(sortFields, oidSort));


    }

    /**
     * Creates and applies sortBy Fields with proper dataType casting.
     */
    @Test
    public void sortFieldsAppliedAndCast() {
        //GIVEN
        SelectEntityApiDTO selectEntity =
                SelectEntityApiDTO.selectEntity(EntityType.PHYSICAL_MACHINE).build();

        FieldApiDTO integerFieldApiDTO = RelatedEntityFieldApiDTO.entityCount(EntityType.VIRTUAL_MACHINE);
        FieldApiDTO doubleFieldApiDTO = CommodityFieldApiDTO.utilization(CommodityType.MEM);
        OrderByApiDTO orderByIntegerField = OrderByApiDTO.asc(integerFieldApiDTO);
        OrderByApiDTO orderByDoubleField = OrderByApiDTO.desc(doubleFieldApiDTO);

        PaginationApiDTO pagination = PaginationApiDTO.orderBy(orderByIntegerField, orderByDoubleField).build();
        final EntityQueryApiDTO request = EntityQueryApiDTO.queryEntity(selectEntity, pagination);

        EntityQuery query = entityQuery(request);

        //WHEN
        LinkedHashSet<SortField<?>> sortFields = query.buildOrderByFields();

        //THEN
        assertTrue(sortFields.size() == 3);
        final String integerSort = "cast(attrs->>'num_vms' as bigint) asc";
        final String doubleSort = "cast(attrs->>'mem_utilization' as double) desc";
        final String defaultOidField = "\"extractor\".\"search_entity\".\"oid\" asc";
        assertTrue(containsSort(sortFields, integerSort));
        assertTrue(containsSort(sortFields, doubleSort));
        assertTrue(containsSort(sortFields, defaultOidField));
    }

    /**
     * Tests building of select statements from orderByDtos.
     */
    @Test
    public void buildSelectAddsOrderByFields() {
        //GIVEN
        SelectEntityApiDTO selectEntity =
                SelectEntityApiDTO.selectEntity(EntityType.PHYSICAL_MACHINE).build();

        FieldApiDTO integerFieldApiDTO = RelatedEntityFieldApiDTO.entityCount(EntityType.VIRTUAL_MACHINE);
        FieldApiDTO doubleFieldApiDTO = CommodityFieldApiDTO.utilization(CommodityType.MEM);
        OrderByApiDTO orderByIntegerField = OrderByApiDTO.asc(integerFieldApiDTO);
        OrderByApiDTO orderByDoubleField = OrderByApiDTO.desc(doubleFieldApiDTO);

        PaginationApiDTO pagination = PaginationApiDTO.orderBy(orderByIntegerField, orderByDoubleField).build();
        EntityQueryApiDTO request = EntityQueryApiDTO.queryEntity(selectEntity, pagination);

        EntityQuery query = entityQuery(request);

        //WHEN
        Set<String> fields = query.buildOrderByFields().stream().map(SortField::toString).collect(Collectors.toSet());

        //THEN
        assertNotNull(fields);
        assertTrue(fields.contains("cast(attrs->>'num_vms' as bigint) asc"));
        assertTrue(fields.contains("cast(attrs->>'mem_utilization' as double) desc"));
        assertTrue(fields.contains("\"extractor\".\"search_entity\".\"oid\" asc"));

        Set<String> sortTrackers = query.sortedOnColumns.stream().map(SortedOnColumn::getField).map(Field::toString).collect(Collectors.toSet());
        assertTrue(sortTrackers.contains("cast(attrs->>'num_vms' as bigint)"));
        assertTrue(sortTrackers.contains("cast(attrs->>'mem_utilization' as double)"));
        assertTrue(sortTrackers.contains("\"extractor\".\"search_entity\".\"oid\""));
    }

    /**
     * Expect default fields name and oid to be added when not orderByDtos provided.
     */
    @Test
    public void buildSelectAddsDefaultOrderByFields() {
        //GIVEN
        final EntityQueryApiDTO request = basicRequestForEntityType(EntityType.VIRTUAL_MACHINE);
        EntityQuery query = entityQuery(request);

        //WHEN
        Set<String> fields = query.buildOrderByFields().stream().map(SortField::toString).collect(Collectors.toSet());

        //THEN
        assertNotNull(fields);
        //Added by default
        assertTrue(fields.contains("\"extractor\".\"search_entity\".\"name\" asc"));
        //Added by default
        assertTrue(fields.contains("\"extractor\".\"search_entity\".\"oid\" asc"));

        Set<String> sortTrackers = query.sortedOnColumns.stream().map(SortedOnColumn::getField).map(Field::toString).collect(Collectors.toSet());
        assertTrue("Default sortBy oid should have been added", sortTrackers.contains("\"extractor\".\"search_entity\".\"name\""));
        assertTrue("Default sortBy name should have been added", sortTrackers.contains("\"extractor\".\"search_entity\".\"oid\""));
    }


    /**
     * Expect pagination results to include nextCursor and totalRecordCount
     *
     * <p>In this specific case
     * 1. The default sortBy name, oid should be applied in that order,
     *    cursor cause will fail if not the case
     * 2. The number of records exceeds the limit requested.
     *    Last Record should be used to create the next-cursor</p>
     *
     * @throws SearchQueryFailedException problems processing request
     */
    @Test
    public void testPaginationCursorResults() throws SearchQueryFailedException {
        //GIVEN
        final EntityType type = EntityType.VIRTUAL_MACHINE;
        final FieldApiDTO primitiveOid = PrimitiveFieldApiDTO.oid();
        final FieldApiDTO primitiveName = PrimitiveFieldApiDTO.name();
        final SelectEntityApiDTO selectEntity = SelectEntityApiDTO.selectEntity(EntityType.VIRTUAL_MACHINE)
                .fields(primitiveOid, primitiveName)
                .build();

        PaginationApiDTO pagination = PaginationApiDTO.orderBy().limit(1).build();

        EntityQueryApiDTO request = EntityQueryApiDTO.queryEntity(selectEntity, pagination);
        EntityQuery query = entityQuery(request);

        //Jooq Fields for building results
        final Field oidField = query.buildAndTrackSelectFieldFromEntityType(primitiveOid);
        final Field nameField = query.buildAndTrackSelectFieldFromEntityType(primitiveName);
        //Values for jooq results
        final Long oidValue = 123L;
        final String nameValue = "walter";

        Result<Record2> result = dSLContextSpy.newResult(oidField, nameField);
        //This results record is the one to be used for creating cursor.
        result.add(dSLContextSpy.newRecord(oidField, nameField).values(oidValue, nameValue));
        result.add(dSLContextSpy.newRecord(oidField, nameField).values(5L, "potatoes"));

        doReturn(result).when(dSLContextSpy).fetch(any(Select.class));
        final int totalRecordCount = 12;
        doReturn(totalRecordCount).when(dSLContextSpy).fetchCount(any(Select.class));

        //WHEN
        SearchQueryPaginationResponse<SearchQueryRecordApiDTO> paginationResponse = query.readQueryAndExecute();
        ResponseEntity<List<SearchQueryRecordApiDTO>>  responseEntity = paginationResponse.getRestResponse();

        //THEN
        assertTrue(responseEntity.getBody().size() == 1);
        assertTrue(responseEntity.getHeaders().get("X-Next-Cursor").get(0).equals(SearchPaginationUtil.constructNextCursor(
                Lists.newArrayList(nameValue, String.valueOf(oidValue)))));
        assertTrue(responseEntity.getHeaders().get("X-Total-Record-Count").get(0).equals(String.valueOf(totalRecordCount)));
    }

    /**
     * Expect pagination results to include nextCursor and totalRecordCount
     *
     * <p>In this specific case
     * 1. sortBy and cursor will come from user configured {@link OrderByApiDTO}
     * 2. The number of records exceeds the limit requested.
     *    Last Record should be used to create the next-cursor</p>
     *
     * @throws SearchQueryFailedException problems processing request
     */
    @Test
    public void testPaginationCursorGeneratedFromOrderByFields() throws SearchQueryFailedException {
        //GIVEN
        final FieldApiDTO primitiveOid = PrimitiveFieldApiDTO.oid();
        final FieldApiDTO primitiveName = PrimitiveFieldApiDTO.name();
        final FieldApiDTO commodityDoubleField = CommodityFieldApiDTO.capacity(CommodityType.VMEM);
        final FieldApiDTO relatedEntityFieldApiDTOMultiText = RelatedEntityFieldApiDTO.entityNames(EntityType.DISKARRAY);

        OrderByApiDTO orderByRelatedEntity = OrderByApiDTO.asc(relatedEntityFieldApiDTOMultiText);
        OrderByApiDTO orderByCommodity = OrderByApiDTO.desc(commodityDoubleField);
        OrderByApiDTO orderByPrimitiveName = OrderByApiDTO.desc(primitiveName);

        final SelectEntityApiDTO selectEntity = SelectEntityApiDTO.selectEntity(EntityType.VIRTUAL_MACHINE)
                .fields(primitiveOid, primitiveName)
                .build();

        PaginationApiDTO pagination = PaginationApiDTO.orderBy(orderByRelatedEntity,
                orderByCommodity, orderByPrimitiveName)
                .limit(1).build();

        EntityQueryApiDTO request = EntityQueryApiDTO.queryEntity(selectEntity, pagination);
        EntityQuery query = entityQuery(request);

        //Jooq Fields for building results
        final Field oidField = query.buildFieldForApiField(primitiveOid, true);
        final Field nameField = query.buildFieldForApiField(primitiveName, true);
        final Field commodityField = query.buildFieldForApiField(commodityDoubleField, true);
        final Field relatedEntityField = query.buildFieldForApiField(relatedEntityFieldApiDTOMultiText, true);
        //Values for jooq results
        final Long oidValue = 123L;
        final String nameValue = "walter";
        final double commodityValue = 34.555;
        final String relatedEntityValue = "[\"vsphere-dc20-DC01\"]";

        Result<Record4> result = dSLContextSpy.newResult(oidField, nameField, commodityField, relatedEntityField);
        //This results record is the one to be used for creating cursor.
        result.add(dSLContextSpy.newRecord(oidField, nameField, commodityField, relatedEntityField).values(oidValue, nameValue, commodityValue, relatedEntityValue));
        result.add(dSLContextSpy.newRecord(oidField, nameField, commodityField, relatedEntityField).values(5L, "potatoes", 54.3, ""));

        doReturn(result).when(dSLContextSpy).fetch(any(Select.class));
        final int totalRecordCount = 12;
        doReturn(totalRecordCount).when(dSLContextSpy).fetchCount(any(Select.class));

        //WHEN
        SearchQueryPaginationResponse<SearchQueryRecordApiDTO> paginationResponse = query.readQueryAndExecute();
        ResponseEntity<List<SearchQueryRecordApiDTO>>  responseEntity = paginationResponse.getRestResponse();

        //THEN
        String expectedCursor = SearchPaginationUtil.constructNextCursor(
                Lists.newArrayList(relatedEntityValue, String.valueOf(commodityValue), nameValue, String.valueOf(oidValue)));
        assertTrue(responseEntity.getBody().size() == 1);
        assertTrue(responseEntity.getHeaders().get("X-Next-Cursor").get(0).equals(expectedCursor));
        assertTrue(responseEntity.getHeaders().get("X-Total-Record-Count").get(0).equals(String.valueOf(totalRecordCount)));
    }

    /**
     * Expect pagination results to include nextCursor and totalRecordCount
     *
     * <p>In this specific case the number of records is less than limit.  Next-cursor should be
     * null</p>
     *
     * @throws SearchQueryFailedException problems processing request
     */
    @Test
    public void testPaginationNullCursor() throws SearchQueryFailedException {
        //GIVEN
        final EntityType type = EntityType.VIRTUAL_MACHINE;
        final FieldApiDTO primitiveOid = PrimitiveFieldApiDTO.oid();
        final FieldApiDTO primitiveName = PrimitiveFieldApiDTO.name();

        final SelectEntityApiDTO selectEntity = SelectEntityApiDTO.selectEntity(EntityType.VIRTUAL_MACHINE)
                .fields(primitiveOid, primitiveName)
                .build();

        PaginationApiDTO pagination = PaginationApiDTO.orderBy().limit(3).build();

        EntityQueryApiDTO request = EntityQueryApiDTO.queryEntity(selectEntity, pagination);
        EntityQuery query = entityQuery(request);

        //Jooq Fields for building results
        final Field oidField = query.buildAndTrackSelectFieldFromEntityType(primitiveOid);
        final Field nameField = query.buildAndTrackSelectFieldFromEntityType(primitiveName);
        //Values for jooq results

        Result<Record2> result = dSLContextSpy.newResult(oidField, nameField);
        result.add(dSLContextSpy.newRecord(oidField, nameField).values(5L, "potatoes"));

        doReturn(result).when(dSLContextSpy).fetch(any(Select.class));
        final int totalRecordCount = 12;
        doReturn(totalRecordCount).when(dSLContextSpy).fetchCount(any(Select.class));

        //WHEN
        SearchQueryPaginationResponse<SearchQueryRecordApiDTO> paginationResponse = query.readQueryAndExecute();
        ResponseEntity<List<SearchQueryRecordApiDTO>> responseEntity = paginationResponse.getRestResponse();

        //THEN
        assertTrue(responseEntity.getBody().size() == 1);
        assertNull(responseEntity.getHeaders().get("X-Next-Cursor").get(0));
        assertTrue(responseEntity.getHeaders().get("X-Total-Record-Count").get(0).equals(String.valueOf(totalRecordCount)));
    }

    /**
     * Expected query to contain seek generated where conditions from nextCursor and orderbys.
     * @throws SearchQueryFailedException problems processing request
     */
    @Test
    public void testSeekBuildWithNextCursor() throws SearchQueryFailedException {
        //GIVEN
        final Long oidValue = 123L;
        final String nameValue = "walter";
        final double commodityValue = 34.555;
        final String relatedEntityValue = "[\"vsphere-dc20-DC01\"]";
        String expectedCursor = SearchPaginationUtil.constructNextCursor(
                Lists.newArrayList(relatedEntityValue, String.valueOf(commodityValue), nameValue, String.valueOf(oidValue)));

        final FieldApiDTO primitiveOid = PrimitiveFieldApiDTO.oid();
        final FieldApiDTO primitiveName = PrimitiveFieldApiDTO.name();
        final FieldApiDTO commodityDoubleField = CommodityFieldApiDTO.capacity(CommodityType.VMEM);
        final FieldApiDTO relatedEntityFieldApiDTOMultiText = RelatedEntityFieldApiDTO.entityNames(EntityType.DISKARRAY);

        OrderByApiDTO orderByRelatedEntity = OrderByApiDTO.asc(relatedEntityFieldApiDTOMultiText);
        OrderByApiDTO orderByCommodity = OrderByApiDTO.desc(commodityDoubleField);
        OrderByApiDTO orderByPrimitiveName = OrderByApiDTO.desc(primitiveName);

        final SelectEntityApiDTO selectEntity = SelectEntityApiDTO.selectEntity(EntityType.VIRTUAL_MACHINE)
                .fields(primitiveOid, primitiveName)
                .build();

        PaginationApiDTO pagination = PaginationApiDTO.orderBy(orderByRelatedEntity,
                orderByCommodity, orderByPrimitiveName)
                .cursor(expectedCursor)
                .limit(1).build();

        EntityQueryApiDTO request = EntityQueryApiDTO.queryEntity(selectEntity, pagination);
        EntityQuery querySpy = spy(entityQuery(request));

        doReturn(mock(Result.class)).when(dSLContextSpy).fetch(any(Select.class));
        doReturn(4).when(dSLContextSpy).fetchCount(any(Select.class));
        doReturn(null).when(querySpy).paginateNextCursorResponse(any(), any());

        //WHEN
        Select<Record> paginatedQuery = querySpy.buildCompleteQuery();

        //THEN
        String expectedQuery = "select \n"
                + "  \"extractor\".\"search_entity\".\"oid\" as \"oid\", \n"
                + "  \"extractor\".\"search_entity\".\"name\" as \"name\", \n"
                + "  cast(attrs->>'related_diskarray' as varchar) as \"related_diskarray\", \n"
                + "  cast(attrs->>'vmem_capacity' as double precision) as \"vmem_capacity\", \n"
                + "  \"extractor\".\"search_entity\".\"type\" as \"type\"\n"
                + "from \"extractor\".\"search_entity\"\n" + "where (\n"
                + "  \"extractor\".\"search_entity\".\"type\" = 'VIRTUAL_MACHINE'\n" + "  and (\n"
                + "    cast(attrs->>'related_diskarray' as varchar) > '[\"vsphere-dc20-DC01\"]'\n"
                + "    or (\n"
                + "      cast(attrs->>'related_diskarray' as varchar) = '[\"vsphere-dc20-DC01\"]'\n"
                + "      and cast(attrs->>'vmem_capacity' as double precision) < 34.555\n"
                + "    )\n" + "    or (\n"
                + "      cast(attrs->>'related_diskarray' as varchar) = '[\"vsphere-dc20-DC01\"]'\n"
                + "      and cast(attrs->>'vmem_capacity' as double precision) = 34.555\n"
                + "      and \"extractor\".\"search_entity\".\"name\" < 'walter'\n" + "    )\n"
                + "    or (\n"
                + "      cast(attrs->>'related_diskarray' as varchar) = '[\"vsphere-dc20-DC01\"]'\n"
                + "      and cast(attrs->>'vmem_capacity' as double precision) = 34.555\n"
                + "      and \"extractor\".\"search_entity\".\"name\" = 'walter'\n"
                + "      and \"extractor\".\"search_entity\".\"oid\" > 123\n" + "    )\n" + "  )\n"
                + ")\n" + "order by \n" + "  cast(attrs->>'related_diskarray' as varchar) asc, \n"
                + "  cast(attrs->>'vmem_capacity' as double precision) desc, \n"
                + "  \"extractor\".\"search_entity\".\"name\" desc, \n"
                + "  \"extractor\".\"search_entity\".\"oid\" asc\n" + "limit 2";
        assertTrue(paginatedQuery.toString().equals(expectedQuery));
    }

    /**
     * Expect query constructed from previousCursor to have reverse orderBys and where clauses.
     * @throws SearchQueryFailedException problems processing request
     */
    @Test
    public void testQueryWithPreviousCursor() throws SearchQueryFailedException {
        //GIVEN
        final Long oidValue = 123L;
        final double commodityValue = 34.555;
        String previousCursor = SearchPaginationUtil.constructPreviousCursor(
                Lists.newArrayList(String.valueOf(commodityValue), String.valueOf(oidValue)));

        final FieldApiDTO primitiveOid = PrimitiveFieldApiDTO.oid();
        final FieldApiDTO commodityDoubleField = CommodityFieldApiDTO.capacity(CommodityType.VMEM);

        OrderByApiDTO orderByCommodity = OrderByApiDTO.asc(commodityDoubleField);

        final SelectEntityApiDTO selectEntity = SelectEntityApiDTO.selectEntity(EntityType.VIRTUAL_MACHINE)
                .fields(primitiveOid, commodityDoubleField)
                .build();

        PaginationApiDTO pagination = PaginationApiDTO.orderBy(orderByCommodity)
                .cursor(previousCursor)
                .limit(1).build();

        EntityQueryApiDTO request = EntityQueryApiDTO.queryEntity(selectEntity, pagination);
        EntityQuery querySpy = spy(entityQuery(request));

        doReturn(mock(Result.class)).when(dSLContextSpy).fetch(any(Select.class));
        doReturn(4).when(dSLContextSpy).fetchCount(any(Select.class));
        doReturn(null).when(querySpy).paginatePreviousResponse(any(), any());

        //WHEN
        Select<Record> paginatedQuery = querySpy.buildCompleteQuery();

        //THEN
        String expectedQuery = "select \n"
                + "  \"extractor\".\"search_entity\".\"oid\" as \"oid\", \n"
                + "  \"extractor\".\"search_entity\".\"name\" as \"name\", \n"
                + "  cast(attrs->>'vmem_capacity' as double precision) as \"vmem_capacity\", \n"
                + "  \"extractor\".\"search_entity\".\"type\" as \"type\"\n"
                + "from \"extractor\".\"search_entity\"\n" + "where (\n"
                + "  \"extractor\".\"search_entity\".\"type\" = 'VIRTUAL_MACHINE'\n"
                + "  and (cast(attrs->>'vmem_capacity' as double precision), \"extractor\".\"search_entity\".\"oid\") < (34.555, 123)\n"
                + ")\n" + "order by \n"
                + "  cast(attrs->>'vmem_capacity' as double precision) desc, \n"
                + "  \"extractor\".\"search_entity\".\"oid\" desc\n" + "limit 2";
        assertTrue(paginatedQuery.toString().equals(expectedQuery));
    }

    /**
     * Request for next page from nextCursor returns nextCursor, previousCursor, totalRecordCount.
     * @throws SearchQueryFailedException problems processing request
     */
    @Test
    public void testNextPage() throws SearchQueryFailedException {
        //GIVEN
        final EntityType type = EntityType.VIRTUAL_MACHINE;
        final FieldApiDTO primitiveOid = PrimitiveFieldApiDTO.oid();
        final FieldApiDTO primitiveName = PrimitiveFieldApiDTO.name();

        final SelectEntityApiDTO selectEntity = SelectEntityApiDTO.selectEntity(EntityType.VIRTUAL_MACHINE)
                .fields(primitiveOid, primitiveName)
                .build();
        final Long oidValue = 123L;
        final double commodityValue = 34.555;

        String expectedCursor = SearchPaginationUtil.constructNextCursor(
                Lists.newArrayList(String.valueOf(commodityValue), String.valueOf(oidValue)));

        PaginationApiDTO pagination = PaginationApiDTO.orderBy().cursor(expectedCursor).limit(2).build();

        EntityQueryApiDTO request = EntityQueryApiDTO.queryEntity(selectEntity, pagination);
        EntityQuery query = entityQuery(request);

        //Jooq Fields for building results
        final Field oidField = query.buildAndTrackSelectFieldFromEntityType(primitiveOid);
        final Field nameField = query.buildAndTrackSelectFieldFromEntityType(primitiveName);
        //Values for jooq results
        final Long resultOidValue = 5L;
        final double resultCommodityValue = 99.99;

        Result<Record2> result = dSLContextSpy.newResult(oidField, nameField);
        result.add(dSLContextSpy.newRecord(oidField, nameField).values(resultOidValue, resultCommodityValue));
        result.add(dSLContextSpy.newRecord(oidField, nameField).values(resultOidValue, resultCommodityValue));
        result.add(dSLContextSpy.newRecord(oidField, nameField).values(resultOidValue, resultCommodityValue));

        doReturn(result).when(dSLContextSpy).fetch(any(Select.class));
        final int totalRecordCount = 12;
        doReturn(totalRecordCount).when(dSLContextSpy).fetchCount(any(Select.class));

        //WHEN
        SearchQueryPaginationResponse<SearchQueryRecordApiDTO> paginationResponse = query.readQueryAndExecute();
        ResponseEntity<List<SearchQueryRecordApiDTO>> responseEntity = paginationResponse.getRestResponse();

        //THEN
        assertTrue(responseEntity.getBody().size() == 2);
        assertNotNull(responseEntity.getHeaders().get("X-Next-Cursor").get(0));
        assertNotNull(responseEntity.getHeaders().get("X-Previous-Cursor").get(0));

        String resultNextCursor = SearchPaginationUtil.constructNextCursor(
                Lists.newArrayList(String.valueOf(resultCommodityValue), String.valueOf(resultOidValue)));
        String resultPreviousCursor = SearchPaginationUtil.constructPreviousCursor(
                Lists.newArrayList(String.valueOf(resultCommodityValue), String.valueOf(resultOidValue)));
        assertTrue(responseEntity.getHeaders().get("X-Previous-Cursor").get(0).equals(resultPreviousCursor));
        assertTrue(responseEntity.getHeaders().get("X-Next-Cursor").get(0).equals(resultNextCursor));

        assertTrue(responseEntity.getHeaders().get("X-Total-Record-Count").get(0).equals(String.valueOf(totalRecordCount)));
    }

    /**
     * Request for previous page from nextCursor returns nextCursor, previousCursor, totalRecordCount.
     * @throws SearchQueryFailedException problems processing request
     */
    @Test
    public void testPreviousPage3To2() throws SearchQueryFailedException {
        //GIVEN
        final FieldApiDTO primitiveOid = PrimitiveFieldApiDTO.oid();
        final FieldApiDTO primitiveName = PrimitiveFieldApiDTO.name();

        final SelectEntityApiDTO selectEntity = SelectEntityApiDTO.selectEntity(EntityType.VIRTUAL_MACHINE)
                .fields(primitiveOid, primitiveName)
                .build();
        final Long oidValue = 123L;
        final double commodityValue = 34.555;

        String expectedCursor = SearchPaginationUtil.constructPreviousCursor(
                Lists.newArrayList(String.valueOf(commodityValue), String.valueOf(oidValue)));

        PaginationApiDTO pagination = PaginationApiDTO.orderBy().cursor(expectedCursor).limit(2).build();

        EntityQueryApiDTO request = EntityQueryApiDTO.queryEntity(selectEntity, pagination);
        EntityQuery query = entityQuery(request);

        //Jooq Fields for building results
        final Field oidField = query.buildAndTrackSelectFieldFromEntityType(primitiveOid);
        final Field nameField = query.buildAndTrackSelectFieldFromEntityType(primitiveName);
        //Values for jooq results
        final Long resultOidValueNext = 5L;
        final double resultCommodityValueNext = 99.99;
        final Long resultOidValuePrevious = 6L;
        final double resultCommodityValuePrevious = 199.99;

        Result<Record2> result = dSLContextSpy.newResult(oidField, nameField);
        result.add(dSLContextSpy.newRecord(oidField, nameField).values(resultOidValueNext, resultCommodityValueNext));
        result.add(dSLContextSpy.newRecord(oidField, nameField).values(resultOidValuePrevious, resultCommodityValuePrevious));
        result.add(dSLContextSpy.newRecord(oidField, nameField).values(7L, "56"));

        doReturn(result).when(dSLContextSpy).fetch(any(Select.class));
        final int totalRecordCount = 12;
        doReturn(totalRecordCount).when(dSLContextSpy).fetchCount(any(Select.class));

        //WHEN
        SearchQueryPaginationResponse<SearchQueryRecordApiDTO> paginationResponse = query.readQueryAndExecute();
        ResponseEntity<List<SearchQueryRecordApiDTO>> responseEntity = paginationResponse.getRestResponse();

        //THEN
        //Check expected results
        assertTrue(responseEntity.getBody().size() == 2);
        assertTrue(responseEntity.getBody().get(0).getOid() == resultOidValuePrevious);
        assertTrue(responseEntity.getBody().get(1).getOid() == resultOidValueNext);

        assertNotNull(responseEntity.getHeaders().get("X-Next-Cursor").get(0));
        assertNotNull(responseEntity.getHeaders().get("X-Previous-Cursor").get(0));

        String resultNextCursor = SearchPaginationUtil.constructNextCursor(
                Lists.newArrayList(String.valueOf(resultCommodityValueNext), String.valueOf(resultOidValueNext)));
        String resultPreviousCursor = SearchPaginationUtil.constructPreviousCursor(
                Lists.newArrayList(String.valueOf(resultCommodityValuePrevious), String.valueOf(resultOidValuePrevious)));
        assertTrue(responseEntity.getHeaders().get("X-Previous-Cursor").get(0).equals(resultPreviousCursor));
        assertTrue(responseEntity.getHeaders().get("X-Next-Cursor").get(0).equals(resultNextCursor));
        assertTrue(responseEntity.getHeaders().get("X-Total-Record-Count").get(0).equals(String.valueOf(totalRecordCount)));
    }


    /**
     * Request for previous page from nextCursor returns nextCursor, totalRecordCount, previousCursor Null.
     * @throws SearchQueryFailedException problems processing request
     */
    @Test
    public void testPreviousPage2To1() throws SearchQueryFailedException {
        //GIVEN
        final FieldApiDTO primitiveOid = PrimitiveFieldApiDTO.oid();
        final FieldApiDTO primitiveName = PrimitiveFieldApiDTO.name();

        final SelectEntityApiDTO selectEntity = SelectEntityApiDTO.selectEntity(EntityType.VIRTUAL_MACHINE)
                .fields(primitiveOid, primitiveName)
                .build();
        final Long oidValue = 123L;
        final double commodityValue = 34.555;

        String expectedCursor = SearchPaginationUtil.constructPreviousCursor(
                Lists.newArrayList(String.valueOf(commodityValue), String.valueOf(oidValue)));

        PaginationApiDTO pagination = PaginationApiDTO.orderBy().cursor(expectedCursor).limit(2).build();

        EntityQueryApiDTO request = EntityQueryApiDTO.queryEntity(selectEntity, pagination);
        EntityQuery query = entityQuery(request);

        //Jooq Fields for building results
        final Field oidField = query.buildAndTrackSelectFieldFromEntityType(primitiveOid);
        final Field nameField = query.buildAndTrackSelectFieldFromEntityType(primitiveName);
        //Values for jooq results
        final Long resultOidValueNext = 5L;
        final double resultCommodityValueNext = 99.99;
        final Long resultOidValuePrevious = 6L;
        final double resultCommodityValuePrevious = 199.99;

        Result<Record2> result = dSLContextSpy.newResult(oidField, nameField);
        result.add(dSLContextSpy.newRecord(oidField, nameField).values(resultOidValueNext, resultCommodityValueNext));
        result.add(dSLContextSpy.newRecord(oidField, nameField).values(resultOidValuePrevious, resultCommodityValuePrevious));

        doReturn(result).when(dSLContextSpy).fetch(any(Select.class));
        final int totalRecordCount = 12;
        doReturn(totalRecordCount).when(dSLContextSpy).fetchCount(any(Select.class));

        //WHEN
        SearchQueryPaginationResponse<SearchQueryRecordApiDTO> paginationResponse = query.readQueryAndExecute();
        ResponseEntity<List<SearchQueryRecordApiDTO>> responseEntity = paginationResponse.getRestResponse();

        //THEN
        assertTrue(responseEntity.getBody().size() == 2);
        assertNotNull(responseEntity.getHeaders().get("X-Next-Cursor").get(0));
        assertFalse(responseEntity.getHeaders().containsKey("X-Previous-Cursor"));

        String resultNextCursor = SearchPaginationUtil.constructNextCursor(
                Lists.newArrayList(String.valueOf(resultCommodityValueNext), String.valueOf(resultOidValueNext)));
        String resultPreviousCursor = SearchPaginationUtil.constructPreviousCursor(
                Lists.newArrayList(String.valueOf(resultCommodityValuePrevious), String.valueOf(resultOidValuePrevious)));
        assertTrue(responseEntity.getHeaders().get("X-Next-Cursor").get(0).equals(resultNextCursor));

        assertTrue(responseEntity.getHeaders().get("X-Total-Record-Count").get(0).equals(String.valueOf(totalRecordCount)));
    }
}
