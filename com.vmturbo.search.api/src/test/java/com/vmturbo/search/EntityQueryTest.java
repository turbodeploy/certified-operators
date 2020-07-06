package com.vmturbo.search;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Record4;
import org.jooq.Result;
import org.jooq.SQLDialect;
import org.jooq.Select;
import org.jooq.SortField;
import org.jooq.impl.DSL;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

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
        return new EntityQuery(entityQueryApiDTO, dSLContextSpy);
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
     * @throws Exception problems processing request
     */
    @Test
    public void processEntityQuery() throws Exception {
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

    private boolean containsSort(final List<SortField<?>> sortFields, final String expectedSort) {
        return sortFields.stream().map(Object::toString).anyMatch(s -> expectedSort.equals(s));
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
        List<SortField<?>> sortFields = query.buildOrderByFields();

        //THEN
        assertTrue(sortFields.size() == 1);
        final String nameSort = "\"extractor\".\"search_entity\".\"name\" desc";
        assertTrue(containsSort(sortFields, nameSort));

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
        List<SortField<?>> sortFields = query.buildOrderByFields();

        //THEN
        assertTrue(sortFields.size() == 2);
        final String integerSort = "cast(attrs->>'num_vms' as bigint) asc";
        final String doubleSort = "cast(attrs->>'mem_utilization' as double) desc";
        assertTrue(containsSort(sortFields, integerSort));
        assertTrue(containsSort(sortFields, doubleSort));
    }
}