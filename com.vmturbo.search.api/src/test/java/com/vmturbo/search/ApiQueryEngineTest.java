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
import org.jooq.impl.DSL;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.vmturbo.api.dto.searchquery.CommodityFieldApiDTO;
import com.vmturbo.api.dto.searchquery.EntityQueryApiDTO;
import com.vmturbo.api.dto.searchquery.FieldApiDTO;
import com.vmturbo.api.dto.searchquery.FieldValueApiDTO;
import com.vmturbo.api.dto.searchquery.FieldValueApiDTO.Type;
import com.vmturbo.api.dto.searchquery.InclusionConditionApiDTO;
import com.vmturbo.api.dto.searchquery.IntegerConditionApiDTO;
import com.vmturbo.api.dto.searchquery.MultiTextFieldValueApiDTO;
import com.vmturbo.api.dto.searchquery.NumberConditionApiDTO;
import com.vmturbo.api.dto.searchquery.NumberFieldValueApiDTO;
import com.vmturbo.api.dto.searchquery.PrimitiveFieldApiDTO;
import com.vmturbo.api.dto.searchquery.RelatedActionFieldApiDTO;
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
import com.vmturbo.search.metadata.SearchEntityMetadata;
import com.vmturbo.search.metadata.SearchEntityMetadataMapping;
import com.vmturbo.sql.utils.DbEndpoint;

public class ApiQueryEngineTest {

    private ApiQueryEngine apiQueryEngineSpy;
    private DbEndpoint mockReadonlyDbEndpoint;
    private DSLContext dSLContextSpy;
    private SearchEntityMetadataMapping oidPrimitive = SearchEntityMetadataMapping.PRIMITIVE_OID;
    private ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Set up for test.
     *
     * @throws Exception thrown if db access fails to succeed.
     */
    @Before
    public void setup() throws Exception {
        this.mockReadonlyDbEndpoint = mock(DbEndpoint.class);
        this.dSLContextSpy = spy(DSL.using(SQLDialect.POSTGRES));
        doReturn(this.dSLContextSpy).when(this.mockReadonlyDbEndpoint).dslContext();
        this.apiQueryEngineSpy = spy(new ApiQueryEngine(mockReadonlyDbEndpoint, true));
    }

    /**
     *Creates {@link EntityQueryApiDTO} configured for requesting data for entityType.
     *
     * @param entityType The requested entityType of interest
     * @return {@link EntityQueryApiDTO} configured to entityType
     */
    public static EntityQueryApiDTO basicRequestForEntityType(EntityType entityType) {
        SelectEntityApiDTO selectEntity = SelectEntityApiDTO.selectEntity(EntityType.VIRTUAL_MACHINE).build();
        return EntityQueryApiDTO.queryEntity(selectEntity);
    }

    /**
     * Build basic fields absent of what {@link SelectEntityApiDTO} requests.
     */
    @Test
    public void buildSelectFieldsWithNoExtraFieldsSpecified() {
        //GIVEN
        final EntityQueryApiDTO request = basicRequestForEntityType(EntityType.VIRTUAL_MACHINE);
        this.apiQueryEngineSpy.setMetaDataMapping(request);

        Map<FieldApiDTO, SearchEntityMetadataMapping> mappings = SearchEntityMetadata.VIRTUAL_MACHINE.getMetadataMappingMap();
        //WHEN
        Set<String> fields = this.apiQueryEngineSpy.buildSelectFields()
                .stream()
                .map(Field::getName)
                .collect(Collectors.toSet());

        //THEN
        assertTrue(fields.contains(mappings.get(PrimitiveFieldApiDTO.oid()).getColumnName()));
        assertTrue(fields.contains(mappings.get(PrimitiveFieldApiDTO.name()).getColumnName()));
        assertTrue(fields.contains(mappings.get(PrimitiveFieldApiDTO.entitySeverity()).getColumnName()));
        assertTrue(fields.contains(mappings.get(PrimitiveFieldApiDTO.entityState()).getColumnName()));
        assertTrue(fields.contains(mappings.get(PrimitiveFieldApiDTO.entityType()).getColumnName()));
        assertTrue(fields.contains(mappings.get(PrimitiveFieldApiDTO.environmentType()).getColumnName()));
    }

    /**
     *  Expect return empty set when {@link SelectEntityApiDTO#getFields()} empty.
     */
    @Test
    public void getPrimitiveFieldsWithEmptyFields() {
        //GIVEN
        final EntityQueryApiDTO request = basicRequestForEntityType(EntityType.VIRTUAL_MACHINE);
        this.apiQueryEngineSpy.setMetaDataMapping(request);

        //WHEN
        Set<Field> fields = this.apiQueryEngineSpy.buildNonCommonFields(request.getSelect());

        //THEN
        assertTrue(fields.isEmpty());
    }

    /**
     *  Expect return set when {@link SelectEntityApiDTO#getFields()} empty.
     */
    @Test
    public void getFieldsWithPrimitiveTextFields() {
        //GIVEN

        final EntityType type = EntityType.VIRTUAL_MACHINE;
        final FieldApiDTO primitiveEntityField = getAnyEntityKeyField(type, PrimitiveFieldApiDTO.class, null);

        SelectEntityApiDTO selectEntity = SelectEntityApiDTO.selectEntity(EntityType.VIRTUAL_MACHINE).fields(
                primitiveEntityField,
                primitiveEntityField, // Test to make sure duplicate removed
                PrimitiveFieldApiDTO.primitive("I WILL NEVER EXIST") //should be filtered out
        ).build();

        this.apiQueryEngineSpy.setMetaDataMapping(EntityQueryApiDTO.queryEntity(selectEntity));

        //WHEN
        Set<Field> fields = this.apiQueryEngineSpy.buildNonCommonFields(selectEntity);

        //THEN
        Field primiField = this.apiQueryEngineSpy.buildFieldForEntityField(primitiveEntityField, true);
        assertTrue(fields.size() == 1 );
        assertTrue(fields.contains(primiField));
    }

    /**
     *  Expect no duplicates in results
     */
    @Test
    public void getFieldsNoDuplicatesReturned() {
        //GIVEN
        PrimitiveFieldApiDTO severityField = PrimitiveFieldApiDTO.entitySeverity();

        SelectEntityApiDTO selectEntity = SelectEntityApiDTO.selectEntity(EntityType.VIRTUAL_MACHINE).fields(
                severityField,
                severityField, // Test to make sure duplicate removed
                severityField // Test to make sure duplicate removed
        ).build();

        this.apiQueryEngineSpy.setMetaDataMapping(EntityQueryApiDTO.queryEntity(selectEntity));

        //WHEN
        Set<Field> fields = this.apiQueryEngineSpy.buildNonCommonFields(selectEntity);

        //THEN
        assertTrue(fields.size() == 1 );
    }

    /**
     *  Expect invalid select request to be filtered out, empty results
     */
    @Test
    public void getFieldsInvalidEntriesFilteredOut() {
        //GIVEN
        SelectEntityApiDTO selectEntity = SelectEntityApiDTO.selectEntity(EntityType.VIRTUAL_MACHINE).fields(
                PrimitiveFieldApiDTO.primitive("I WILL NEVER EXIST") //should be filter out
        ).build();

        this.apiQueryEngineSpy.setMetaDataMapping(EntityQueryApiDTO.queryEntity(selectEntity));

        //WHEN
        Set<Field> fields = this.apiQueryEngineSpy.buildNonCommonFields(selectEntity);

        //THEN
        assertTrue(fields.isEmpty());

    }

    /** Return {@link EntityType} representative of existing key for SearchEntityMetadata.
     *
     * @param entityType entityType mappings to target
     * @param expectedKeyClass the key class match wanted
     * @param apiDatatype {@link Type} match wanted
     * @return
     */
    private FieldApiDTO getAnyEntityKeyField(@Nonnull EntityType entityType,
            @Nonnull Class<? extends FieldApiDTO> expectedKeyClass,
            @Nullable Type apiDatatype) {
        return SearchEntityMetadata.valueOf(entityType.name())
                .getMetadataMappingMap()
                .entrySet()
                .stream()
                .filter(entry -> {
                    final FieldApiDTO key = entry.getKey();
                    final SearchEntityMetadataMapping value = entry.getValue();
                    final boolean sameType = Objects.isNull(apiDatatype) ? true : value.getApiDatatype().equals(apiDatatype);
                    final boolean sameKey = !key.equals(this.oidPrimitive)
                            && key.getClass().equals(expectedKeyClass);
                    return sameKey && sameType;
                })
                .findAny()
                .get()
                .getKey();
    }

    /**
     *  Expect creates {@link Field} from {@link CommodityFieldApiDTO}.
     */
    @Test
    public void getPrimitiveFieldsWithCommodityFields() {
        //GIVEN
        final EntityType type = EntityType.VIRTUAL_MACHINE;
        final FieldApiDTO commodityField = getAnyEntityKeyField(type, CommodityFieldApiDTO.class, null);
        final SelectEntityApiDTO selectEntity = SelectEntityApiDTO.selectEntity(type)
                .fields(commodityField)
                .build();

        this.apiQueryEngineSpy.setMetaDataMapping(EntityQueryApiDTO.queryEntity(selectEntity));

        //WHEN
        final Set<Field> fields = this.apiQueryEngineSpy.buildNonCommonFields(selectEntity);

        //THEN
        assertFalse(fields.isEmpty());
        Field comField = this.apiQueryEngineSpy.buildFieldForEntityField(commodityField, true);
        assertTrue(fields.contains(comField));
    }

    /**
     *  Expect creates {@link Field} from {@link RelatedEntityFieldApiDTO}.
     */
    @Test
    public void getPrimitiveFieldsWithRelatedEntityFields() {
        //GIVEN
        final EntityType type = EntityType.VIRTUAL_MACHINE;
        final FieldApiDTO relatedEntityField = getAnyEntityKeyField(type, RelatedEntityFieldApiDTO.class, null);
        final SelectEntityApiDTO selectEntity = SelectEntityApiDTO.selectEntity(type)
                .fields(relatedEntityField)
                .build();

        this.apiQueryEngineSpy.setMetaDataMapping(EntityQueryApiDTO.queryEntity(selectEntity));

        //WHEN
        final Set<Field> fields = this.apiQueryEngineSpy.buildNonCommonFields(selectEntity);

        //THEN
        assertFalse(fields.isEmpty());
        Field relField = this.apiQueryEngineSpy.buildFieldForEntityField(relatedEntityField, true);
        assertTrue(fields.contains(relField));
    }

    /**
     * Expect result to generate correct response dtos.
     *
     * <p>This is an end to end test of the class.  The query results are mocked and
     * test focus on expected {@link SearchQueryRecordApiDTO}</p>
     * @throws Exception problems processing request
     */
    @Test
    public void processEntityQuery() throws Exception {
        //GIVEN
        final EntityType type = EntityType.VIRTUAL_MACHINE;
        final FieldApiDTO primitiveOid = PrimitiveFieldApiDTO.oid();
        final FieldApiDTO primitiveTextField = getAnyEntityKeyField(type, PrimitiveFieldApiDTO.class, Type.TEXT);
        final FieldApiDTO commodityNumericField = getAnyEntityKeyField(type, CommodityFieldApiDTO.class, Type.NUMBER);
        final FieldApiDTO relatedEntityMultiTextField = getAnyEntityKeyField(type, RelatedEntityFieldApiDTO.class, Type.MULTI_TEXT);

        final SelectEntityApiDTO selectEntity = SelectEntityApiDTO.selectEntity(EntityType.VIRTUAL_MACHINE)
                .fields(primitiveOid,
                        primitiveTextField,
                        commodityNumericField, // Test to make sure duplicate removed
                        relatedEntityMultiTextField
                ).build();
        EntityQueryApiDTO request = EntityQueryApiDTO.queryEntity(selectEntity);

        this.apiQueryEngineSpy.setMetaDataMapping(request);

        //Jooq Fields for building results
        final Field oidField = this.apiQueryEngineSpy.buildAndTrackSelectFieldFromEntityType(primitiveOid);
        final Field primitive = this.apiQueryEngineSpy.buildAndTrackSelectFieldFromEntityType(primitiveTextField);
        final Field commodity = this.apiQueryEngineSpy.buildAndTrackSelectFieldFromEntityType(commodityNumericField);
        final Field relateEntity = this.apiQueryEngineSpy.buildAndTrackSelectFieldFromEntityType(relatedEntityMultiTextField);
        //Values for jooq results
        final Long oidValue = 123L;
        final String primitiveTextValue = "primitiveTextValue";
        final String commodityNumericValue = "123.456";
        final String relatedEntityMultiTextValue = "[\"relatedEntityMultiTextValue\"]";

        Result<Record4> result = dSLContextSpy.newResult(oidField, primitive, commodity, relateEntity);
        result.add(dSLContextSpy.newRecord(oidField, primitive, commodity, relateEntity)
                .values(oidValue, primitiveTextValue, commodityNumericValue, relatedEntityMultiTextValue));

        doReturn(result).when(dSLContextSpy).fetch(any(Select.class));

        //WHEN
        SearchQueryPaginationResponse<SearchQueryRecordApiDTO> paginationResponse = this.apiQueryEngineSpy.processEntityQuery(request);
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
                    assertTrue(((NumberFieldValueApiDTO)resultValue).getValue() == (Double.valueOf(commodityNumericValue)));
                    break;
                case RELATED_ENTITY:
                    try {
                        assertTrue(((MultiTextFieldValueApiDTO)resultValue).getValue().get(0).equals(
                                objectMapper.readValue(relatedEntityMultiTextValue, String[].class)[0]));
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
        final SelectEntityApiDTO selectEntity = SelectEntityApiDTO.selectEntity(type)
                .fields(relatedEntityField)
                .build();

        this.apiQueryEngineSpy.setMetaDataMapping(EntityQueryApiDTO.queryEntity(selectEntity));
        //WHEN
        Condition condition = this.apiQueryEngineSpy.buildWhereClauses().get(0);

        //THEN
        String expectedCondition = "\"extractor\".\"search_entity\".\"type\" = 'VIRTUAL_MACHINE'";
        assertTrue(condition.toString().equals(expectedCondition));
    }

    /**
     * Expect correct translation of {@link WhereApiDTO} clause for {@link TextConditionApiDTO} a primary column.
     */
    @Test
    public void buildWhereClauseTextConditionEnum() {
        //GIVEN
        TextConditionApiDTO enumCondition = PrimitiveFieldApiDTO.entitySeverity().like(EntitySeverity.CRITICAL.getLiteral());

        final EntityType type = EntityType.VIRTUAL_MACHINE;
        final FieldApiDTO relatedEntityField = getAnyEntityKeyField(type, RelatedEntityFieldApiDTO.class, null);
        final WhereApiDTO where = WhereApiDTO.where().and(enumCondition).build();
        final SelectEntityApiDTO selectEntity = SelectEntityApiDTO.selectEntity(type)
                .fields(relatedEntityField)
                .build();

        EntityQueryApiDTO request = EntityQueryApiDTO.queryEntity(selectEntity, where);
        this.apiQueryEngineSpy.setMetaDataMapping(request);

        //WHEN
        List<Condition> conditions = this.apiQueryEngineSpy.buildWhereClauses();

        //THEN
        assertTrue(conditions.size() == 2);
        Condition condition = conditions.get(1);
        assertTrue(condition.toString().contains("like_regex")); //Regex expression
        assertTrue(condition.toString().contains("(?i)")); //Case Insensitive
        String expectedCondition = "(\"extractor\".\"search_entity\".\"severity\" like_regex '(?i)CRITICAL')";
        assertTrue(condition.toString().equals(expectedCondition));

    }

    /**
     * Expect correct translation of {@link WhereApiDTO} clause for {@link TextConditionApiDTO} of non enum value.
     */
    @Test
    public void buildWhereClauseTextConditionNonEnum() {
        //GIVEN
        TextConditionApiDTO enumCondition = PrimitiveFieldApiDTO.primitive("guestOsType").like("foobar");

        final EntityType type = EntityType.VIRTUAL_MACHINE;
        final FieldApiDTO relatedEntityField = getAnyEntityKeyField(type, RelatedEntityFieldApiDTO.class, null);
        final WhereApiDTO where = WhereApiDTO.where().and(enumCondition).build();
        final SelectEntityApiDTO selectEntity = SelectEntityApiDTO.selectEntity(type)
                .fields(relatedEntityField)
                .build();

        EntityQueryApiDTO request = EntityQueryApiDTO.queryEntity(selectEntity, where);
        this.apiQueryEngineSpy.setMetaDataMapping(request);

        //WHEN
        List<Condition> conditions = this.apiQueryEngineSpy.buildWhereClauses();

        //THEN
        assertTrue(conditions.size() == 2);
        Condition condition = conditions.get(1);
        assertTrue(condition.toString().contains("like_regex")); //Regex expression
        assertTrue(condition.toString().contains("(?i)")); //Case Insensitive
        String expectedCondition = "(attrs->>'guest_os_type' like_regex '(?i)foobar')";
        assertTrue(condition.toString().equals(expectedCondition));
    }

    /**
     * Expect correct translation of {@link WhereApiDTO} clause for {@link InclusionConditionApiDTO} of enum value.
     */
    @Test
    public void buildWhereClauseInclusionCondition() {
        //GIVEN
        String[] states = {"ACTIVE", "IDLE"};
        InclusionConditionApiDTO enumCondition = PrimitiveFieldApiDTO.entityState()
                .in(states);

        final EntityType type = EntityType.VIRTUAL_MACHINE;
        final WhereApiDTO where = WhereApiDTO.where().and(enumCondition).build();
        final SelectEntityApiDTO selectEntity = SelectEntityApiDTO.selectEntity(type)
                .fields(PrimitiveFieldApiDTO.entityState())
                .build();

        EntityQueryApiDTO request = EntityQueryApiDTO.queryEntity(selectEntity, where);
        this.apiQueryEngineSpy.setMetaDataMapping(request);

        //WHEN
        List<Condition> conditions = this.apiQueryEngineSpy.buildWhereClauses();

        //THEN
        assertTrue(conditions.size() == 2);
        Condition condition = conditions.get(1);

        String expectedCondition1 = "\"extractor\".\"search_entity\".\"state\" in (\n  "
                + "'POWERED_ON', 'POWERED_OFF'\n)";
        assertTrue(condition.toString().equals(expectedCondition1));
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
        final SelectEntityApiDTO selectEntity = SelectEntityApiDTO.selectEntity(type)
                .fields(commodityField)
                .build();

        EntityQueryApiDTO request = EntityQueryApiDTO.queryEntity(selectEntity, where);
        this.apiQueryEngineSpy.setMetaDataMapping(request);

        //WHEN
        List<Condition> conditions = this.apiQueryEngineSpy.buildWhereClauses();

        //THEN
        assertTrue(conditions.size() == 2);
        Condition condition = conditions.get(1);
        String expectedCondition1 = "cast(attrs->>'vcpu_used' as decimal) = 98.89";
        assertTrue(condition.toString().equals(expectedCondition1));
    }

    /**
     * Expect correct translation of {@link WhereApiDTO} clause for {@link IntegerConditionApiDTO}.
     */
    @Test
    public void buildWhereClauseIntegerCondition() {
        //GIVEN
        final EntityType type = EntityType.VIRTUAL_MACHINE;
        final FieldApiDTO commodityField = RelatedActionFieldApiDTO.actionCount();
        Long longValue = 98L;
        IntegerConditionApiDTO integerConditionApiDTO = commodityField.eq(longValue);
        final WhereApiDTO where = WhereApiDTO.where().and(integerConditionApiDTO).build();
        final SelectEntityApiDTO selectEntity = SelectEntityApiDTO.selectEntity(type)
                .fields(commodityField)
                .build();

        EntityQueryApiDTO request = EntityQueryApiDTO.queryEntity(selectEntity, where);
        this.apiQueryEngineSpy.setMetaDataMapping(request);

        //WHEN
        List<Condition> conditions = this.apiQueryEngineSpy.buildWhereClauses();

        //THEN
        assertTrue(conditions.size() == 2);
        Condition condition = conditions.get(1);
        String expectedCondition1 = "cast(\"extractor\".\"search_entity\".\"num_actions\" as bigint) = 98";
        assertTrue(condition.toString().equals(expectedCondition1));
    }

    /**
     * Expect units to be returned for {@link CommodityFieldApiDTO}.
     */
    @Test
    public void mapRecordToValueReturningUnits() {
        //GIVEN
        SearchEntityMetadataMapping columnMetadata = SearchEntityMetadataMapping.COMMODITY_CPU_UTILIZATION;
        FieldApiDTO fieldApiDto = CommodityFieldApiDTO.utilization(CommodityType.VCPU);
        this.apiQueryEngineSpy.entityMetadata = mock(Map.class);
        doReturn(columnMetadata).when(this.apiQueryEngineSpy.entityMetadata).get(any());
        final Field commodityField = this.apiQueryEngineSpy.buildAndTrackSelectFieldFromEntityType(fieldApiDto);
        Record record = dSLContextSpy.newRecord(commodityField).values("45");

        //WHEN
        NumberFieldValueApiDTO value = (NumberFieldValueApiDTO) this.apiQueryEngineSpy.mapRecordToValue(record, columnMetadata, fieldApiDto).get();

        //THEN
        assertNotNull(value.getUnits());
        assertTrue(value.getUnits().equals(columnMetadata.getUnitsString()));
    }

}

