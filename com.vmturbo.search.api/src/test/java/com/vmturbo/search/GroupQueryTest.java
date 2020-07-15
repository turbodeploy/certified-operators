package com.vmturbo.search;

import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

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

import com.vmturbo.api.dto.searchquery.AggregateCommodityFieldApiDTO;
import com.vmturbo.api.dto.searchquery.EnumFieldValueApiDTO;
import com.vmturbo.api.dto.searchquery.FieldApiDTO;
import com.vmturbo.api.dto.searchquery.FieldValueApiDTO;
import com.vmturbo.api.dto.searchquery.FieldValueApiDTO.Type;
import com.vmturbo.api.dto.searchquery.GroupQueryApiDTO;
import com.vmturbo.api.dto.searchquery.IntegerFieldValueApiDTO;
import com.vmturbo.api.dto.searchquery.MemberFieldApiDTO;
import com.vmturbo.api.dto.searchquery.NumberFieldValueApiDTO;
import com.vmturbo.api.dto.searchquery.PrimitiveFieldApiDTO;
import com.vmturbo.api.dto.searchquery.SearchQueryRecordApiDTO;
import com.vmturbo.api.dto.searchquery.SelectEntityApiDTO;
import com.vmturbo.api.dto.searchquery.SelectGroupApiDTO;
import com.vmturbo.api.dto.searchquery.TextFieldValueApiDTO;
import com.vmturbo.api.enums.GroupType;
import com.vmturbo.api.pagination.searchquery.SearchQueryPaginationResponse;
import com.vmturbo.extractor.schema.enums.EntityType;
import com.vmturbo.extractor.schema.tables.SearchEntity;
import com.vmturbo.search.mappers.GroupTypeMapper;
import com.vmturbo.search.metadata.SearchGroupMetadata;
import com.vmturbo.search.metadata.SearchMetadataMapping;

/**
 * Tests for GroupQuery.
 *
 * <p>Majority of unit tests are located in {@link EntityQueryTest} as
 * the 2 classes extend same {@link AbstractSearchQuery}.</p>
 */
public class GroupQueryTest {

    private static final SearchMetadataMapping oidPrimitive = SearchMetadataMapping.PRIMITIVE_OID;

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

    /**
     * Returns {@link EntityType} representative of existing key for SearchEntityMetadata.
     *
     * @param groupType groupType mappings to target
     * @param expectedKeyClass the key class match wanted
     * @param apiDatatype {@link Type} match wanted
     * @return the field representative of existing key for SearchEntityMetadata
     */
    private FieldApiDTO getAnyGroupKeyField(@Nonnull GroupType groupType,
            @Nonnull Class<? extends FieldApiDTO> expectedKeyClass,
            @Nullable Type apiDatatype) {
        return SearchGroupMetadata.valueOf(groupType.name())
                .getMetadataMappingMap()
                .entrySet()
                .stream()
                .filter(entry -> {
                    final FieldApiDTO key = entry.getKey();
                    final SearchMetadataMapping value = entry.getValue();
                    final boolean sameType = Objects.isNull(apiDatatype) ? true : value.getApiDatatype().equals(apiDatatype);
                    final boolean sameKey = !key.equals(this.oidPrimitive)
                            && key.getClass().equals(expectedKeyClass);
                    return sameKey && sameType;
                })
                .findAny()
                .get()
                .getKey();
    }

    private GroupQuery groupQuery(final GroupQueryApiDTO groupQueryDto) {
        return new GroupQuery(groupQueryDto, dSLContextSpy, 100, 101);
    }

    /**
     * Expect result to generate correct response dtos.
     *
     * <p>This is an end to end test of the class.  The query results are mocked and
     * test focus on expected {@link SearchQueryRecordApiDTO}</p>
     * @throws SearchQueryFailedException problems processing request
     */
    @Test
    public void processGroupQuery() throws SearchQueryFailedException {
        //GIVEN
        final GroupType type = GroupType.COMPUTE_HOST_CLUSTER;
        final FieldApiDTO primitiveOid = PrimitiveFieldApiDTO.oid();
        final FieldApiDTO primitiveTextField = PrimitiveFieldApiDTO.origin();
        final FieldApiDTO aggregatedCommodityNumericField = getAnyGroupKeyField(type, AggregateCommodityFieldApiDTO.class, Type.NUMBER);
        final FieldApiDTO memberFieldApiDTO = getAnyGroupKeyField(type, MemberFieldApiDTO.class, Type.INTEGER);

        final SelectGroupApiDTO selectGroup = SelectGroupApiDTO.selectGroup(GroupType.COMPUTE_HOST_CLUSTER)
                .fields(primitiveOid,
                        primitiveTextField,
                        aggregatedCommodityNumericField, // Test to make sure duplicate removed
                        memberFieldApiDTO
                ).build();

        GroupQueryApiDTO request = GroupQueryApiDTO.queryGroup(selectGroup);
        GroupQuery query = groupQuery(request);

        //Jooq Fields for building results
        final Field oidField = query.buildAndTrackSelectFieldFromEntityType(primitiveOid);
        final Field primitive = query.buildAndTrackSelectFieldFromEntityType(primitiveTextField);
        final Field commodity = query.buildAndTrackSelectFieldFromEntityType(aggregatedCommodityNumericField);
        final Field relateEntity = query.buildAndTrackSelectFieldFromEntityType(memberFieldApiDTO);
        //Values for jooq results
        final Long oidValue = 123L;
        final String primitiveTextValue = "primitiveTextValue";
        final String aggregatedCommodityNumericValue = "123.456";
        final String memberFieldApiDTOValue = String.valueOf(oidValue);

        Result<Record4>
                result = dSLContextSpy.newResult(oidField, primitive, commodity, relateEntity);
        result.add(dSLContextSpy.newRecord(oidField, primitive, commodity, relateEntity)
                .values(oidValue, primitiveTextValue, aggregatedCommodityNumericValue, memberFieldApiDTOValue));

        doReturn(result).when(dSLContextSpy).fetch(any(Select.class));
        doReturn(4).when(dSLContextSpy).fetchCount(any(Select.class));

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
                case AGGREGATE_COMMODITY:
                    assertTrue(((NumberFieldValueApiDTO)resultValue).getValue() == (Double.valueOf(aggregatedCommodityNumericValue)));
                    break;
                case MEMBER:
                    assertTrue(((IntegerFieldValueApiDTO)resultValue).getValue() == Long.valueOf(memberFieldApiDTOValue));
                    break;
                default:
                    Assert.fail("Unexpected Value");
            }
        });
    }


    /**
     * Expect correct mapping of {@link EntityType} to {@link GroupType}
     */
    @Test
    public void mapRecordToValueReturnsGroupTypeApiEnum() {
        //GIVEN
        final SelectGroupApiDTO selectGroup = SelectGroupApiDTO.selectGroup(GroupType.COMPUTE_HOST_CLUSTER).build();
        GroupQueryApiDTO request = GroupQueryApiDTO.queryGroup(selectGroup);
        GroupQuery query = groupQuery(request);

        EntityType recordValue = EntityType.COMPUTE_HOST_CLUSTER;
        Record record = dSLContextSpy.newRecord(SearchEntity.SEARCH_ENTITY.TYPE).values(recordValue);
        PrimitiveFieldApiDTO groupTypeFieldDto = PrimitiveFieldApiDTO.groupType();
        //WHEN
        EnumFieldValueApiDTO
                value = (EnumFieldValueApiDTO) query.mapRecordToValue(record, SearchMetadataMapping.PRIMITIVE_ENTITY_TYPE, groupTypeFieldDto).get();

        //THEN
        assertTrue(value.getValue().equals(GroupTypeMapper.fromSearchSchemaToApi(recordValue).toString()));
    }

    /**
     * Build basic fields absent of what {@link SelectEntityApiDTO} requests.
     */
    @Test
    public void buildSelectFieldsWithNoExtraFieldsSpecified() {
        //GIVEN
        final SelectGroupApiDTO selectGroup = SelectGroupApiDTO.selectGroup(GroupType.GROUP).build();
        final GroupQueryApiDTO request = GroupQueryApiDTO.queryGroup(selectGroup);
        GroupQuery query = groupQuery(request);

        Map<FieldApiDTO, SearchMetadataMapping> mappings = SearchGroupMetadata.GROUP.getMetadataMappingMap();
        //WHEN
        Set<String> fields =
                query.buildSelectFields().stream().map(Field::getName).collect(Collectors.toSet());

        //THEN
        assertTrue(fields.contains(mappings.get(PrimitiveFieldApiDTO.oid()).getColumnName()));
        assertTrue(fields.contains(mappings.get(PrimitiveFieldApiDTO.name()).getColumnName()));
        assertTrue(fields.contains(mappings.get(PrimitiveFieldApiDTO.groupType()).getColumnName()));
    }

}
