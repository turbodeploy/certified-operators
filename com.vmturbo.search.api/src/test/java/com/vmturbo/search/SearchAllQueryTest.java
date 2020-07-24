package com.vmturbo.search;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record3;
import org.jooq.Result;
import org.jooq.SQLDialect;
import org.jooq.Select;
import org.jooq.SortField;
import org.jooq.impl.DSL;
import org.junit.Before;
import org.junit.Test;

import com.vmturbo.api.dto.searchquery.ConditionApiDTO;
import com.vmturbo.api.dto.searchquery.FieldApiDTO;
import com.vmturbo.api.dto.searchquery.OrderByApiDTO;
import com.vmturbo.api.dto.searchquery.PaginationApiDTO;
import com.vmturbo.api.dto.searchquery.PrimitiveFieldApiDTO;
import com.vmturbo.api.dto.searchquery.SearchAllQueryApiDTO;
import com.vmturbo.api.dto.searchquery.SearchQueryRecordApiDTO;
import com.vmturbo.api.dto.searchquery.SelectAllApiDTO;
import com.vmturbo.api.dto.searchquery.WhereApiDTO;
import com.vmturbo.api.enums.EntityType;
import com.vmturbo.api.enums.GroupType;
import com.vmturbo.api.pagination.searchquery.SearchQueryPaginationResponse;
import com.vmturbo.search.metadata.SearchMetadataMapping;

/**
 * Tests SearchAllQuery.
 */
public class SearchAllQueryTest {

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
     * Creates instance of {@link SearchAllQuery}.
     * @param searchAllQueryApiDTO request to build instance of
     * @return instance of {@link SearchAllQuery}
     */
    private SearchAllQuery searchAllQuery(final SearchAllQueryApiDTO searchAllQueryApiDTO) {
        return new SearchAllQuery(searchAllQueryApiDTO, dSLContextSpy, 100, 101);
    }

    /**
     * {@link SelectAllApiDTO} empty, metadataMapping alwqys set to const map.
     */
    @Test
    public void testMetaDataSetFromConstList() {
        //GIVEN
        WhereApiDTO whereApiDTO = WhereApiDTO.where().build();
        SearchAllQueryApiDTO searchAllQueryApiDTO = SearchAllQueryApiDTO.queryAll(whereApiDTO);
        SearchAllQuery searchAllQuery = searchAllQuery(searchAllQueryApiDTO);

        //When
        Map<FieldApiDTO, SearchMetadataMapping> queryMetadata = searchAllQuery.getMetadataMapping();


        //Then
        assertEquals(queryMetadata, SearchAllQuery.SEARCH_ALL_METADATA);
    }

    /**
     * Supports {@link ConditionApiDTO} configured on name.
     */
    @Test
    public void testWhereConfiguredToNameWorks() {
        //GIVEN

        WhereApiDTO whereApiDTO = WhereApiDTO.where().and(PrimitiveFieldApiDTO.name().like("foo")).build();
        SearchAllQueryApiDTO searchAllQueryApiDTO = SearchAllQueryApiDTO.queryAll(whereApiDTO);
        SearchAllQuery searchAllQuery = searchAllQuery(searchAllQueryApiDTO);

        //When
        List<Condition> nameCondition = searchAllQuery.buildGenericConditions();


        //Then
        assertEquals(1, nameCondition.size());

    }

    /**
     * Does Not Support {@link ConditionApiDTO} NOT configured on name.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testWhereConfiguredNotConfiguredToName() {
        //GIVEN
        WhereApiDTO whereApiDTO = WhereApiDTO.where().and(PrimitiveFieldApiDTO.primitive("s").like("foo")).build();
        SearchAllQueryApiDTO searchAllQueryApiDTO = SearchAllQueryApiDTO.queryAll(whereApiDTO);
        SearchAllQuery searchAllQuery = searchAllQuery(searchAllQueryApiDTO);

        //WHEN
        searchAllQuery.buildGenericConditions();
    }

    /**
     * Test SearchAll process start to finish, returning mocked data for parsing.
     * @throws SearchQueryFailedException problems processing request
     */
    @Test
    public void processSearchAllQuery() throws SearchQueryFailedException {
        //GIVEN
        SelectAllApiDTO selectAllApiDTO = SelectAllApiDTO.selectAll().entityType(EntityType.VIRTUAL_MACHINE).groupType(GroupType.GROUP).build();
        WhereApiDTO whereApiDTO = WhereApiDTO.where().and(PrimitiveFieldApiDTO.name().like("foo")).build();
        PaginationApiDTO paginationApiDTO = PaginationApiDTO.orderBy(OrderByApiDTO.desc(PrimitiveFieldApiDTO.name())).build();
        SearchAllQueryApiDTO searchAllQueryApiDTO = SearchAllQueryApiDTO.queryAll(selectAllApiDTO, whereApiDTO, paginationApiDTO);

        SearchAllQuery searchAllQuery = searchAllQuery(searchAllQueryApiDTO);
        final FieldApiDTO primitiveOid = PrimitiveFieldApiDTO.oid();
        final FieldApiDTO primitiveType = AbstractQuery.PRIMITIVE_TYPE;
        final FieldApiDTO primitiveName = PrimitiveFieldApiDTO.name();

        final Field oidField = searchAllQuery.buildFieldForApiField(primitiveOid, true);
        final Field typefield = searchAllQuery.buildFieldForApiField(primitiveType, true);
        final Field nameField = searchAllQuery.buildFieldForApiField(primitiveName, true);

        final Long oidValue1 = 123L;
        final com.vmturbo.extractor.schema.enums.EntityType primitiveTypeValue1 = com.vmturbo.extractor.schema.enums.EntityType.PHYSICAL_MACHINE;
        final String primitiveNameValue1 = "foobar1";
        final Long oidValue2 = 123L;
        final com.vmturbo.extractor.schema.enums.EntityType primitiveTypeValue2 = com.vmturbo.extractor.schema.enums.EntityType.COMPUTE_CLUSTER;
        final String primitiveNameValue2 = "foobar2";

        Result<Record3> result = dSLContextSpy.newResult(oidField, typefield, nameField);
        result.add(dSLContextSpy.newRecord(oidField, typefield, nameField)
                .values(oidValue1, primitiveTypeValue1, primitiveNameValue1));
        result.add(dSLContextSpy.newRecord(oidField, typefield, nameField)
                .values(oidValue2, primitiveTypeValue2, primitiveNameValue2));

        doReturn(result).when(dSLContextSpy).fetch(any(Select.class));
        doReturn(12).when(dSLContextSpy).fetchCount(any(Select.class));

        //WHEN
        SearchQueryPaginationResponse<SearchQueryRecordApiDTO> paginationResponse = searchAllQuery.readQueryAndExecute();
        List<SearchQueryRecordApiDTO> dtoResults = paginationResponse.getRestResponse().getBody();

        assertTrue(dtoResults.size() == 2);
        assertTrue(dtoResults.get(0).getOid() == oidValue1);
        assertTrue(dtoResults.get(0).getValues().size() == 2);
        assertTrue(dtoResults.get(1).getOid() == oidValue2);
        assertTrue(dtoResults.get(1).getValues().size() == 2);
    }


    /**
     * Test SearchAll process start to finish, returning mocked data for parsing.
     * @throws SearchQueryFailedException problems processing request
     */
    @Test
    public void buildSelectClause() throws SearchQueryFailedException {
        //GIVEN
        SelectAllApiDTO selectAllApiDTO = SelectAllApiDTO.selectAll().entityType(EntityType.VIRTUAL_MACHINE).groupType(GroupType.GROUP).build();
        WhereApiDTO whereApiDTO = WhereApiDTO.where().build();
        SearchAllQueryApiDTO searchAllQueryApiDTO = SearchAllQueryApiDTO.queryAll(selectAllApiDTO, whereApiDTO);

        SearchAllQuery searchAllQuery = searchAllQuery(searchAllQueryApiDTO);

        //WHEN
        Set<Field> fields = searchAllQuery.buildSelectFields();

        //THEN
        assertEquals(3, fields.size());
        Set<String> fieldNames = fields.stream().map(field -> field.getName()).collect(Collectors.toSet());
        assertTrue(fieldNames.contains("oid"));
        assertTrue(fieldNames.contains("name"));
        assertTrue(fieldNames.contains("type"));
    }

    /**
     * Test orderyclause built from pagination dtos.
     * @throws SearchQueryFailedException problems processing request
     */
    @Test
    public void buildOrderBy() throws SearchQueryFailedException {
        //GIVEN
        WhereApiDTO whereApiDTO = WhereApiDTO.where().build();
        PaginationApiDTO paginationApiDTO = PaginationApiDTO.orderBy(OrderByApiDTO.desc(PrimitiveFieldApiDTO.name())).build();

        SearchAllQueryApiDTO searchAllQueryApiDTO = SearchAllQueryApiDTO.queryAll(whereApiDTO, paginationApiDTO);

        SearchAllQuery searchAllQuery = searchAllQuery(searchAllQueryApiDTO);

        //WHEN
        LinkedHashSet<SortField<?>> fields = searchAllQuery.buildOrderByFields();

        //THEN
        assertEquals(2, fields.size());
        final String nameSort = "\"extractor\".\"search_entity\".\"name\" desc nulls last";
        final String oidSort = "\"extractor\".\"search_entity\".\"oid\" asc nulls first";
        assertTrue(containsSort(fields, nameSort));
        assertTrue(containsSort(fields, oidSort));
    }

    /**
     * Error thrown, when not sorting on name.
     * @throws SearchQueryFailedException problems processing request
     */
    @Test(expected = IllegalArgumentException.class)
    public void buildOrderByFailsOnNoneNameSort() throws SearchQueryFailedException {
        //GIVEN
        WhereApiDTO whereApiDTO = WhereApiDTO.where().build();
        PaginationApiDTO paginationApiDTO = PaginationApiDTO.orderBy(OrderByApiDTO.desc(PrimitiveFieldApiDTO.groupType())).build();

        SearchAllQueryApiDTO searchAllQueryApiDTO = SearchAllQueryApiDTO.queryAll(whereApiDTO, paginationApiDTO);

        SearchAllQuery searchAllQuery = searchAllQuery(searchAllQueryApiDTO);

        //WHEN
        searchAllQuery.buildOrderByFields();
    }

    /**
     * Matches sortFields to expectedSort.
     * @param sortFields collection to check
     * @param expectedSort the expected sort
     * @return true if matches
     */
    private boolean containsSort(final LinkedHashSet<SortField<?>> sortFields, final String expectedSort) {
        return sortFields.stream().map(Object::toString).anyMatch(s -> expectedSort.equals(s));
    }
}
