package com.vmturbo.search;

import static com.vmturbo.api.enums.Origin.DISCOVERED;
import static com.vmturbo.api.enums.Origin.valueOf;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import java.util.List;

import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record3;
import org.jooq.Result;
import org.jooq.SQLDialect;
import org.jooq.Select;
import org.jooq.impl.DSL;
import org.junit.Before;
import org.junit.Test;

import com.vmturbo.api.dto.searchquery.EnumFieldValueApiDTO;
import com.vmturbo.api.dto.searchquery.FieldApiDTO.FieldType;
import com.vmturbo.api.dto.searchquery.FieldValueApiDTO;
import com.vmturbo.api.dto.searchquery.GroupCountRequestApiDTO;
import com.vmturbo.api.dto.searchquery.GroupCountRequestApiDTO.GroupByCriterion;
import com.vmturbo.api.dto.searchquery.PrimitiveFieldApiDTO;
import com.vmturbo.api.dto.searchquery.SearchCountRecordApiDTO;
import com.vmturbo.api.dto.searchquery.TextFieldValueApiDTO;
import com.vmturbo.api.enums.GroupType;
import com.vmturbo.api.enums.Origin;
import com.vmturbo.extractor.schema.enums.EntityType;
import com.vmturbo.extractor.schema.tables.SearchEntity;

/**
 * Tests for GroupCountQuery.
 */
public class GroupCountQueryTest {

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
     * Expect result to generate correct response dtos.
     *
     * <p>This is an end to end test of the class.  The query results are mocked and
     * test focus on expected {@link SearchCountRecordApiDTO}</p>
     *
     * @throws Exception problems processing request
     */
    @Test
    public void processGroupCountQuery() throws Exception {
        //GIVEN
        final GroupCountRequestApiDTO request = GroupCountRequestApiDTO.groupCountRequest(GroupByCriterion.values());

        //The class under test
        final GroupCountQuery countQuery = new GroupCountQuery(request, dSLContextSpy);

        //Jooq Fields for building results
        final Field groupTypeField = SearchEntity.SEARCH_ENTITY.TYPE;
        final Field groupOriginField = countQuery.buildFieldForApiField(PrimitiveFieldApiDTO.origin(), true);
        final Field countField = DSL.count();
        //Values for jooq results
        final EntityType groupTypeValue = EntityType.COMPUTE_HOST_CLUSTER;
        final String groupOriginValue = "DISCOVERED";
        final int countValue = 14;

        Result<Record3> response = dSLContextSpy.newResult(groupTypeField, groupOriginField, countField);
        response.add(dSLContextSpy.newRecord(groupTypeField, groupOriginField, countField)
            .values(groupTypeValue, groupOriginValue, countValue));

        doReturn(response).when(dSLContextSpy).fetch(any(Select.class));

        //WHEN
        List<SearchCountRecordApiDTO> results = countQuery.count();

        //THEN
        assertEquals(1, results.size());
        SearchCountRecordApiDTO result = results.iterator().next();
        assertEquals(countValue, result.getCount());
        List<FieldValueApiDTO> resultGroupByValues = result.getGroupBys();
        assertEquals(2, resultGroupByValues.size());
        resultGroupByValues.forEach(resultValue -> {
            assertEquals(FieldType.PRIMITIVE, resultValue.getField().getFieldType());
            PrimitiveFieldApiDTO field = (PrimitiveFieldApiDTO)resultValue.getField();
            switch (field.getFieldName()) {
                case "groupType":
                    EnumFieldValueApiDTO enumFieldValue = (EnumFieldValueApiDTO)resultValue;
                    com.vmturbo.api.enums.GroupType groupType =
                        com.vmturbo.api.enums.GroupType.valueOf(enumFieldValue.getValue());
                    assertEquals(GroupType.COMPUTE_HOST_CLUSTER, groupType);
                    break;
                case "origin":
                    TextFieldValueApiDTO textFieldValue = (TextFieldValueApiDTO)resultValue;
                    Origin origin = valueOf(textFieldValue.getValue());
                    assertEquals(DISCOVERED, origin);
                    break;
            }
        });
    }
}
