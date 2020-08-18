package com.vmturbo.search;

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

import com.vmturbo.api.dto.searchquery.EntityCountRequestApiDTO;
import com.vmturbo.api.dto.searchquery.EntityCountRequestApiDTO.GroupByCriterion;
import com.vmturbo.api.dto.searchquery.EnumFieldValueApiDTO;
import com.vmturbo.api.dto.searchquery.FieldApiDTO.FieldType;
import com.vmturbo.api.dto.searchquery.FieldValueApiDTO;
import com.vmturbo.api.dto.searchquery.PrimitiveFieldApiDTO;
import com.vmturbo.api.dto.searchquery.SearchCountRecordApiDTO;
import com.vmturbo.extractor.schema.enums.EntityType;
import com.vmturbo.extractor.schema.enums.EnvironmentType;
import com.vmturbo.extractor.schema.tables.SearchEntity;

/**
 * Tests for EntityCountQuery.
 */
public class EntityCountQueryTest {

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
    public void processEntityCountQuery() throws Exception {
        //GIVEN
        final EntityCountRequestApiDTO request = EntityCountRequestApiDTO.entityCountRequest(GroupByCriterion.values());

        //The class under test
        final EntityCountQuery countQuery = new EntityCountQuery(request, dSLContextSpy);

        //Jooq Fields for building results
        final Field entityTypeField = SearchEntity.SEARCH_ENTITY.TYPE;
        final Field environmentTypeField = SearchEntity.SEARCH_ENTITY.ENVIRONMENT;
        final Field countField = DSL.count();
        //Values for jooq results
        final EntityType entityTypeValue = EntityType.VIRTUAL_MACHINE;
        final EnvironmentType environmentTypeValue = EnvironmentType.CLOUD;
        final int countValue = 88;

        Result<Record3> response = dSLContextSpy.newResult(entityTypeField, environmentTypeField, countField);
        response.add(dSLContextSpy.newRecord(entityTypeField, environmentTypeField, countField)
            .values(entityTypeValue, environmentTypeValue, countValue));

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
            EnumFieldValueApiDTO enumFieldValue = (EnumFieldValueApiDTO)resultValue;
            switch (field.getFieldName()) {
                case "entityType":
                    com.vmturbo.api.enums.EntityType entityType =
                        com.vmturbo.api.enums.EntityType.fromString(enumFieldValue.getValue());
                    assertEquals(com.vmturbo.api.enums.EntityType.VirtualMachine, entityType);
                    break;
                case "environmentType":
                    com.vmturbo.api.enums.EnvironmentType environmentType =
                        com.vmturbo.api.enums.EnvironmentType.valueOf(enumFieldValue.getValue());
                    assertEquals(com.vmturbo.api.enums.EnvironmentType.CLOUD, environmentType);
                    break;
            }
        });
    }
}
