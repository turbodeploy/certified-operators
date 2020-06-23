package com.vmturbo.search;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record1;
import org.jooq.Result;
import org.jooq.SQLDialect;
import org.jooq.Select;
import org.jooq.impl.DSL;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.vmturbo.api.dto.searchquery.EntityQueryApiDTO;
import com.vmturbo.api.enums.EntityType;
import com.vmturbo.api.pagination.searchquery.SearchQueryPaginationResponse;
import com.vmturbo.sql.utils.DbEndpoint;

public class ApiQueryEngineTest {

    private ApiQueryEngine apiQueryEngineSpy;
    private DbEndpoint mockReadonlyDbEndpoint;
    private DSLContext dSLContextSpy;

    /**
     * For catching expected exceptions.
     */
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

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
     * Expect UnsupportedOperationException when feature flag is disabled.
     */
    @Test
    public void testFeatureFlagDisabled() throws Exception {
        //GIVEN
        ApiQueryEngine apiQueryEngine = new ApiQueryEngine(mockReadonlyDbEndpoint, false);
        EntityQueryApiDTO request = EntityQueryTest.basicRequestForEntityType(EntityType.VIRTUAL_MACHINE);

        //WHEN
        expectedException.expect(UnsupportedOperationException.class);
        apiQueryEngine.processEntityQuery(request);

        //THEN
        fail("Expected an UnsupportedOperationException, but it was not thrown.");
    }

    /**
     * Expect IllegalArgumentException when feature flag is disabled.
     */
    @Test
    public void testFeatureFlagEnabled() throws Exception {
        //GIVEN
        ApiQueryEngine apiQueryEngine = new ApiQueryEngine(mockReadonlyDbEndpoint, true);
        EntityQueryApiDTO request = EntityQueryTest.basicRequestForEntityType(EntityType.VIRTUAL_MACHINE);

        // Mock the database response
        final Field oidField = DSL.field("oid");
        Result<Record1> result = dSLContextSpy.newResult(oidField);
        result.add(dSLContextSpy.newRecord(oidField).values(188350008821L));

        doReturn(result).when(dSLContextSpy).fetch(any(Select.class));

        //WHEN
        final SearchQueryPaginationResponse response = apiQueryEngine.processEntityQuery(request);

        //THEN
        assertNotNull(response);
    }

}

