package com.vmturbo.api.component.external.api.service;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.search.ApiQueryEngine;

/**
 * Tests for {@link SearchQueryService}.
 */
public class SearchQueryServiceTest {

    private SearchQueryService searchQueryService;
    private final UserSessionContext userSessionContext = Mockito.mock(UserSessionContext.class);

    /**
     * Setup before each test.
     */
    @Before
    public void setup() {
        doReturn(true).when(userSessionContext).isUserScoped();
        this.searchQueryService = new SearchQueryService(mock(ApiQueryEngine.class), userSessionContext);
    }

    /**
     * Test that scoped user is not allowed to use this endpoint.
     *
     * @throws Exception if anything goes wrong
     */
    @Test(expected = UnsupportedOperationException.class)
    public void testScopedUserNotAllowedForSearchAll() throws Exception {
        searchQueryService.searchAll(null);
    }

    /**
     * Test that scoped user is not allowed to use this endpoint.
     *
     * @throws Exception if anything goes wrong
     */
    @Test(expected = UnsupportedOperationException.class)
    public void testScopedUserNotAllowedForSearchEntities() throws Exception {
        searchQueryService.searchEntities(null);
    }

    /**
     * Test that scoped user is not allowed to use this endpoint.
     *
     * @throws Exception if anything goes wrong
     */
    @Test(expected = UnsupportedOperationException.class)
    public void testScopedUserNotAllowedForSearchGroups() throws Exception {
        searchQueryService.searchGroups(null);
    }

    /**
     * Test that scoped user is not allowed to use this endpoint.
     *
     * @throws Exception if anything goes wrong
     */
    @Test(expected = UnsupportedOperationException.class)
    public void testScopedUserNotAllowedForCountEntities() throws Exception {
        searchQueryService.countEntities(null);
    }

    /**
     * Test that scoped user is not allowed to use this endpoint.
     *
     * @throws Exception if anything goes wrong
     */
    @Test(expected = UnsupportedOperationException.class)
    public void testScopedUserNotAllowedForCountGroups() throws Exception {
        searchQueryService.countGroups(null);
    }

    /**
     * Test that scoped user is not allowed to use this endpoint.
     *
     * @throws Exception if anything goes wrong
     */
    @Test(expected = UnsupportedOperationException.class)
    public void testScopedUserNotAllowedForEntityFields() throws Exception {
        searchQueryService.entityFields(null);
    }

    /**
     * Test that scoped user is not allowed to use this endpoint.
     *
     * @throws Exception if anything goes wrong
     */
    @Test(expected = UnsupportedOperationException.class)
    public void testScopedUserNotAllowedForGroupFields() throws Exception {
        searchQueryService.groupFields(null);
    }
}