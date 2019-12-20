package com.vmturbo.auth.api.authorization;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;

import com.google.common.collect.ImmutableList;

import org.apache.commons.collections4.CollectionUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;

import io.jsonwebtoken.lang.Collections;

import com.vmturbo.auth.api.authorization.scoping.EntityAccessScope;
import com.vmturbo.auth.api.authorization.scoping.UserScopeUtils;
import com.vmturbo.auth.api.usermgmt.AuthUserDTO;
import com.vmturbo.common.protobuf.userscope.UserScope.EntityAccessScopeContents;
import com.vmturbo.common.protobuf.userscope.UserScope.EntityAccessScopeResponse;
import com.vmturbo.common.protobuf.userscope.UserScope.OidSetDTO;
import com.vmturbo.common.protobuf.userscope.UserScope.OidSetDTO.AllOids;
import com.vmturbo.common.protobuf.userscope.UserScope.OidSetDTO.OidArray;
import com.vmturbo.common.protobuf.userscope.UserScopeMoles.UserScopeServiceMole;
import com.vmturbo.common.protobuf.userscope.UserScopeServiceGrpc;
import com.vmturbo.common.protobuf.userscope.UserScopeServiceGrpc.UserScopeServiceBlockingStub;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.common.identity.OidSet;

/**
 *
 */
public class UserSessionContextTest {

    private UserScopeServiceMole userScopeService = spy(UserScopeServiceMole.class);

    @Rule
    public GrpcTestServer mockServer = GrpcTestServer.newServer(userScopeService);

    private UserScopeServiceBlockingStub userScopeServiceClient;

    private Clock clock = Clock.systemUTC();

    UserSessionContext userSessionContext;

    @Before
    public void setup() {
        // clear the security context
        SecurityContextHolder.getContext().setAuthentication(null);
        userScopeServiceClient = UserScopeServiceGrpc.newBlockingStub(mockServer.getChannel());
        userSessionContext = new UserSessionContext(userScopeServiceClient, clock);
    }

    @After
    public void teardown() {
        // clear the security context
        SecurityContextHolder.getContext().setAuthentication(null);
    }

    @Test
    public void testDefaultScope() {
        // the default user context will basically have no restrictions.
        Assert.assertFalse(UserScopeUtils.isUserScoped());
        EntityAccessScope entityAccessScope = userSessionContext.getUserAccessScope();
        // the default user access scope will allow access to all entities and have no specific
        // entities in scope.
        Assert.assertTrue(entityAccessScope.containsAll());
        Assert.assertTrue(entityAccessScope.getEntityAccessFilter().containsAll());
        Assert.assertEquals(0, entityAccessScope.getScopeGroupMembers().size());
        Assert.assertEquals(0, entityAccessScope.getScopeGroupIds().size());
        long[] testOids = {0,1,2};
        // default filtering behavior shouldn't filter anything out
        OidSet filteredSet = entityAccessScope.filter(testOids);
        Assert.assertTrue(filteredSet.contains(Collections.arrayToList(testOids)));
    }

    @Test
    public void testToOidSetNoOids() {
        // test no oids
        OidSet noOids = userSessionContext.toOidSet(OidSetDTO.getDefaultInstance());
        Assert.assertEquals(0, noOids.size());
    }

    @Test
    public void testToOidSetAllOids() {
        // test all oids
        OidSet allOids = userSessionContext.toOidSet(OidSetDTO.newBuilder()
                .setAllOids(AllOids.getDefaultInstance())
                .build());
        Assert.assertTrue(allOids.containsAll());
    }

    @Test
    public void testToOidSet() {
        // test a few oids
        OidSet someOids = userSessionContext.toOidSet(OidSetDTO.newBuilder()
                .setArray(OidArray.newBuilder().addAllOids(Arrays.asList(1L,2L,3L)))
                .build());
        Assert.assertFalse(someOids.containsAll());
        Assert.assertEquals(3, someOids.size());
        Assert.assertTrue(someOids.contains(1L));
        Assert.assertTrue(someOids.contains(2L));
        Assert.assertTrue(someOids.contains(3L));
    }

    @Test
    public void testCacheDisabled() {
        List<Long> oidsInScope = Arrays.asList(1L,2L,3L);
        EntityAccessScopeContents group1And2Contents = EntityAccessScopeContents.newBuilder()
                .setAccessibleOids(OidSetDTO.newBuilder()
                        .setArray(OidArray.newBuilder()
                                .addAllOids(oidsInScope)))
                .build();
        EntityAccessScopeResponse response = EntityAccessScopeResponse.newBuilder()
                .setEntityAccessScopeContents(group1And2Contents)
                .build();

        populateUserSessionWithScope(Arrays.asList(1L));
        doReturn(response).when(userScopeService).getEntityAccessScopeMembers(any());
        EntityAccessScope accessScope = userSessionContext.getUserAccessScope();
        Assert.assertEquals(3, accessScope.accessibleOids().size());
        Assert.assertTrue(accessScope.contains(oidsInScope));

        // try identical call again -- the results should be the same
        accessScope = userSessionContext.getUserAccessScope();
        Assert.assertEquals(3, accessScope.accessibleOids().size());
        Assert.assertTrue(accessScope.contains(oidsInScope));
        // verify two fetches fetch were performed, i.e. the value was not cached.
        verify(userScopeService, times(2)).getEntityAccessScopeMembers(any());
    }

    @Test
    public void testCacheEnabled() {
        // use a cached version of the session context
        userSessionContext = new UserSessionContext(userScopeServiceClient, clock, 10, 10, 10);

        List<Long> oidsInScope = Arrays.asList(1L,2L,3L);
        EntityAccessScopeContents group1And2Contents = EntityAccessScopeContents.newBuilder()
                .setAccessibleOids(OidSetDTO.newBuilder()
                        .setArray(OidArray.newBuilder()
                                .addAllOids(oidsInScope)))
                .build();
        EntityAccessScopeResponse response = EntityAccessScopeResponse.newBuilder()
                .setEntityAccessScopeContents(group1And2Contents)
                .build();

        populateUserSessionWithScope(Arrays.asList(1L));
        doReturn(response).when(userScopeService).getEntityAccessScopeMembers(any());
        EntityAccessScope accessScope = userSessionContext.getUserAccessScope();
        Assert.assertEquals(3, accessScope.accessibleOids().size());
        Assert.assertTrue(accessScope.contains(oidsInScope));

        // try identical call again -- the results should be the same
        accessScope = userSessionContext.getUserAccessScope();
        Assert.assertEquals(3, accessScope.accessibleOids().size());
        Assert.assertTrue(accessScope.contains(oidsInScope));
        // verify only one fetch was performed, i.e. the value was cached for the second call.
        verify(userScopeService, times(1)).getEntityAccessScopeMembers(any());
    }

    /**
     * Test the cache expiration and re-fetch mechanism.
     */
    @Test
    public void testCacheResync() {
        final Clock mockClock = mock(Clock.class);
        int cacheExpirationSecs = 10;
        // use cached version of the session context
        userSessionContext = new UserSessionContext(userScopeServiceClient, mockClock, cacheExpirationSecs, 10, 10);

        List<Long> oidsInScope = Arrays.asList(1L,2L,3L);
        EntityAccessScopeContents group1And2Contents = EntityAccessScopeContents.newBuilder()
                .setAccessibleOids(OidSetDTO.newBuilder()
                        .setArray(OidArray.newBuilder()
                                .addAllOids(oidsInScope)))
                .build();
        EntityAccessScopeResponse response = EntityAccessScopeResponse.newBuilder()
                .setEntityAccessScopeContents(group1And2Contents)
                .build();

        populateUserSessionWithScope(Arrays.asList(1L));
        int startTime = 1;
        // set the clock to startTime
        when(mockClock.instant()).thenReturn(Instant.ofEpochSecond(startTime));
        doReturn(response).when(userScopeService).getEntityAccessScopeMembers(any());
        EntityAccessScope accessScope = userSessionContext.getUserAccessScope();
        Assert.assertEquals(3, accessScope.accessibleOids().size());
        Assert.assertTrue(accessScope.contains(oidsInScope));

        List<Long> newOids = Arrays.asList(1L,2L,3L,4L);
        // change the would-be scope data
        EntityAccessScopeContents newContents = EntityAccessScopeContents.newBuilder()
                .setAccessibleOids(OidSetDTO.newBuilder()
                        .setArray(OidArray.newBuilder()
                                .addAllOids(newOids)))
                .build();
        doReturn(EntityAccessScopeResponse.newBuilder()
            .setEntityAccessScopeContents(newContents)
            .build()).when(userScopeService).getEntityAccessScopeMembers(any());

        // advance the clock just shy of the expiration time
        when(mockClock.instant()).thenReturn(Instant.ofEpochSecond(startTime + cacheExpirationSecs - 1));
        // try identical call again -- the results should be the same as the original and use the cache
        accessScope = userSessionContext.getUserAccessScope();
        Assert.assertEquals(3, accessScope.accessibleOids().size());
        Assert.assertTrue(accessScope.contains(oidsInScope));
        // verify only one fetch was performed, i.e. the value was cached for the second call.
        verify(userScopeService, times(1)).getEntityAccessScopeMembers(any());

        // advance the clock again, past the cache expiry time
        when(mockClock.instant()).thenReturn(Instant.ofEpochSecond(startTime + cacheExpirationSecs + 1));
        // same call again -- we should see new results now
        accessScope = userSessionContext.getUserAccessScope();
        Assert.assertEquals(4, accessScope.accessibleOids().size());
        Assert.assertTrue(accessScope.contains(newOids));
    }

    private void populateUserSessionWithScope(List<Long> groupIds) {
        Authentication auth = new UsernamePasswordAuthenticationToken(
                new AuthUserDTO(null,
                        "admin",
                        "pass",
                        "10.10.10.10",
                        "11111",
                        "token",
                        ImmutableList.of("ADMINISTRATOR"),
                        groupIds),
                "admin000",
                CollectionUtils.emptyCollection());

        SecurityContextHolder.getContext().setAuthentication(auth);

    }

}
