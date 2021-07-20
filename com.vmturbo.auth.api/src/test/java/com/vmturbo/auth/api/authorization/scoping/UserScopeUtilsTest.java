package com.vmturbo.auth.api.authorization.scoping;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.auth.api.authorization.AuthorizationException.UserAccessScopeException;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.components.common.identity.ArrayOidSet;
import com.vmturbo.components.common.identity.OidSet;

/**
 *
 */
public class UserScopeUtilsTest {

    @Test
    public void testFullAccess() throws UserAccessScopeException {
        List<Long> aCoupleOids = Arrays.asList(1L,2L,3L);
        EntityAccessScope scope = new EntityAccessScope(null, null, null, null);
        // the default scope should have access to everything you can throw at it.
        Assert.assertTrue(scope.containsAll());
        Assert.assertTrue(scope.contains(1L));
        Assert.assertTrue(scope.contains(aCoupleOids));
        // filter "market" request
        OidSet filteredMarketRequest = UserScopeUtils.filterEntityRequest(scope, Collections.emptyList());
        Assert.assertTrue(filteredMarketRequest.containsAll());
        // filter specific entities request
        OidSet filteredRequest = UserScopeUtils.filterEntityRequest(scope, aCoupleOids);
        Assert.assertFalse(filteredRequest.containsAll());
        Assert.assertTrue(filteredRequest.contains(aCoupleOids));
    }

    @Test
    public void testScopedAccess() throws UserAccessScopeException {
        List<Long> accessibleOids = Arrays.asList(1L,5L);
        Map<Integer, OidSet> oidsByEntityType = Collections.singletonMap(
                ApiEntityType.VIRTUAL_MACHINE.typeNumber(), new ArrayOidSet(accessibleOids));
        EntityAccessScope scope = new EntityAccessScope(null, new ArrayOidSet(Arrays.asList(1L)),
                new ArrayOidSet(accessibleOids), oidsByEntityType);
        // this scope should only see 1 and 5.
        Assert.assertFalse(scope.containsAll());
        Assert.assertTrue(scope.contains(1L));
        Assert.assertFalse(scope.contains(3L));
        // filter "market" request -- we should get back our original scope.
        OidSet filteredMarketRequest = UserScopeUtils.filterEntityRequest(scope, Collections.emptyList());
        Assert.assertFalse(filteredMarketRequest.containsAll());
        // "market" request for a scoped user will be reduced to the original scope group members
        Assert.assertTrue(filteredMarketRequest.contains(1L));
        Assert.assertFalse(filteredMarketRequest.contains(5L));
        // filter specific entities request - test request for subset of accessible oids
        Assert.assertTrue(UserScopeUtils.filterEntityRequest(scope, Arrays.asList(1L)).contains(1L));
        // simple check on entity types
        Assert.assertEquals(0, scope.getAccessibleOidsByEntityType(ApiEntityType.PHYSICAL_MACHINE).size());
        Assert.assertTrue(scope.getAccessibleOidsByEntityType(ApiEntityType.VIRTUAL_MACHINE).contains(1L));

    }

    @Test(expected = UserAccessScopeException.class)
    public void testScopedAccessDenied() throws UserAccessScopeException {
        // verify that "access denied" is thrown when the filterEntityRequest() param includes oids
        // not in the accessible set.
        List<Long> accessibleOids = Arrays.asList(1L,5L);
        EntityAccessScope scope = new EntityAccessScope(null, null,
                new ArrayOidSet(accessibleOids), null);
        // test request for entities not in subset -- should throw an AccessDeniedError.
        UserScopeUtils.filterEntityRequest(scope, Arrays.asList(1L,2L));
    }

    @Test
    public void testContainsSharedRole() {
        Assert.assertFalse(UserScopeUtils.containsSharedRole(null));
        Assert.assertFalse(UserScopeUtils.containsSharedRole(Collections.emptyList()));
        Assert.assertFalse(UserScopeUtils.containsSharedRole(Arrays.asList("SHARED_J/K")));
        Assert.assertTrue(UserScopeUtils.containsSharedRole(Arrays.asList("SHARED_OBSERVER")));
        Assert.assertTrue(UserScopeUtils.containsSharedRole(Arrays.asList("shared_observer")));
        Assert.assertFalse(UserScopeUtils.containsSharedRole(Arrays.asList("ADMIN", "SCHMADMIN")));
        Assert.assertTrue(UserScopeUtils.containsSharedRole(Arrays.asList("ADMIN", "SHARED_ADVISOR")));
    }
}
