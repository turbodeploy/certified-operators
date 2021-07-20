package com.vmturbo.auth.api.authorization.scoping;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.ImmutableList;

import org.junit.Test;

import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.components.common.identity.ArrayOidSet;
import com.vmturbo.components.common.identity.OidFilter;
import com.vmturbo.components.common.identity.OidSet;

/**
 * Test EntityAccessScope.
 */
public class EntityAccessScopeTest {

    /**
     * Test getAccessibleOidsByEntityTypes.
     */
    @Test
    public void testGetAccessibleOidsByEntityTypes() {
        // let's set up a test access scope
        Collection<Long> groups = ImmutableList.of(1L);
        OidSet scopeGroupMembers = new ArrayOidSet(new long[]{2L, 3L});
        OidFilter accessFilter = new ArrayOidSet(new long[]{2L, 3L, 4L, 5L});
        Map<Integer, OidSet> accessibleOidsByType = new HashMap<>();
        OidSet typeASet = new ArrayOidSet(new long[]{2L});
        accessibleOidsByType.put(ApiEntityType.VIRTUAL_MACHINE.typeNumber(), typeASet);
        OidSet typeBSet = new ArrayOidSet(new long[]{3L, 4L});
        accessibleOidsByType.put(ApiEntityType.PHYSICAL_MACHINE.typeNumber(), typeBSet);
        OidSet typeCSet = new ArrayOidSet(new long[]{5L});
        accessibleOidsByType.put(ApiEntityType.STORAGE.typeNumber(), typeCSet);

        EntityAccessScope scope = new EntityAccessScope(groups, scopeGroupMembers, accessFilter, accessibleOidsByType);
        // verfiy that an invalid or non-existent type gets an empty set
        assertEquals(OidSet.EMPTY_OID_SET, scope.getAccessibleOidsByEntityType(null));
        // test simple case of single type
        assertEquals(typeASet, scope.getAccessibleOidsByEntityType(ApiEntityType.VIRTUAL_MACHINE));
        // verify that requesting multiple types gives back a set containing those types and not the
        // others
        OidSet typesBandC = scope.getAccessibleOidsByEntityTypes(ImmutableList.of(ApiEntityType.PHYSICAL_MACHINE, ApiEntityType.STORAGE));
        assertTrue(typesBandC.contains(typeBSet));
        assertTrue(typesBandC.contains(typeCSet));
        assertFalse(typesBandC.contains(typeASet));
    }
}
