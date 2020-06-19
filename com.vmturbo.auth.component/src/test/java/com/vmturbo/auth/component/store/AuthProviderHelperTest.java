package com.vmturbo.auth.component.store;

import static com.vmturbo.auth.api.authorization.jwt.SecurityConstant.PREDEFINED_ROLE_SET;
import static com.vmturbo.auth.component.store.AuthProviderHelper.areValidRoles;
import static org.junit.Assert.*;

import java.util.stream.Collectors;

import org.assertj.core.util.Lists;
import org.junit.Test;

/**
 * Validate {@link AuthProviderHelper}.
 */
public class AuthProviderHelperTest {

    /**
     * Test valid roles.
     */
    @Test
    public void testAreValidRoles() {
        assertTrue(areValidRoles(PREDEFINED_ROLE_SET.stream().collect(Collectors.toList())));
    }

    /**
     * Test invalid roles.
     */
    @Test
    public void testAreValidRolesNegative() {
        assertFalse(areValidRoles(Lists.emptyList()));
        assertFalse(areValidRoles(Lists.newArrayList("invalideRole")));
    }
}