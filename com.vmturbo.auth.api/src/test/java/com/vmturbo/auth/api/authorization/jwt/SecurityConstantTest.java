package com.vmturbo.auth.api.authorization.jwt;

import static com.vmturbo.auth.api.authorization.jwt.SecurityConstant.ADMINISTRATOR;
import static com.vmturbo.auth.api.authorization.jwt.SecurityConstant.ADVISOR;
import static com.vmturbo.auth.api.authorization.jwt.SecurityConstant.AUTOMATOR;
import static com.vmturbo.auth.api.authorization.jwt.SecurityConstant.DEPLOYER;
import static com.vmturbo.auth.api.authorization.jwt.SecurityConstant.OBSERVER;
import static com.vmturbo.auth.api.authorization.jwt.SecurityConstant.OPERATIONAL_OBSERVER;
import static com.vmturbo.auth.api.authorization.jwt.SecurityConstant.PREDEFINED_ROLE_SET;
import static com.vmturbo.auth.api.authorization.jwt.SecurityConstant.SHARED_ADVISOR;
import static com.vmturbo.auth.api.authorization.jwt.SecurityConstant.SHARED_OBSERVER;
import static com.vmturbo.auth.api.authorization.jwt.SecurityConstant.SITE_ADMIN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

/**
 * Test {@link SecurityConstant} are defined correctly.
 */
public class SecurityConstantTest {

    /**
     * Verify the set has predefined roles in String type.
     */
    @Test
    public void testPrefinedRoleSet() {
        assertTrue(PREDEFINED_ROLE_SET.contains("ADMINISTRATOR"));
        assertTrue(PREDEFINED_ROLE_SET.contains("SITE_ADMIN"));
        assertTrue(PREDEFINED_ROLE_SET.contains("AUTOMATOR"));
        assertTrue(PREDEFINED_ROLE_SET.contains("DEPLOYER"));
        assertTrue(PREDEFINED_ROLE_SET.contains("ADVISOR"));
        assertTrue(PREDEFINED_ROLE_SET.contains("OBSERVER"));
        assertTrue(PREDEFINED_ROLE_SET.contains("OPERATIONAL_OBSERVER"));
        assertTrue(PREDEFINED_ROLE_SET.contains("SHARED_ADVISOR"));
        assertTrue(PREDEFINED_ROLE_SET.contains("SHARED_OBSERVER"));
        assertEquals(9, PREDEFINED_ROLE_SET.size());
    }

    /**
     * Verify the security constants.
     */
    @Test
    public void testConstant() {
        assertEquals("ADMINISTRATOR", ADMINISTRATOR);
        assertEquals("SITE_ADMIN", SITE_ADMIN);
        assertEquals("AUTOMATOR", AUTOMATOR);
        assertEquals("DEPLOYER", DEPLOYER);
        assertEquals("ADVISOR", ADVISOR);
        assertEquals("OBSERVER", OBSERVER);
        assertEquals("OPERATIONAL_OBSERVER", OPERATIONAL_OBSERVER);
        assertEquals("SHARED_ADVISOR", SHARED_ADVISOR);
        assertEquals("SHARED_OBSERVER", SHARED_OBSERVER);
    }
}