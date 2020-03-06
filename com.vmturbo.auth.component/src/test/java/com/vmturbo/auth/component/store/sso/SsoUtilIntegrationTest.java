package com.vmturbo.auth.component.store.sso;

import static com.vmturbo.auth.api.authorization.jwt.SecurityConstant.ADMINISTRATOR;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Collections;

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.vmturbo.auth.api.usermgmt.SecurityGroupDTO;

/**
 * Integration test for {@link SsoUtil}.
 */
@Ignore("only work when connection to vmturbo.com network.")
public class SsoUtilIntegrationTest {

    private static final String PROVIDER_URI = "ldap://dell1.corp.vmturbo.com";
    private static final String USER_NAME = "provide your own AD user name in corp.vmturbo.com domain";
    private static final String PASSWORD = "provide your own AD password";
    private static final String WRONG_PASSWORD = "WRONGPASS";
    private static final String GROUP_NAME = "VPNusers";
    private static final String ADMIN_GROUP = "admin group";
    private static SsoUtil ssoUtil;

    /**
     * Before class.
     */
    @BeforeClass
    public static void setupBeforeClass() {
        ssoUtil = new SsoUtil();
        ssoUtil.setDomainName("corp.vmturbo.com");
        ssoUtil.setLoginProviderURI(PROVIDER_URI);
    }

    /**
     * Test is AD available.
     */
    @Test
    public void testIsAdAvailable() {
        assertTrue(ssoUtil.isADAvailable());
    }

    /**
     * Authenticate user in group, positive case.
     */
    @Test
    public void testAuthenticateUserInGroup() {
        SecurityGroupDTO securityGroup = new SecurityGroupDTO(ADMIN_GROUP, "", ADMINISTRATOR);
        ssoUtil.putSecurityGroup(GROUP_NAME, securityGroup);
        assertNotNull(ssoUtil.authenticateUserInGroup(USER_NAME, PASSWORD,
                Collections.singleton(PROVIDER_URI)));
    }

    /**
     * Authenticate user in group, negative case - no such group.
     */
    @Test
    public void testAuthenticateUserInGroupNegative() {
        SecurityGroupDTO securityGroup = new SecurityGroupDTO(ADMIN_GROUP, "", ADMINISTRATOR);
        assertNull(ssoUtil.authenticateUserInGroup(USER_NAME, PASSWORD,
                Collections.singleton(PROVIDER_URI)));
    }

    /**
     * Authenticate with LDAP user, positive case.
     */
    @Test
    public void testAuthenticateUser() {
        SecurityGroupDTO securityGroup = new SecurityGroupDTO(ADMIN_GROUP, "", ADMINISTRATOR);
        ssoUtil.authenticateADUser(USER_NAME, PASSWORD);
    }

    /**
     * Authenticate with LDAP user, negative case.
     * Fill in with wrong username password combination.
     */
    @Test(expected = SecurityException.class)
    public void testAuthenticateUserNegative() {
        SecurityGroupDTO securityGroup = new SecurityGroupDTO(ADMIN_GROUP, "", ADMINISTRATOR);
        ssoUtil.authenticateADUser(USER_NAME, WRONG_PASSWORD);
    }
}