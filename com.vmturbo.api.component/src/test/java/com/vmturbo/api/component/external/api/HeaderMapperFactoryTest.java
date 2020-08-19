package com.vmturbo.api.component.external.api;

import static com.vmturbo.auth.api.authorization.jwt.SecurityConstant.ADMINISTRATOR;
import static com.vmturbo.auth.api.authorization.jwt.SecurityConstant.ADVISOR;
import static com.vmturbo.auth.api.authorization.jwt.SecurityConstant.AUTOMATOR;
import static com.vmturbo.auth.api.authorization.jwt.SecurityConstant.DEPLOYER;
import static com.vmturbo.auth.api.authorization.jwt.SecurityConstant.OBSERVER;
import static com.vmturbo.auth.api.authorization.jwt.SecurityConstant.SITE_ADMIN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.Collections;
import java.util.Optional;

import com.google.common.collect.ImmutableMap;

import org.junit.Test;

import com.vmturbo.api.component.security.HeaderMapper;
import com.vmturbo.auth.api.authorization.jwt.SecurityConstant.PredefinedRole;

/**
 * Test {@link HeaderMapperFactory}.
 */
public class HeaderMapperFactoryTest {

    private static final String ACCOUNT = "x-barracuda-account";
    private static final String ROLE = "x-barracuda-roles";
    private static final String PUBLIC_KEY = "PublicKey";
    private static final String TOKEN = "X-Starship-Auth-Token";

    private static final String ADMIN_ROLES =
            "Server Administrator,Account Administrator,Device Administrator,System Administrator";

    private static final String SITE_ADMIN_ROLES = "Site admin";

    // injected spaces between roles
    private static final String AUTOMATOR_ROLES = "Automator,  IWO_Automator";

    // inject space at front
    private static final String DEPLOYER_ROLE = " Deployer,   IWO_Deployer";

    private static final String ADVISOR_ROLE = "Advisor";

    private static final String READ_ONLY_ROLE = "Read-Only";

    /**
     * Test happy path.
     */
    @Test
    public void testGetHeaderMapper() {
        final ImmutableMap<PredefinedRole, String> roleMap =
                ImmutableMap.<PredefinedRole, String>builder().put(PredefinedRole.ADMINISTRATOR,
                        ADMIN_ROLES)
                        .put(PredefinedRole.SITE_ADMIN, SITE_ADMIN_ROLES)
                        .put(PredefinedRole.AUTOMATOR, AUTOMATOR_ROLES)
                        .put(PredefinedRole.DEPLOYER, DEPLOYER_ROLE)
                        .put(PredefinedRole.ADVISOR, ADVISOR_ROLE)
                        .put(PredefinedRole.OBSERVER, READ_ONLY_ROLE)
                        .build();
        final HeaderMapper headerMapper =
                HeaderMapperFactory.getHeaderMapper(Optional.empty(), roleMap, ACCOUNT, ROLE,
                        PUBLIC_KEY, TOKEN);
        assertEquals(ACCOUNT, headerMapper.getUserName());
        assertEquals(ROLE, headerMapper.getRole());
        assertEquals(TOKEN, headerMapper.getJwtTokenTag());
        assertEquals(PUBLIC_KEY, headerMapper.getJwtTokenPublicKeyTag());

        assertEquals(ADMINISTRATOR, headerMapper.getAuthGroup("Server Administrator"));
        assertEquals(ADMINISTRATOR, headerMapper.getAuthGroup("Account Administrator"));
        assertEquals(ADMINISTRATOR, headerMapper.getAuthGroup("Device Administrator"));
        assertEquals(ADMINISTRATOR, headerMapper.getAuthGroup("System Administrator"));
        assertEquals(SITE_ADMIN, headerMapper.getAuthGroup("Site admin"));
        assertEquals(AUTOMATOR, headerMapper.getAuthGroup("Automator"));
        assertEquals(AUTOMATOR, headerMapper.getAuthGroup("IWO_Automator"));

        assertEquals(DEPLOYER, headerMapper.getAuthGroup("Deployer"));
        assertEquals(DEPLOYER, headerMapper.getAuthGroup("IWO_Deployer"));
        assertEquals(ADVISOR, headerMapper.getAuthGroup("Advisor"));
        assertEquals(OBSERVER, headerMapper.getAuthGroup("Read-Only"));
    }

    /**
     * Test injected empty mapping properties.
     */
    @Test
    public void testGetHeaderMapperNegative() {
        final HeaderMapper headerMapper =
                HeaderMapperFactory.getHeaderMapper(Optional.empty(), Collections.emptyMap(), "",
                        "", "", "");
        assertEquals("", headerMapper.getUserName());
        assertEquals("", headerMapper.getRole());
        assertEquals("", headerMapper.getJwtTokenTag());
        assertEquals("", headerMapper.getJwtTokenPublicKeyTag());

        assertNull(headerMapper.getAuthGroup("Server Administrator"));
        assertNull(headerMapper.getAuthGroup("Account Administrator"));
        assertNull(headerMapper.getAuthGroup("Device Administrator"));
        assertNull(headerMapper.getAuthGroup("System Administrator"));
        assertNull(headerMapper.getAuthGroup("Site admin"));
        assertNull(headerMapper.getAuthGroup("Automator"));
        assertNull(headerMapper.getAuthGroup("IWO_Automator"));

        assertNull(headerMapper.getAuthGroup("Deployer"));
        assertNull(headerMapper.getAuthGroup("IWO_Deployer"));
        assertNull(headerMapper.getAuthGroup("Advisor"));
        assertNull(headerMapper.getAuthGroup("Read-Only"));
    }
}