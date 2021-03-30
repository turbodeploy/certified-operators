package com.vmturbo.auth.component.store;

import static com.vmturbo.auth.api.authorization.jwt.SecurityConstant.ADMINISTRATOR;
import static com.vmturbo.auth.api.authorization.jwt.SecurityConstant.OPERATIONAL_OBSERVER;
import static com.vmturbo.auth.component.store.AuthProviderBase.PREFIX;
import static com.vmturbo.auth.component.store.AuthProviderHelper.changePasswordAllowed;
import static com.vmturbo.auth.component.store.AuthProviderHelper.mayAlterUserWithRoles;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import com.google.common.collect.ImmutableList;

import com.vmturbo.auth.api.authorization.keyprovider.IKeyImportIndicator;
import org.assertj.core.util.Lists;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;

import com.vmturbo.auth.api.usermgmt.AuthUserDTO;
import com.vmturbo.auth.api.usermgmt.AuthUserDTO.PROVIDER;
import com.vmturbo.auth.api.usermgmt.SecurityGroupDTO;
import com.vmturbo.auth.component.licensing.LicenseCheckService;
import com.vmturbo.auth.component.policy.ReportPolicy;
import com.vmturbo.auth.component.policy.UserPolicy;
import com.vmturbo.auth.component.policy.UserPolicy.LoginPolicy;
import com.vmturbo.auth.component.store.AuthProviderBase.UserInfo;
import com.vmturbo.auth.component.store.sso.SsoUtil;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.kvstore.KeyValueStore;

/**
 * Test the rule to create new Administrator users.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@TestPropertySource(properties = {"identityGeneratorPrefix=1"})
@WebAppConfiguration
public class AuthProviderRoleTest {

    private static final String ROLE_SITE_ADMIN = "ROLE_SITE_ADMIN";
    private static final String ROLE_ADMINISTRATOR = "ROLE_ADMINISTRATOR";
    private static final String ROLE_OBSERVER = "ROLE_OBSERVER";
    private static final String OBSERVER = "OBSERVER";
    private static final String ADMIN = "admin";
    private static final String SITE_ADMIN = "siteadmin";

    private KeyValueStore mockKeystore;
    private LicenseCheckService licenseCheckService;

    AuthProvider authProviderUnderTest;

    private static final IKeyImportIndicator keyImportIndicator = () -> false;

    /**
     * Set up the Spring context with a security context.
     */
    @Before
    public void setup() {
        mockKeystore = mock(KeyValueStore.class);
        licenseCheckService = mock(LicenseCheckService.class);
        Supplier<String> keyValueDir = () -> "/";
        authProviderUnderTest = new AuthProvider(mockKeystore, null, keyValueDir, null, new UserPolicy(LoginPolicy.ALL,
                new ReportPolicy(licenseCheckService)),
                new SsoUtil(), false, false, null);
    }

    /**
     * Administrators may create a new user with ADMINISTRATOR role.
     */
    @WithMockUser(roles = "ADMINISTRATOR")
    @Test
    public void testAddAdminFromAdmin() {
        // arrange
        when(mockKeystore.get(anyString())).thenReturn(Optional.empty());
        // act
        authProviderUnderTest.add(PROVIDER.LOCAL, "test-user", "test-password",
                Lists.newArrayList("ADMINISTRATOR"), null);
        // assert

        final ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
        final ArgumentCaptor<String> jsonCaptor = ArgumentCaptor.forClass(String.class);
        verify(mockKeystore).put(keyCaptor.capture(), jsonCaptor.capture());
        assertThat(keyCaptor.getAllValues().size(), equalTo(1));
        assertThat(keyCaptor.getValue(), equalTo("users/LOCAL/TEST-USER"));
        assertThat(jsonCaptor.getValue(), containsString("ADMINISTRATOR"));
    }

    /**
     * SITE_ADMINs may *not* create a new user with ADMINISTRATOR role.
     */
    @WithMockUser(roles = "SITE_ADMIN")
    @Test
    public void testAddAdminFromSiteAdmin() {
        // arrange
        when(mockKeystore.get(anyString())).thenReturn(Optional.empty());
        // act
        try {
            authProviderUnderTest.add(PROVIDER.LOCAL, "test-user", "test-password",
                    Lists.newArrayList("ADMINISTRATOR"), null);
            fail("Security exception should have been thrown");
        } catch (SecurityException e) {
            assertThat(e.getMessage(), containsString("Only a user with ADMINISTRATOR role user " +
                    "may create another user with ADMINISTRATOR role."));
        }
    }

    /**
     * site admin may list users.
     */
    @WithMockUser(roles = "SITE_ADMIN")
    @Test
    public void testListAdminFromAdmin() {
        // arrange
        when(mockKeystore.get(anyString())).thenReturn(Optional.empty());
        // act
        authProviderUnderTest.list();
        // assert
        verify(mockKeystore).getByPrefix(PREFIX);
    }

    /**
     * Admin user can alter administrator role.
     */
    @Test
    public void testMayAlterUserWithRolesAdministrator() {
        SecurityContextHolder.getContext()
                .setAuthentication(getAuthentication(ROLE_ADMINISTRATOR, ADMIN));
        assertTrue(mayAlterUserWithRoles(ImmutableList.of(ADMINISTRATOR)));
        SecurityContextHolder.getContext().setAuthentication(null);
    }

    /**
     * Helper test method to setup security context.
     *
     * @param role user assgined role.
     * @param user user name.
     * @return {@link Authentication}.
     */
    public static Authentication getAuthentication(String role, String user) {
        // Setup caller authentication
        Set<GrantedAuthority> grantedAuths = new HashSet<>();
        grantedAuths.add(new SimpleGrantedAuthority(role));
        AuthUserDTO authUserDTO = new AuthUserDTO(user, "9527", Collections.singletonList(role));
        return new UsernamePasswordAuthenticationToken(authUserDTO, "***", grantedAuths);
    }

    /**
     * site admin cannot alter administrator role.
     */
    @Test
    public void testMayAlterUserWithRolesSiteAdminWithAdminRole() {
        SecurityContextHolder.getContext()
                .setAuthentication(getAuthentication(ROLE_SITE_ADMIN, ADMIN));
        assertFalse(mayAlterUserWithRoles(
                ImmutableList.of(ADMINISTRATOR.toUpperCase())));
        SecurityContextHolder.getContext().setAuthentication(null);
    }

    /**
     * site admin can alter non-administrator role.
     */
    @Test
    public void testMayAlterUserWithRolesSiteAdminWithNonAdminRole() {
        SecurityContextHolder.getContext()
                .setAuthentication(getAuthentication(ROLE_SITE_ADMIN, ADMIN));
        assertTrue(mayAlterUserWithRoles(ImmutableList.of(OBSERVER)));
        SecurityContextHolder.getContext().setAuthentication(null);
    }

    /**
     * Not login user doesn't allow any operation
     */
    @Test
    public void testMayAlterUserWithRolesSiteAdminNotLogin() {
        assertFalse(mayAlterUserWithRoles(ImmutableList.of(OBSERVER)));
    }

    /**
     * Admin user can change anyone's password
     */
    @Test
    public void testChangePasswordAllowedForAdminUser() {
        SecurityContextHolder.getContext()
                .setAuthentication(getAuthentication(ROLE_ADMINISTRATOR, ADMIN));
        // itself
        assertTrue(changePasswordAllowed(buildUser(ADMINISTRATOR, Optional.empty())));
        // other admin
        assertTrue(changePasswordAllowed(buildUser(ADMINISTRATOR, Optional.of("anotherAdmin"))));
        SecurityContextHolder.getContext().setAuthentication(null);
    }

    /**
     * site admin can change password for itself and non-admin user
     */
    @Test
    public void testChangePasswordAllowedForSiteAdminUser() {
        SecurityContextHolder.getContext()
                .setAuthentication(getAuthentication(ROLE_SITE_ADMIN, SITE_ADMIN));
        assertTrue(changePasswordAllowed(buildUser(OBSERVER, Optional.of("observerUser"))));
        assertFalse(changePasswordAllowed(buildUser(ADMINISTRATOR, Optional.empty())));
        SecurityContextHolder.getContext().setAuthentication(null);
    }

    /**
     * Non admin user can only change itself's password
     */
    @Test
    public void testChangePasswordAllowedNonAdminChangeItself() {
        SecurityContextHolder.getContext()
                .setAuthentication(getAuthentication(ROLE_OBSERVER, OBSERVER));
        assertFalse(changePasswordAllowed(buildUser(ADMINISTRATOR, Optional.empty())));
        assertTrue(changePasswordAllowed(buildUser(OBSERVER, Optional.of(OBSERVER))));
        SecurityContextHolder.getContext().setAuthentication(null);
    }

    /**
     * Not login user cannot change any password.
     */
    @Test
    public void testChangePasswordAllowedNotLogin() {
        assertFalse(changePasswordAllowed(buildUser(ADMINISTRATOR, Optional.empty())));
        assertFalse(changePasswordAllowed(buildUser(OBSERVER, Optional.of(OBSERVER))));
    }

    /**
     * Combine scopes.
     * 1. if one group is NOT scoped, the user will NOT be scoped.
     * 2. if all groups are scoped, the user will have all the scopes.
     */
    @Test
    public void testCombinedScopes() {
        SecurityGroupDTO securityGroup =
                new SecurityGroupDTO("ADMIN_GROUP", "", OBSERVER, Lists.newArrayList(1L, 3L));
        SecurityGroupDTO securityGroup1 =
                new SecurityGroupDTO("ADMIN_GROUP", "", OBSERVER, Lists.newArrayList(2L));
        SecurityGroupDTO securityGroup2 =
                new SecurityGroupDTO("ADMIN_GROUP", "", OBSERVER, Collections.emptyList());
        SecurityGroupDTO securityGroup3 =
                new SecurityGroupDTO("ADMIN_GROUP", "", OBSERVER, Lists.newArrayList(4L));
        SecurityGroupDTO securityGroup4 =
                new SecurityGroupDTO("ADMIN_GROUP", "", OPERATIONAL_OBSERVER, Lists.newArrayList(4L));
        // #1
        assertEquals(0, authProviderUnderTest.combineScopes(ImmutableList.of(securityGroup, securityGroup1, securityGroup2)).size());
        // #2
        assertThat(authProviderUnderTest.combineScopes(ImmutableList.of(securityGroup, securityGroup1, securityGroup3)), hasItems(1L, 2L, 3L, 4L));
    }

    // Helper to build UserInfo.
    private static UserInfo buildUser(String role, Optional<String> username) {
        UserInfo info = new UserInfo();
        info.provider = PROVIDER.LOCAL;
        info.userName = username.orElse(ADMIN);

        info.passwordHash = HashAuthUtils.secureHash("password");
        info.uuid = String.valueOf(IdentityGenerator.next());
        info.unlocked = true;
        // ensure role are upper case for any I/O operations, here is saving to Consul.
        info.roles = Collections.singletonList(role);
        info.scopeGroups = null;
        return info;
    }
}
