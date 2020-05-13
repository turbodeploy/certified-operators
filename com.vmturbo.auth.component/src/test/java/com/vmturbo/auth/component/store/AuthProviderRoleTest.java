package com.vmturbo.auth.component.store;

import static com.vmturbo.auth.api.authorization.jwt.SecurityConstant.ADMINISTRATOR;
import static com.vmturbo.auth.component.store.AuthProviderBase.PREFIX;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import com.google.common.collect.ImmutableList;

import org.assertj.core.util.Lists;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
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
    @Mock
    Supplier<String> keyValueDir;
    @InjectMocks
    AuthProvider authProviderUnderTest;
    @Mock
    private KeyValueStore mockKeystore;

    /**
     * Set up the Spring context with a security context.
     */
    @Before
    public void setup() {

        MockitoAnnotations.initMocks(this);
    }

    /**
     * Administrators may create a new user with ADMINISTRATOR role.
     */
    @WithMockUser(roles = "ADMINISTRATOR")
    @Test
    public void testAddAdminFromAdmin() {
        // arrange
        when(mockKeystore.get(anyString())).thenReturn(Optional.empty());
        when(keyValueDir.get()).thenReturn("/");
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
        when(keyValueDir.get()).thenReturn("/");
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
        when(keyValueDir.get()).thenReturn("/");
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
        assertTrue(authProviderUnderTest.mayAlterUserWithRoles(ImmutableList.of(ADMINISTRATOR)));
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
        assertFalse(authProviderUnderTest.mayAlterUserWithRoles(
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
        assertTrue(authProviderUnderTest.mayAlterUserWithRoles(ImmutableList.of(OBSERVER)));
        SecurityContextHolder.getContext().setAuthentication(null);
    }

    /**
     * Not login user doesn't allow any operation
     */
    @Test
    public void testMayAlterUserWithRolesSiteAdminNotLogin() {
        assertFalse(authProviderUnderTest.mayAlterUserWithRoles(ImmutableList.of(OBSERVER)));
    }

    /**
     * Admin user can change anyone's password
     */
    @Test
    public void testChangePasswordAllowedForAdminUser() {
        SecurityContextHolder.getContext()
                .setAuthentication(getAuthentication(ROLE_ADMINISTRATOR, ADMIN));
        // itself
        assertTrue(authProviderUnderTest.changePasswordAllowed(ADMIN));
        // other admin
        assertTrue(authProviderUnderTest.changePasswordAllowed("anotherAdmin"));
        SecurityContextHolder.getContext().setAuthentication(null);
    }

    /**
     * site admin can change password for itself and non-admin user
     */
    @Test
    public void testChangePasswordAllowedForSiteAdminUser() {
        SecurityContextHolder.getContext()
                .setAuthentication(getAuthentication(ROLE_SITE_ADMIN, ADMIN));
        assertTrue(authProviderUnderTest.changePasswordAllowed(ADMIN));
        assertFalse(authProviderUnderTest.changePasswordAllowed(OBSERVER));
        SecurityContextHolder.getContext().setAuthentication(null);
    }

    /**
     * Non admin user can only change itself's password
     */
    @Test
    public void testChangePasswordAllowedNonAdminChangeItself() {
        SecurityContextHolder.getContext()
                .setAuthentication(getAuthentication(ROLE_OBSERVER, OBSERVER));
        assertFalse(authProviderUnderTest.changePasswordAllowed(ADMIN));
        assertTrue(authProviderUnderTest.changePasswordAllowed(OBSERVER));
        SecurityContextHolder.getContext().setAuthentication(null);
    }

    /**
     * Not login user cannot change any password.
     */
    @Test
    public void testChangePasswordAllowedNotLogin() {
        assertFalse(authProviderUnderTest.changePasswordAllowed(ADMIN));
        assertFalse(authProviderUnderTest.changePasswordAllowed(OBSERVER));
    }
}