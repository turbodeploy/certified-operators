package com.vmturbo.api.component.security;

import static com.vmturbo.api.component.security.IntersightIdTokenVerifierTest.PUBLIC_KEY_WITH_PREFIX_SUBFIX_ES256;
import static com.vmturbo.api.component.security.IntersightIdTokenVerifierTest.PUBLIC_KEY_WITH_PREFIX_SUFFIX;
import static com.vmturbo.auth.api.authorization.jwt.SecurityConstant.X_TURBO_ROLE;
import static com.vmturbo.auth.api.authorization.jwt.SecurityConstant.X_TURBO_TOKEN;
import static com.vmturbo.auth.api.authorization.jwt.SecurityConstant.X_TURBO_USER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.common.collect.ImmutableMap;

import org.junit.Before;
import org.junit.Test;
import org.springframework.security.core.context.SecurityContextHolder;

import com.vmturbo.api.component.communication.HeaderAuthenticationToken;

/**
 * Verified {@link HeaderAuthenticationFilter}.
 */
public class HeaderAuthenticationFilterTest {

    private static final String USER_1 = "user1";
    private static final String ROLE_1 = "Server Administrator";
    private static final String GROUP_1 = "CWOM_ADMINISTRATOR";
    private static final String TOKEN_1 = "TOKEN1";
    private static final Map<String, String> INTERSIGHT_TO_COWM_ROLE_MAPPING =
            ImmutableMap.<String, String>builder().put("Server Administrator", "CWOM_ADMINISTRATOR")
                    .put("Account Administrator", "CWOM_ADMINISTRATOR")
                    .put("Read-Only", "CWOM_OBSERVER")
                    .put("Device Technician", "CWOM_ADMINISTRATOR")
                    .put("HyperFlex Cluster Administrator", "CWOM_ADMINISTRATOR")
                    .put("Device Administrator", "CWOM_ADMINISTRATOR")
                    .put("User Access Administrator", "CWOM_ADMINISTRATOR")
                    .build();
    private static final String X_STARSHIP_AUTH_TOKEN = "X-Starship-Auth-Token";
    private static final String X_STARSHIP_AUTH_TOKEN_PUBLIC_KEY =
            "X-Starship-Auth-Token-PublicKey";
    private HttpServletRequest request;
    private HttpServletResponse response;
    private HeaderAuthenticationFilter filter;
    private FilterChain chain;

    /**
     * Before methods.
     */
    @Before
    public void setup() {
        SecurityContextHolder.clearContext();
        request = mock(HttpServletRequest.class);
        response = mock(HttpServletResponse.class);
        filter = new HeaderAuthenticationFilter(
                new IntersightHeaderMapper(INTERSIGHT_TO_COWM_ROLE_MAPPING, X_TURBO_USER,
                        X_TURBO_ROLE, X_STARSHIP_AUTH_TOKEN_PUBLIC_KEY, X_STARSHIP_AUTH_TOKEN),
                Optional.of(PUBLIC_KEY_WITH_PREFIX_SUBFIX_ES256), true);
        chain = mock(FilterChain.class);
    }

    /**
     * Verify method will properly create authentication token with user and group only.
     *
     * @throws ServletException should not throw
     * @throws IOException should not throw
     */
    @Test
    public void testDoFilterInternalUserAndGroup() throws ServletException, IOException {
        when(request.getHeader(X_TURBO_USER)).thenReturn(USER_1);
        when(request.getHeader(X_TURBO_ROLE)).thenReturn(ROLE_1);

        // Don't pass in public key.
        filter = new HeaderAuthenticationFilter(
                new IntersightHeaderMapper(INTERSIGHT_TO_COWM_ROLE_MAPPING, X_TURBO_USER,
                        X_TURBO_ROLE, X_STARSHIP_AUTH_TOKEN_PUBLIC_KEY, X_STARSHIP_AUTH_TOKEN),
                Optional.empty(), true);
        filter.doFilterInternal(request, response, chain);
        assertTrue(SecurityContextHolder.getContext()
                .getAuthentication() instanceof HeaderAuthenticationToken);
        HeaderAuthenticationToken authentication =
                (HeaderAuthenticationToken)SecurityContextHolder.getContext().getAuthentication();
        assertEquals(USER_1, authentication.getUserName());
        assertEquals(GROUP_1, authentication.getGroup());
        assertFalse(authentication.getJwtToken().isPresent());
        // ensure this user doesn't have permission
        assertTrue(authentication.getAuthorities().isEmpty());
    }

    /**
     * Verify method will properly create authentication token with JWT only. When the public key
     * is injected when starting up.
     *
     * @throws ServletException should not throw
     * @throws IOException should not throw
     */
    @Test
    public void testDoFilterInternalJwtWithKeyFromStartup() throws ServletException, IOException {
        when(request.getHeader(X_TURBO_TOKEN)).thenReturn(TOKEN_1);
        when(request.getHeader(X_STARSHIP_AUTH_TOKEN)).thenReturn(TOKEN_1);
        filter = new HeaderAuthenticationFilter(
                new IntersightHeaderMapper(INTERSIGHT_TO_COWM_ROLE_MAPPING, X_TURBO_USER,
                        X_TURBO_ROLE, X_STARSHIP_AUTH_TOKEN_PUBLIC_KEY, X_STARSHIP_AUTH_TOKEN),
                Optional.of(PUBLIC_KEY_WITH_PREFIX_SUFFIX), false);
        filter.doFilterInternal(request, response, chain);
        assertTrue(SecurityContextHolder.getContext()
                .getAuthentication() instanceof HeaderAuthenticationToken);
        HeaderAuthenticationToken authentication =
                (HeaderAuthenticationToken)SecurityContextHolder.getContext().getAuthentication();
        assertEquals(TOKEN_1, authentication.getJwtToken().get());
        assertNull(authentication.getGroup());
        assertEquals("anonymous", authentication.getUserName());
        // ensure this user doesn't have permission
        assertTrue(authentication.getAuthorities().isEmpty());
    }

    /**
     * Verify method will properly create authentication token with JWT only. When the public key
     * is injected when starting up.
     *
     * @throws ServletException should not throw
     * @throws IOException should not throw
     */
    @Test
    public void testDoFilterInternalJwtWithKeyFromStartupLatest() throws ServletException, IOException {
        when(request.getHeader(X_TURBO_TOKEN)).thenReturn(TOKEN_1);
        when(request.getHeader(X_STARSHIP_AUTH_TOKEN)).thenReturn(TOKEN_1);
        filter.doFilterInternal(request, response, chain);
        assertTrue(SecurityContextHolder.getContext()
                .getAuthentication() instanceof HeaderAuthenticationToken);
        HeaderAuthenticationToken authentication =
                (HeaderAuthenticationToken)SecurityContextHolder.getContext().getAuthentication();
        assertEquals(TOKEN_1, authentication.getJwtToken().get());
        assertNull(authentication.getGroup());
        assertEquals("anonymous", authentication.getUserName());
        // ensure this user doesn't have permission
        assertTrue(authentication.getAuthorities().isEmpty());
    }

    /**
     * Verify method will properly create authentication token with JWT only. When the public key
     * is passed in along with request.
     *
     * @throws ServletException should not throw
     * @throws IOException should not throw
     */
    @Test
    public void testDoFilterInternalJwtWithKeyFromRequestLatest() throws ServletException, IOException {
        when(request.getHeader(X_TURBO_TOKEN)).thenReturn(TOKEN_1);
        when(request.getHeader(X_STARSHIP_AUTH_TOKEN)).thenReturn(TOKEN_1);

        when(request.getHeader("X-Starship-Auth-Token-PublicKey")).thenReturn(PUBLIC_KEY_WITH_PREFIX_SUBFIX_ES256);
        filter = new HeaderAuthenticationFilter(
                new IntersightHeaderMapper(INTERSIGHT_TO_COWM_ROLE_MAPPING, X_TURBO_USER,
                        X_TURBO_ROLE, X_STARSHIP_AUTH_TOKEN_PUBLIC_KEY, X_STARSHIP_AUTH_TOKEN),
                Optional.empty(), true);
        filter.doFilterInternal(request, response, chain);
        assertTrue(SecurityContextHolder.getContext()
                .getAuthentication() instanceof HeaderAuthenticationToken);
        HeaderAuthenticationToken authentication =
                (HeaderAuthenticationToken)SecurityContextHolder.getContext().getAuthentication();
        assertEquals(TOKEN_1, authentication.getJwtToken().get());
        assertNull(authentication.getGroup());
        assertEquals("anonymous", authentication.getUserName());
        // ensure this user doesn't have permission
        assertTrue(authentication.getAuthorities().isEmpty());
    }

    /**
     * Verify method will properly create authentication token with JWT only. When the public key
     * is passed in along with request. This is to test previous version behavior.
     *
     * @throws ServletException should not throw
     * @throws IOException should not throw
     */
    @Test
    public void testDoFilterInternalJwtWithKeyFromRequest() throws ServletException, IOException {
        when(request.getHeader(X_TURBO_TOKEN)).thenReturn(TOKEN_1);
        when(request.getHeader(X_STARSHIP_AUTH_TOKEN)).thenReturn(TOKEN_1);

        when(request.getHeader("X-Starship-Auth-Token-PublicKey")).thenReturn(PUBLIC_KEY_WITH_PREFIX_SUFFIX);
        filter = new HeaderAuthenticationFilter(
                new IntersightHeaderMapper(INTERSIGHT_TO_COWM_ROLE_MAPPING, X_TURBO_USER,
                        X_TURBO_ROLE, X_STARSHIP_AUTH_TOKEN_PUBLIC_KEY, X_STARSHIP_AUTH_TOKEN),
                Optional.empty(), false);
        filter.doFilterInternal(request, response, chain);
        assertTrue(SecurityContextHolder.getContext()
                .getAuthentication() instanceof HeaderAuthenticationToken);
        HeaderAuthenticationToken authentication =
                (HeaderAuthenticationToken)SecurityContextHolder.getContext().getAuthentication();
        assertEquals(TOKEN_1, authentication.getJwtToken().get());
        assertNull(authentication.getGroup());
        assertEquals("anonymous", authentication.getUserName());
        // ensure this user doesn't have permission
        assertTrue(authentication.getAuthorities().isEmpty());
    }

    /**
     * Verify method will properly create authentication token with JWT only (take precedent)
     * when both JWT, user and group are in the requests.
     *
     * @throws ServletException should not throw
     * @throws IOException should not throw
     */
    @Test
    public void testDoFilterInternalJwtAndUserGroup() throws ServletException, IOException {
        when(request.getHeader(X_TURBO_USER)).thenReturn(USER_1);
        when(request.getHeader(X_TURBO_ROLE)).thenReturn(ROLE_1);
        // when(request.getHeader(X_TURBO_TOKEN)).thenReturn(TOKEN_1);
        when(request.getHeader(X_STARSHIP_AUTH_TOKEN)).thenReturn(TOKEN_1);
        final FilterChain filterChain = mock(FilterChain.class);
        filter.doFilterInternal(request, response, filterChain);
        assertTrue(SecurityContextHolder.getContext()
                .getAuthentication() instanceof HeaderAuthenticationToken);
        HeaderAuthenticationToken authentication =
                (HeaderAuthenticationToken)SecurityContextHolder.getContext().getAuthentication();
        assertEquals(TOKEN_1, authentication.getJwtToken().get());
        assertNull(authentication.getGroup());
        assertEquals("anonymous", authentication.getUserName());
        // ensure this user doesn't have permission
        assertTrue(authentication.getAuthorities().isEmpty());
    }

    /**
     * Verify method will NOT create authentication token when no header is passed.
     *
     * @throws ServletException should not throw
     * @throws IOException should not throw
     */
    @Test
    public void testDoFilterInternalEmpty() throws ServletException, IOException {
        final FilterChain filterChain = mock(FilterChain.class);
        filter.doFilterInternal(request, response, filterChain);
        assertNull(SecurityContextHolder.getContext().getAuthentication());
    }

    /**
     * Verify method will NOT create authentication token when only user passed in the header.
     *
     * @throws ServletException should not throw
     * @throws IOException should not throw
     */
    @Test
    public void testDoFilterInternalUserOnly() throws ServletException, IOException {
        when(request.getHeader(X_TURBO_USER)).thenReturn(USER_1);
        final FilterChain filterChain = mock(FilterChain.class);
        filter.doFilterInternal(request, response, filterChain);
        assertNull(SecurityContextHolder.getContext().getAuthentication());
    }

    /**
     * Verify method will NOT create authentication token when only group passed in the header.
     *
     * @throws ServletException should not throw
     * @throws IOException should not throw
     */
    @Test
    public void testDoFilterInternalGroupOnly() throws ServletException, IOException {
        when(request.getHeader(X_TURBO_ROLE)).thenReturn(ROLE_1);
        final FilterChain filterChain = mock(FilterChain.class);
        filter.doFilterInternal(request, response, filterChain);
        assertNull(SecurityContextHolder.getContext().getAuthentication());
    }
}