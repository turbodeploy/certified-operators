package com.vmturbo.api.component.external.api.util;

import static org.hamcrest.beans.SamePropertyValuesAs.samePropertyValuesAs;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;

import org.junit.Test;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;

import com.vmturbo.auth.api.authorization.jwt.JwtCallCredential;
import com.vmturbo.auth.api.usermgmt.AuthUserDTO;
import com.vmturbo.auth.api.usermgmt.AuthUserDTO.PROVIDER;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;

/**
 * Test {@link ApiUtils#generateJWTCallCredential()} and {@link ApiUtils#getClientIp(HttpServletRequest)}
 *
 */
public class ApiUtilsTest {
    private static final String TEST_TOKEN = "test token";
    private static final String _10_0_0_200 = "10.0.0.200";
    private static final String _10_0_0_1 = "10.0.0.1";
    private static final String X_FORWARDED_FOR = "X-FORWARDED-FOR";
    private final JwtCallCredential jwtCallCredential = new JwtCallCredential(TEST_TOKEN);

    @Test
    public void testGenerateJWTCallCredential() throws Exception {
        // setup authentication object
        Set<GrantedAuthority> grantedAuths = new HashSet<>();
        grantedAuths.add(new SimpleGrantedAuthority("ROLE_NONADMINISTRATOR"));
        AuthUserDTO user = new AuthUserDTO(PROVIDER.LOCAL, "admin", null, "testUUID",
                TEST_TOKEN, new ArrayList<>());
        Authentication authentication = new UsernamePasswordAuthenticationToken(user, "***", grantedAuths);

        // populate security context
        SecurityContextHolder.getContext().setAuthentication(authentication);

        // verify the JWTCallCredential is returned
        assertTrue(ApiUtils.generateJWTCallCredential().isPresent());
        JwtCallCredential returnJwtCallCredential = ApiUtils.generateJWTCallCredential().get();

        // verify the JWTCallCredential has the same JWT token as before
        assertThat(returnJwtCallCredential, samePropertyValuesAs(jwtCallCredential));
    }

    @Test
    public void testGetClientIpWithRemoteAddr() {
        HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getHeader(X_FORWARDED_FOR)).thenReturn("");
        when(request.getRemoteAddr()).thenReturn(_10_0_0_1);
        Optional<String> ipAddress = ApiUtils.getClientIp(request);
        assertEquals("IP address should be available", _10_0_0_1, ipAddress.get());
    }

    @Test
    public void testGetClientIpWithForwardHeader() {
        HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getHeader(X_FORWARDED_FOR)).thenReturn(_10_0_0_1);
        when(request.getRemoteAddr()).thenReturn(_10_0_0_200);
        Optional<String> ipAddress = ApiUtils.getClientIp(request);
        assertEquals("IP address should be available", _10_0_0_1, ipAddress.get());
    }

    public static ActionEntity createActionEntity(long id) {
        // set some fake type for now
        final int defaultEntityType = 1;
        return ActionEntity.newBuilder()
                    .setId(id)
                    .setType(defaultEntityType)
                    .build();
    }

    public static ActionEntity createActionEntity(long id, int type) {
        return ActionEntity.newBuilder()
                    .setId(id)
                    .setType(type)
                    .build();
    }
}
