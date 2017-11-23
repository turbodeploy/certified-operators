package com.vmturbo.api.component.external.api.util;

import static org.hamcrest.beans.SamePropertyValuesAs.samePropertyValuesAs;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import org.junit.Test;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;

import com.vmturbo.auth.api.authorization.jwt.JwtCallCredential;
import com.vmturbo.auth.api.usermgmt.AuthUserDTO;
import com.vmturbo.auth.api.usermgmt.AuthUserDTO.PROVIDER;

/**
 * Test {@link ApiUtils#generateJWTCallCredential()}
 */
public class ApiUtilsTest {
    public static final String TEST_TOKEN = "test token";
    JwtCallCredential jwtCallCredential = new JwtCallCredential(TEST_TOKEN);

    //
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
}