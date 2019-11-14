package com.vmturbo.api.component.communication;

import static com.vmturbo.api.component.external.api.service.UsersService.HTTP_ACCEPT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Optional;

import com.google.common.collect.Lists;

import org.apache.commons.lang.StringUtils;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.web.client.RestTemplate;

import com.vmturbo.auth.api.authorization.AuthorizationException;
import com.vmturbo.auth.api.authorization.jwt.JWTAuthorizationToken;
import com.vmturbo.auth.api.authorization.jwt.JWTAuthorizationVerifier;
import com.vmturbo.auth.api.authorization.jwt.SecurityConstant;
import com.vmturbo.auth.api.authorization.kvstore.IComponentJwtStore;
import com.vmturbo.auth.api.usermgmt.AuthUserDTO;

/**
 * Verified {@link HeaderAuthenticationProvider}.
 */
public class HeaderAuthenticationProviderTest {

    private static final String GROUP_1 = "group1";
    private static final String USER_1 = "user1";
    private static final Optional<String> TOKEN_1 = Optional.of("token1");
    private static final String JWTTOKEN = "jwttoken";
    private static final String ADMINISTRATOR = "ADMINISTRATOR";
    private static final String IP_ADDRESS = "127.0.0.1";
    private HeaderAuthenticationProvider provider;
    private HeaderAuthenticationToken authentication;
    private ResponseEntity<String> result = Mockito.mock(ResponseEntity.class);
    private JWTAuthorizationToken jwtAuthorizationToken;

    /**
     * Before tests.
     *
     * @throws AuthorizationException when authorization failed
     */
    @Before
    public void setup() throws AuthorizationException {
        final IComponentJwtStore componentJwtStore = mock(IComponentJwtStore.class);
        final JWTAuthorizationVerifier verifier = mock(JWTAuthorizationVerifier.class);
        when(verifier.verify(any(), any())).thenReturn(
                new AuthUserDTO(USER_1, "uuid", Lists.newArrayList(ADMINISTRATOR)));
        jwtAuthorizationToken = mock(JWTAuthorizationToken.class);
        when(componentJwtStore.generateToken()).thenReturn(jwtAuthorizationToken);
        when(jwtAuthorizationToken.getCompactRepresentation()).thenReturn(JWTTOKEN);
        final RestTemplate restTemplate = mock(RestTemplate.class);
        HttpHeaders headers = new HttpHeaders();
        headers.setAccept(HTTP_ACCEPT);
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.set(RestAuthenticationProvider.AUTH_HEADER_NAME, JWTTOKEN);
        headers.set(SecurityConstant.COMPONENT_ATTRIBUTE, null);
        HttpEntity<List> entity = new HttpEntity<>(headers);
        when(result.getBody()).thenReturn("");
        when(restTemplate.exchange("http://localhost:8080/users/authorize/user1/group1/127.0.0.1",
                HttpMethod.GET, new HttpEntity<>(headers), String.class)).thenReturn(result);
        provider = new HeaderAuthenticationProvider("localhost", 8080, "auth",
                restTemplate, verifier,
                componentJwtStore);
        authentication = mock(HeaderAuthenticationToken.class);
    }

    /**
     * Test authenticate with user and group in the header.
     */
    @Test
    public void testAuthenticateWithUserAndGroup() {
        when(authentication.getUserName()).thenReturn(USER_1);
        when(authentication.getGroup()).thenReturn(GROUP_1);
        when(authentication.getJwtToken()).thenReturn(Optional.empty());
        when(authentication.getRemoteIpAddress()).thenReturn(IP_ADDRESS);
        UsernamePasswordAuthenticationToken auth =
                (UsernamePasswordAuthenticationToken)provider.authenticate(authentication);
        final AuthUserDTO userDTO = (AuthUserDTO)auth.getPrincipal();
        assertEquals(USER_1, userDTO.getUser());
        assertEquals(Lists.newArrayList(ADMINISTRATOR), userDTO.getRoles());
        assertTrue(StringUtils.isEmpty(userDTO.getToken()));
    }

    /**
     * Test authenticate with JWT in the header.
     */
    @Test
    public void testAuthenticateWithJwt() {
        when(authentication.getUserName()).thenReturn(USER_1);
        when(authentication.getGroup()).thenReturn("");
        when(authentication.getJwtToken()).thenReturn(TOKEN_1);
        when(authentication.getRemoteIpAddress()).thenReturn(IP_ADDRESS);
        UsernamePasswordAuthenticationToken auth =
                (UsernamePasswordAuthenticationToken)provider.authenticate(authentication);
        final AuthUserDTO userDTO = (AuthUserDTO)auth.getPrincipal();
        assertEquals(USER_1, userDTO.getUser());
        assertEquals(Lists.newArrayList(ADMINISTRATOR), userDTO.getRoles());
        assertEquals(TOKEN_1.get(), userDTO.getToken());
    }
}