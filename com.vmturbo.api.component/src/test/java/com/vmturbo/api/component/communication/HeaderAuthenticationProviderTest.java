package com.vmturbo.api.component.communication;

import static com.vmturbo.api.component.external.api.service.UsersService.HTTP_ACCEPT;
import static com.vmturbo.api.component.security.IntersightIdTokenVerifierTest.JWT_TOKEN;
import static com.vmturbo.api.component.security.IntersightIdTokenVerifierTest.PUBLIC_KEY_ONLY;
import static com.vmturbo.auth.api.authorization.jwt.SecurityConstant.AUTH_HEADER_NAME;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.Lists;

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

import com.vmturbo.api.component.security.HeaderMapper;
import com.vmturbo.api.component.security.IntersightHeaderMapper;
import com.vmturbo.api.component.security.IntersightIdTokenVerifier;
import com.vmturbo.auth.api.authorization.AuthorizationException;
import com.vmturbo.auth.api.authorization.jwt.JWTAuthorizationToken;
import com.vmturbo.auth.api.authorization.jwt.JWTAuthorizationVerifier;
import com.vmturbo.auth.api.authorization.jwt.SecurityConstant;
import com.vmturbo.auth.api.authorization.kvstore.IComponentJwtStore;
import com.vmturbo.auth.api.usermgmt.AuthUserDTO;
import com.vmturbo.auth.api.usermgmt.AuthorizeUserInputDTO;

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
    private IComponentJwtStore componentJwtStore;
    private JWTAuthorizationVerifier verifier;
    private HeaderMapper realMapper;

    /**
     * Before tests.
     *
     * @throws AuthorizationException when authorization failed
     */
    @Before
    public void setup() throws AuthorizationException {
        componentJwtStore = mock(IComponentJwtStore.class);
        verifier = mock(JWTAuthorizationVerifier.class);
        when(verifier.verify(any(), any())).thenReturn(
                new AuthUserDTO(USER_1, "uuid", Lists.newArrayList(ADMINISTRATOR)));
        jwtAuthorizationToken = mock(JWTAuthorizationToken.class);
        when(componentJwtStore.generateToken()).thenReturn(jwtAuthorizationToken);
        when(jwtAuthorizationToken.getCompactRepresentation()).thenReturn(JWTTOKEN);
        realMapper = new IntersightHeaderMapper(Collections.EMPTY_MAP, "", "", "", "");
        authentication = mock(HeaderAuthenticationToken.class);
    }

    private RestTemplate getRestTemplate(@Nonnull final String url,
            @Nonnull final String username,
            @Nullable final Optional<String> groupName,
            @Nonnull final String remoteIpAddress) {
        final RestTemplate restTemplate = mock(RestTemplate.class);
        HttpHeaders headers = new HttpHeaders();
        headers.setAccept(HTTP_ACCEPT);
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.set(AUTH_HEADER_NAME, JWTTOKEN);
        headers.set(SecurityConstant.COMPONENT_ATTRIBUTE, null);

        final HttpEntity<AuthorizeUserInputDTO> entity
                = new HttpEntity<>(new AuthorizeUserInputDTO(username, groupName.orElse(null), remoteIpAddress), headers);

        when(result.getBody()).thenReturn(TOKEN_1.get());
        when(restTemplate.exchange(url, HttpMethod.POST, entity, String.class)).thenReturn(result);
        return restTemplate;
    }

    /**
     * Test authenticate with user and group in the header.
     */
    @Test
    public void testAuthenticateWithUserAndGroup() {
        when(authentication.getUserName()).thenReturn(USER_1);
        when(authentication.getGroup()).thenReturn(GROUP_1);
        when(authentication.getJwtToken()).thenReturn(Optional.empty());
        when(authentication.getPublicKey()).thenReturn(Optional.empty());
        when(authentication.getRemoteIpAddress()).thenReturn(IP_ADDRESS);

        final RestTemplate restTemplate =
                getRestTemplate("http://localhost:8080/users/authorize/", "user1", Optional.of("group1"), "127.0.0.1");
        provider =
                new HeaderAuthenticationProvider("localhost", 8080, "auth", restTemplate, verifier,
                        componentJwtStore, new IntersightIdTokenVerifier("privilegeSet_names"),
                        60 * 60 * 24 * 365 * 30);
        UsernamePasswordAuthenticationToken auth =
                (UsernamePasswordAuthenticationToken)provider.authenticate(authentication);
        final AuthUserDTO userDTO = (AuthUserDTO)auth.getPrincipal();
        assertEquals(USER_1, userDTO.getUser());
        assertEquals(Lists.newArrayList(ADMINISTRATOR), userDTO.getRoles());
        assertEquals(TOKEN_1.get(), userDTO.getToken());
    }

    /**
     * Test authenticate with JWT in the header.
     */
    @Test
    public void testAuthenticateWithJwt() {
        final RestTemplate restTemplate = getRestTemplate(
                "http://localhost:8080/users/authorize/", "devops-admin@local", Optional.of("group1"), "127.0.0.1");
        provider =
                new HeaderAuthenticationProvider("localhost", 8080, "auth", restTemplate, verifier,
                        componentJwtStore, new IntersightIdTokenVerifier("privilegeSet_names"),
                        60 * 60 * 24 * 365 * 30);
        HeaderMapper mapper = mock(HeaderMapper.class);
        when(mapper.getAuthGroup("System Administrator")).thenReturn("group1");
        when(authentication.getUserName()).thenReturn(USER_1);
        when(authentication.getGroup()).thenReturn("");
        when(authentication.getJwtToken()).thenReturn(Optional.of(JWT_TOKEN));
        when(authentication.getPublicKey()).thenReturn(
                realMapper.buildPublicKey(Optional.of(PUBLIC_KEY_ONLY)));
        when(authentication.getRemoteIpAddress()).thenReturn(IP_ADDRESS);
        when(authentication.getHeaderMapper()).thenReturn(mapper);
        UsernamePasswordAuthenticationToken auth =
                (UsernamePasswordAuthenticationToken)provider.authenticate(authentication);
        final AuthUserDTO userDTO = (AuthUserDTO)auth.getPrincipal();
        assertEquals("devops-admin@local", userDTO.getUser());
        assertEquals(Lists.newArrayList(ADMINISTRATOR), userDTO.getRoles());
        assertEquals(TOKEN_1.get(), userDTO.getToken());
    }
}