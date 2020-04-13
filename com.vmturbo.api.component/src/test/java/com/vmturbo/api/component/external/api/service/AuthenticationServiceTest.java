package com.vmturbo.api.component.external.api.service;

import static com.vmturbo.api.component.external.api.service.UsersService.HTTP_ACCEPT;
import static org.mockito.Matchers.any;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import com.google.common.collect.Lists;

import com.vmturbo.api.component.communication.RestAuthenticationProvider;
import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.user.UserApiDTO;
import com.vmturbo.auth.api.authorization.AuthorizationException;
import com.vmturbo.auth.api.authorization.jwt.JWTAuthorizationToken;
import com.vmturbo.auth.api.authorization.jwt.JWTAuthorizationVerifier;
import com.vmturbo.auth.api.authorization.jwt.SecurityConstant;
import com.vmturbo.auth.api.authorization.kvstore.ComponentJwtStore;
import com.vmturbo.auth.api.authorization.kvstore.IComponentJwtStore;
import com.vmturbo.auth.api.usermgmt.AuthUserDTO;

/**
 * Unit tests for when {@link AuthenticationService}. It now only for
 * {@link AuthenticationService#authorize}
 */
public class AuthenticationServiceTest {


    public static final String AUTH_HOST = "AUTH_HOST";
    private static final int AUTH_PORT = 4321;
    public static final String PASSWORD = "password";
    private final String username = "username";
    private final String group = "group";
    private final String ipAddress = "10.10.1.1";
    private AuthenticationService testAuthenticationService;
    private JWTAuthorizationVerifier mockVerifier;
    private RestTemplate mockRestTemplate;
    private IComponentJwtStore componentJwtStore = Mockito.mock(ComponentJwtStore.class);
    private UriComponentsBuilder builder = UriComponentsBuilder.newInstance()
            .scheme("http")
            .host(AUTH_HOST)
            .port(AUTH_PORT)
            .path("/users/authorize/");

    @Before
    public void setup() {
        mockVerifier = Mockito.mock(JWTAuthorizationVerifier.class);
        mockRestTemplate = Mockito.mock(RestTemplate.class);
        testAuthenticationService = new AuthenticationService(
                AUTH_HOST,
                AUTH_PORT,
                "",
                mockVerifier,
                mockRestTemplate,
                componentJwtStore,
                1);
    }

    @Test
    public void testAuthorizeSAMLUser() throws Exception {
        final String authRequest = builder
                .pathSegment(
                        encodeValue(username),
                        ipAddress)
                .build()
                .toUriString();

        AuthUserDTO dto = getAuthUserDTO(username, authRequest);
        Optional<AuthUserDTO> authUserDTO = testAuthenticationService.authorize(username,
                Optional.empty(), ipAddress);
        Assert.assertEquals(dto.getUser(), authUserDTO.get().getUser());
        Assert.assertEquals(dto.getRoles(), authUserDTO.get().getRoles());
    }

    @Test
    public void testInitAdmin() throws Exception {
        BaseApiDTO authUserDTO = testAuthenticationService.initAdmin(username,
                PASSWORD);
        Assert.assertEquals(AuthenticationService.ADMINISTRATOR, ((UserApiDTO)authUserDTO).getRoleName());
        Assert.assertEquals(username, ((UserApiDTO)authUserDTO).getUsername());
    }


    @Test
    public void testAuthorizeSAMLUserWithGroup() throws Exception {
        final String authRequest = builder
                .pathSegment(
                        encodeValue(username),
                        encodeValue(group),
                        ipAddress)
                .build()
                .toUriString();
        AuthUserDTO dto = getAuthUserDTO(username, authRequest);
        Optional<AuthUserDTO> authUserDTO = testAuthenticationService.
                authorize(username, Optional.of(group), ipAddress);
        Assert.assertEquals(dto.getUser(), authUserDTO.get().getUser());
        Assert.assertEquals(dto.getRoles(), authUserDTO.get().getRoles());
    }

    private AuthUserDTO getAuthUserDTO(final String username, final String authRequest)
            throws AuthorizationException {
        Mockito.when(componentJwtStore.generateToken())
                .thenReturn(new JWTAuthorizationToken(""));

        HttpHeaders headers = new HttpHeaders();
        headers.setAccept(HTTP_ACCEPT);
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.set(RestAuthenticationProvider.AUTH_HEADER_NAME,
                componentJwtStore.generateToken().getCompactRepresentation());
        headers.set(SecurityConstant.COMPONENT_ATTRIBUTE, componentJwtStore.getNamespace());
        HttpEntity<List> entity = new HttpEntity<>(headers);

        ResponseEntity<String> responseEntity =
                new ResponseEntity<String>("mockToken", HttpStatus.OK);

        Mockito.when(mockRestTemplate.exchange(authRequest, HttpMethod.GET, entity, String.class))
                .thenReturn(responseEntity);

        AuthUserDTO dto = new AuthUserDTO(username,
                username,
                Lists.newArrayList("administrator"));
        Mockito.when(mockVerifier.verify(any(), any())).thenReturn(dto);
        return dto;
    }

    private String encodeValue(String value) {
        try {
            return URLEncoder.encode(value, StandardCharsets.UTF_8.toString());
        } catch (UnsupportedEncodingException e) {
            return value; // will try the original value.
        }
    }
}
