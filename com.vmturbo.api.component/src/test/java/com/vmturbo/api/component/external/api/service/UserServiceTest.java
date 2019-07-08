package com.vmturbo.api.component.external.api.service;

import static com.vmturbo.api.component.external.api.service.UsersService.HTTP_ACCEPT;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.session.SessionInformation;
import org.springframework.security.core.session.SessionRegistry;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import com.google.common.collect.ImmutableList;

import com.vmturbo.api.component.communication.RestAuthenticationProvider;
import com.vmturbo.api.dto.user.ActiveDirectoryGroupApiDTO;
import com.vmturbo.auth.api.authentication.credentials.SAMLUserUtils;
import com.vmturbo.auth.api.usermgmt.AuthUserDTO;
import com.vmturbo.auth.api.usermgmt.SecurityGroupDTO;


/**
 * Test delete user will also invoke expiring user's active sessions
 */
@RunWith(MockitoJUnitRunner.class)
public class UserServiceTest {

    private static final String TEST_USER = "testUser";

    private final RestTemplate restTemplate = mock(RestTemplate.class);
    private final GroupsService groupsService = mock(GroupsService.class);
    private WidgetSetsService widgetSetsService = mock(WidgetSetsService.class);
    private final SessionInformation sessionInformation = mock(SessionInformation.class);
    private final SessionRegistry sessionRegistry = mock(SessionRegistry.class);

    @InjectMocks
    private UsersService usersService = new UsersService("", 0, restTemplate, "", false,
        groupsService, widgetSetsService);

    @Before
    public void setup() {
        when(sessionRegistry.getAllPrincipals()).thenReturn(ImmutableList.of(TEST_USER));
        when(sessionRegistry.getAllSessions(TEST_USER, false)).thenReturn(
            Collections.singletonList(sessionInformation));
    }

    /**
     * Test delete user will also invoke expiring user's active sessions
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testExipreSession() throws Exception {
        logon("admin");
        final HttpEntity<AuthUserDTO> entity = new HttpEntity<>(composeHttpHeaders());
        final ResponseEntity<AuthUserDTO> responseEntity = ResponseEntity
            .ok(new AuthUserDTO("", "", Collections.emptyList()));
        when(restTemplate.exchange("http://:0/users/remove/testUser", HttpMethod.DELETE, entity, AuthUserDTO.class))
            .thenReturn(responseEntity);
        usersService.deleteUser(TEST_USER);
        verify(widgetSetsService).transferWidgetsets(TEST_USER);
        verify(sessionRegistry).getAllPrincipals();
        verify(sessionRegistry).getAllSessions(TEST_USER, false);
        verify(sessionInformation).expireNow();
    }

    /**
     * Performs the actual logon from the token passed though.
     */
    private void logon(String role) throws Exception {
        // Local authentication
        Set<GrantedAuthority> grantedAuths = new HashSet<>();
        for (String r : role.split("\\|")) {
            grantedAuths.add(new SimpleGrantedAuthority("ROLE" + "_" + r.toUpperCase()));
        }
        SecurityContextHolder.getContext().setAuthentication(
                new UsernamePasswordAuthenticationToken(
                        new AuthUserDTO(null,
                                "admin",
                                "pass",
                                "10.10.10.10",
                                "11111",
                                "token",
                                ImmutableList.of("ADMINISTRATOR"),
                                null),
                        "admin000",
                        grantedAuths));
    }

    @Test
    public void testGetActiveDirectoryGroups() throws Exception {
        final String adGroupName = "VPNUsers";
        final String adGroupType = "DedicatedCustomer";
        final String adGroupRoleName = "observer";

        // mock rest response
        logon("admin");
        HttpHeaders headers = new HttpHeaders();
        headers.setAccept(HTTP_ACCEPT);
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.set(RestAuthenticationProvider.AUTH_HEADER_NAME, "token");
        HttpEntity<List> entity = new HttpEntity<>(headers);

        final String authRequest = UriComponentsBuilder.newInstance()
            .scheme("http")
            .host("")
            .port(0)
            .path("/users/ad/groups")
            .build().toUriString();

        final Long securityGroupOid = 1234L;
        ResponseEntity<List> response = new ResponseEntity<>(ImmutableList.of(
            new SecurityGroupDTO(adGroupName, adGroupType, adGroupRoleName,
                Collections.emptyList(), securityGroupOid)), HttpStatus.OK);
        Mockito.when(restTemplate.exchange(authRequest, HttpMethod.GET, entity, List.class)).thenReturn(response);

        // GET and verify results
        List<ActiveDirectoryGroupApiDTO> adGroups = usersService.getActiveDirectoryGroups();
        assertEquals(1, adGroups.size());

        // check uuid and other fields are set
        assertEquals(String.valueOf(securityGroupOid), adGroups.get(0).getUuid());
        assertEquals(adGroupName, adGroups.get(0).getDisplayName());
        assertEquals(adGroupType, adGroups.get(0).getType());
        assertEquals(adGroupRoleName, adGroups.get(0).getRoleName());
    }

    private HttpHeaders composeHttpHeaders() {
        HttpHeaders headers = new HttpHeaders();
        headers.setAccept(HTTP_ACCEPT);
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.set(RestAuthenticationProvider.AUTH_HEADER_NAME,
            geJwtTokenFromSpringSecurityContext().orElseThrow(() ->
                new SecurityException("Invalid JWT token")));
        return headers;
    }

    private static Optional<String> geJwtTokenFromSpringSecurityContext() {
        return SAMLUserUtils
            .getAuthUserDTO()
            .map(AuthUserDTO::getToken);
    }
}
