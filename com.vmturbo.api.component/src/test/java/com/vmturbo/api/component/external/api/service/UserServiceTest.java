package com.vmturbo.api.component.external.api.service;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.session.SessionInformation;
import org.springframework.security.core.session.SessionRegistry;
import org.springframework.web.client.RestTemplate;

import com.google.common.collect.ImmutableList;

import com.vmturbo.auth.api.usermgmt.AuthUserDTO;


/**
 * Test delete user will also invoke expiring user's active sessions
 */
@RunWith(MockitoJUnitRunner.class)
public class UserServiceTest {


    private static final String TEST_USER = "testUser";
    final RestTemplate restTemplate = Mockito.mock(RestTemplate.class);
    final GroupsService groupsService = Mockito.mock(GroupsService.class);
    final SessionInformation sessionInformation = mock(SessionInformation.class);
    @InjectMocks
    private UsersService usersService = new UsersService("", 0, restTemplate, "", false, groupsService);
    @Mock
    private SessionRegistry sessionRegistry;

    @Before
    public void setup() {
        when(sessionRegistry.getAllPrincipals()).thenReturn(ImmutableList.of(TEST_USER));
        when(sessionRegistry.getAllSessions(TEST_USER, false)).thenReturn(Collections.singletonList(sessionInformation));
    }

    /**
     * Test delete user will also invoke expiring user's active sessions
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testExipreSession() throws Exception {
        logon("admin");
        usersService.deleteUser(TEST_USER);
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
}
