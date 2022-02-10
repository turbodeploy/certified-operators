package com.vmturbo.api.component.external.api.service;

import static com.vmturbo.api.component.external.api.service.UsersService.HTTP_ACCEPT;
import static com.vmturbo.auth.api.authorization.jwt.SecurityConstant.AUTH_HEADER_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Matchers;
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

import com.vmturbo.api.component.ApiTestUtils;
import com.vmturbo.api.component.external.api.mapper.UuidMapper;
import com.vmturbo.api.component.communication.RestAuthenticationProvider;
import com.vmturbo.api.component.external.api.util.ReportingUserCalculator;
import com.vmturbo.api.dto.group.GroupApiDTO;
import com.vmturbo.api.dto.user.ActiveDirectoryApiDTO;
import com.vmturbo.api.dto.user.ActiveDirectoryGroupApiDTO;
import com.vmturbo.api.dto.user.RoleApiDTO;
import com.vmturbo.api.dto.user.UserApiDTO;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.auth.api.authentication.credentials.SAMLUserUtils;
import com.vmturbo.auth.api.licensing.LicenseCheckClient;
import com.vmturbo.auth.api.usermgmt.ActiveDirectoryDTO;
import com.vmturbo.auth.api.usermgmt.AuthUserDTO;
import com.vmturbo.auth.api.usermgmt.SecurityGroupDTO;

/**
 * Test delete user will also invoke expiring user's active sessions
 */
@RunWith(MockitoJUnitRunner.class)
public class UserServiceTest {

    private static final String TEST_USER = "testUser";
    private static final String CORP_VMTURBO_COM = "corp.vmturbo.com";
    private static final String LDAP_DELL_1_VMTURBO_COM = "ldap://dell1.vmturbo.com";
    private static final String OBSERVER = "observer";
    private static final String DEDICATED_CUSTOMER = "DedicatedCustomer";
    private static final String LDAP = "LDAP";
    private static final String SHARED_ADVISOR = "SHARED_ADVISOR";
    private static final String SHARED_OBSERVER = "SHARED_OBSERVER";
    private static final String VALID_GROUP = "285408123157775";
    private static final String VALID_GROUP1 = "285408123157776";
    private static final String INVALID_UUID = "abcde";
    private static final String VALID_ENTITY = "74272581024016";
    private final RestTemplate restTemplate = mock(RestTemplate.class);
    private final GroupsService groupsService = mock(GroupsService.class);
    private WidgetSetsService widgetSetsService = mock(WidgetSetsService.class);
    private final SessionInformation sessionInformation = mock(SessionInformation.class);
    private final SessionRegistry sessionRegistry = mock(SessionRegistry.class);
    private final ReportingUserCalculator reportingUserCalculator = mock(ReportingUserCalculator.class);
    private final LicenseService licenseService = mock(LicenseService.class);

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private UuidMapper uuidMapper = mock(UuidMapper.class);

    @InjectMocks
    private UsersService usersService = new UsersService("", 0, "", uuidMapper, restTemplate, "", false,
        groupsService, widgetSetsService, reportingUserCalculator, licenseService);

    private static final String AUTH_REQUEST = UriComponentsBuilder.newInstance()
            .scheme("http")
            .host("")
            .port(0)
            .path("/users/ad/groups")
            .build().toUriString();
    private static final String AD_GROUP_NAME = "VPNUsers";
    private static final String AD_GROUP_TYPE = "DedicatedCustomer";
    private static final String AD_GROUP_ROLE_NAME = "observer";
    private static final String AD_REQUEST = UriComponentsBuilder.newInstance()
            .scheme("http")
            .host("")
            .port(0)
            .path("/users/ad")
            .build().toUriString();

    @Before
    public void setup() {
        when(sessionRegistry.getAllPrincipals()).thenReturn(ImmutableList.of(TEST_USER));
        when(sessionRegistry.getAllSessions(TEST_USER, false)).thenReturn(
            Collections.singletonList(sessionInformation));
    }

    /**
     * Test get AD.
     *
     * @throws Exception if something wrongs.
     */
    @Test
    public void testGetActiveDirectories() throws Exception {
        logon("admin");
        setupGetAd();
        setupGetAdGroup();
        List<ActiveDirectoryApiDTO> responseDtos = usersService.getActiveDirectories();
        assertEquals(1, responseDtos.size());
        verifyAdResponse(responseDtos.get(0));
    }

    /**
     * Test create AD.
     *
     * @throws Exception if something wrongs.
     */
    @Test
    public void testCreateActiveDirectory() throws Exception {
        logon("admin");
        setupGetAdGroup();
        Mockito.when(restTemplate.exchange(Matchers.eq(AD_REQUEST),
                Matchers.eq(HttpMethod.POST),
                Matchers.<HttpEntity>any(),
                Matchers.<Class<ActiveDirectoryDTO>>any())).
                thenReturn(new ResponseEntity<>(new ActiveDirectoryDTO(CORP_VMTURBO_COM, LDAP_DELL_1_VMTURBO_COM, true), HttpStatus.OK));
        ActiveDirectoryApiDTO responseDto = usersService.createActiveDirectory(getCreateAdInputDto());
        verifyAdResponse(responseDto);
    }

    /**
     * Test update AD.
     *
     * @throws Exception if something went wrong.
     */
    @Test
    public void testUpdateActiveDirectory() throws Exception {
        logon("admin");
        setupGetAdGroup();
        Mockito.when(restTemplate
                .exchange(Matchers.eq(AD_REQUEST), Matchers.eq(HttpMethod.POST), Matchers.any(),
                        Matchers.<Class<ActiveDirectoryDTO>>any())).thenReturn(new ResponseEntity<>(
                new ActiveDirectoryDTO(CORP_VMTURBO_COM, LDAP_DELL_1_VMTURBO_COM, true),
                HttpStatus.OK));
        ActiveDirectoryApiDTO responseDto = usersService.updateActiveDirectory(
                getCreateAdInputDto());
        verifyAdResponse(responseDto);
    }

    /**
     * Test update AD when api call to auth fails.
     *
     * @throws Exception if something went wrong.
     */
    @Test(expected = RuntimeException.class)
    public void testUpdateActiveDirectoryWhenApiCallFails() throws Exception {
        logon("admin");
        setupGetAdGroup();
        Mockito.when(restTemplate
                .exchange(Matchers.eq(AD_REQUEST), Matchers.eq(HttpMethod.POST), Matchers.any(),
                        Matchers.<Class<ActiveDirectoryDTO>>any())).thenThrow(
                RuntimeException.class);
        usersService.updateActiveDirectory(getCreateAdInputDto());
    }

    /**
     * Test delete AD.
     *
     * @throws Exception if something went wrong
     */
    @Test
    public void testDeleteActiveDirectory() throws Exception {
        logon("admin");
        Mockito.when(restTemplate
                .exchange(Matchers.eq(AD_REQUEST), Matchers.eq(HttpMethod.POST), Matchers.any(),
                        Matchers.<Class<Boolean>>any())).thenReturn(
                new ResponseEntity<>(true, HttpStatus.OK));
        assertTrue(usersService.deleteActiveDirectory());
    }

    /**
     * Test delete AD when api call fails.
     *
     * @throws Exception if something went wrong
     */
    @Test
    public void testDeleteActiveDirectoryWhenApiCallFails() throws Exception {
        logon("admin");
        Mockito.when(restTemplate
                .exchange(Matchers.eq(AD_REQUEST), Matchers.eq(HttpMethod.POST), Matchers.any(),
                        Matchers.<Class<Boolean>>any())).thenThrow(RuntimeException.class);
        assertFalse(usersService.deleteActiveDirectory());
    }

    /**
     * Test convert AD info to AuthDTO. If domain name or server is null, the corresponding AuthDTO
     * will have null value, instead of "".
     */
    @Test
    public void testConvertADInfoToAuth() {
        ActiveDirectoryApiDTO inputDto = new ActiveDirectoryApiDTO();
        assertNull(usersService.convertADInfoToAuth(inputDto).getLoginProviderURI());
        assertNull(usersService.convertADInfoToAuth(inputDto).getDomainName());
    }
    /**
     * Test delete active directory group.
     *
     * @throws Exception if somethings happens
     */
    @Test
    public void testDeleteActiveDirectoryGroup() throws Exception {
        logon("admin");
        Mockito.when(restTemplate.exchange(Matchers.eq(UriComponentsBuilder.newInstance()
                        .scheme("http")
                        .host("")
                        .port(0)
                        .path("/users/ad/groups/" + AD_GROUP_NAME + "/")
                        .build().toUriString()),
                Matchers.eq(HttpMethod.DELETE),
                Matchers.<HttpEntity>any(),
                Matchers.<Class<Boolean>>any())).thenReturn(new ResponseEntity<>(new Boolean(true), HttpStatus.OK));
        assertTrue(usersService.deleteActiveDirectoryGroup(AD_GROUP_NAME));
    }

    /**
     * Test create active directory group.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testCreateActiveDirectoryGroup() throws Exception {
        logon("admin");
        Mockito.when(restTemplate.exchange(Matchers.eq(AUTH_REQUEST),
                Matchers.eq(HttpMethod.POST),
                Matchers.<HttpEntity>any(),
                Matchers.<Class<SecurityGroupDTO>>any())).thenReturn(getResponse());
        ActiveDirectoryGroupApiDTO adGroup = usersService
                .createActiveDirectoryGroup(getActiveDirectoryGroupApiDTO(AD_GROUP_NAME, AD_GROUP_TYPE, AD_GROUP_ROLE_NAME));
        verifyResponse(AD_GROUP_NAME, adGroup.getUuid(), AD_GROUP_NAME, adGroup.getDisplayName(), AD_GROUP_TYPE,
                adGroup.getType(), AD_GROUP_ROLE_NAME, adGroup.getRoleName());
    }

    /**
     * Test change active directory group.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testChangeActiveDirectoryGroup() throws Exception {
        logon("admin");
        Mockito.when(restTemplate.exchange(Matchers.eq(AUTH_REQUEST),
                Matchers.eq(HttpMethod.PUT),
                Matchers.<HttpEntity>any(),
                Matchers.<Class<SecurityGroupDTO>>any())).thenReturn(getResponse());
        ActiveDirectoryGroupApiDTO adGroup = usersService
                .changeActiveDirectoryGroup(getActiveDirectoryGroupApiDTO(AD_GROUP_NAME, AD_GROUP_TYPE, AD_GROUP_ROLE_NAME));
        verifyResponse(AD_GROUP_NAME, adGroup.getUuid(), AD_GROUP_NAME, adGroup.getDisplayName(),
                AD_GROUP_TYPE, adGroup.getType(), AD_GROUP_ROLE_NAME, adGroup.getRoleName());
    }

    /**
     * Test create active directory user.
     *
     * @throws Exception if something go wrong.
     */
    @Test
    public void testCreateActiveDirectoryUser() throws Exception {
        logon("admin");
        UserApiDTO resultUser = usersService.createUser(setupUserApiDTO());
        verifyAdUser(resultUser);
    }

    /**
     * Test perform AD operations without logon.
     *
     * @throws Exception if something wrongs.
     */
    @Test(expected = SecurityException.class)
    public void testGetActiveDirectoriesWithoutLogon() throws Exception {
        logout();
        usersService.getActiveDirectories();
    }

    /**
     * Test create active directory group when Auth component is down.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test(expected = RuntimeException.class)
    public void testCreateActiveDirectoryGroupWhenAuthIsDown() throws Exception {
        logon("admin");
        Mockito.when(restTemplate.exchange(Matchers.eq(AUTH_REQUEST),
                Matchers.eq(HttpMethod.POST),
                Matchers.<HttpEntity>any(),
                Matchers.<Class<SecurityGroupDTO>>any())).thenThrow(new RuntimeException("Auth is down"));
        usersService.createActiveDirectoryGroup(getActiveDirectoryGroupApiDTO(AD_GROUP_NAME, AD_GROUP_TYPE, AD_GROUP_ROLE_NAME));
    }

    /**
     * Test create active directory group with invalid user input.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testCreateActiveDirectoryGroupInputValidation() throws Exception {
        logon("admin");
        Mockito.when(restTemplate.exchange(Matchers.eq(AUTH_REQUEST),
                Matchers.eq(HttpMethod.POST),
                Matchers.<HttpEntity>any(),
                Matchers.<Class<SecurityGroupDTO>>any())).thenReturn(getResponse());
        validateADInputs(adGroupFromRequest -> usersService.createActiveDirectoryGroup(adGroupFromRequest));
    }

    /**
     * Test create active directory group with valid user input.
     * Test succeeds if an exception is not thrown.
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testCreateActiveDirectoryGroupValidInputValidation() throws Exception {
        logon("admin");
        Mockito.when(restTemplate.exchange(Matchers.eq(AUTH_REQUEST),
                Matchers.eq(HttpMethod.POST),
                Matchers.<HttpEntity>any(),
                Matchers.<Class<SecurityGroupDTO>>any())).thenReturn(getResponse());
        validateValidADInputs(adGroupFromRequest -> usersService.createActiveDirectoryGroup(adGroupFromRequest));
    }

    /**
     * Test create active directory group with empty scope list.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testCreateActiveDirectoryGroupInputValidationEmptyScopeList() throws Exception {
        logon("admin");
        Mockito.when(restTemplate.exchange(Matchers.eq(AUTH_REQUEST),
                Matchers.eq(HttpMethod.POST),
                Matchers.<HttpEntity>any(),
                Matchers.<Class<SecurityGroupDTO>>any())).thenReturn(getResponse());
        validateInvalidADInputsEmptyScopeList(adGroupFromRequest -> usersService.createActiveDirectoryGroup(adGroupFromRequest));
    }

    /**
     * Test create active directory group with empty UUID in list.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testCreateActiveDirectoryGroupInputInvalidValidationEmptyUuids() throws Exception {
        logon("admin");
        Mockito.when(restTemplate.exchange(Matchers.eq(AUTH_REQUEST),
                Matchers.eq(HttpMethod.POST),
                Matchers.<HttpEntity>any(),
                Matchers.<Class<SecurityGroupDTO>>any())).thenReturn(getResponse());
        validateInvalidADInputsEmptyUuids(adGroupFromRequest -> usersService.createActiveDirectoryGroup(adGroupFromRequest));
    }

    /**
     * Test create active directory group with one empty UUID.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testCreateActiveDirectoryGroupInputValidationOneEmptyUuid() throws Exception {
        logon("admin");
        Mockito.when(restTemplate.exchange(Matchers.eq(AUTH_REQUEST),
                Matchers.eq(HttpMethod.POST),
                Matchers.<HttpEntity>any(),
                Matchers.<Class<SecurityGroupDTO>>any())).thenReturn(getResponse());
        validateInvalidADInputsOneEmptyUuid(adGroupFromRequest -> usersService.createActiveDirectoryGroup(adGroupFromRequest));
    }

    /**
     * Test create active directory group with invalid UUID with empty string.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testCreateActiveDirectoryGroupInputValidationEmptyStringUuid() throws Exception {
        logon("admin");
        Mockito.when(restTemplate.exchange(Matchers.eq(AUTH_REQUEST),
                Matchers.eq(HttpMethod.POST),
                Matchers.<HttpEntity>any(),
                Matchers.<Class<SecurityGroupDTO>>any())).thenReturn(getResponse());
        validateInvalidADInputsEmptyStringUuid(adGroupFromRequest -> usersService.createActiveDirectoryGroup(adGroupFromRequest));
    }

    /**
     * Test create active directory group with valid UUID but UUID is a entity.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testCreateActiveDirectoryGroupInputValidationNonEmptyEntities() throws Exception {
        logon("admin");
        Mockito.when(restTemplate.exchange(Matchers.eq(AUTH_REQUEST),
                Matchers.eq(HttpMethod.POST),
                Matchers.<HttpEntity>any(),
                Matchers.<Class<SecurityGroupDTO>>any())).thenReturn(getResponse());
        validateInvalidADInputsNonEmptyEntities(adGroupFromRequest -> usersService.createActiveDirectoryGroup(adGroupFromRequest));
    }

    /**
     * Test create active directory group with invalid UUID.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testCreateActiveDirectoryGroupInputValidationInvalidUuid() throws Exception {
        logon("admin");
        Mockito.when(restTemplate.exchange(Matchers.eq(AUTH_REQUEST),
                Matchers.eq(HttpMethod.POST),
                Matchers.<HttpEntity>any(),
                Matchers.<Class<SecurityGroupDTO>>any())).thenReturn(getResponse());
        validateInvalidADInputsInvalidUUID(adGroupFromRequest -> usersService.createActiveDirectoryGroup(adGroupFromRequest));
    }

    /**
     * Test change active directory group with invalid user input.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testChangeActiveDirectoryGroupInputValidation() throws Exception {
        logon("admin");
        Mockito.when(restTemplate.exchange(Matchers.eq(AUTH_REQUEST),
                Matchers.eq(HttpMethod.PUT),
                Matchers.<HttpEntity>any(),
                Matchers.<Class<SecurityGroupDTO>>any())).thenReturn(getResponse());
        validateADInputs(adGroupFromRequest -> usersService.changeActiveDirectoryGroup(adGroupFromRequest));
    }

    /**
     * Test change active directory group with invalid Uuids.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testChangeActiveDirectoryGroupInputValidationInvalidUuids() throws Exception {
        logon("admin");
        Mockito.when(restTemplate.exchange(Matchers.eq(AUTH_REQUEST),
                Matchers.eq(HttpMethod.PUT),
                Matchers.<HttpEntity>any(),
                Matchers.<Class<SecurityGroupDTO>>any())).thenReturn(getResponse());
        validateADInputs(adGroupFromRequest -> usersService.changeActiveDirectoryGroup(adGroupFromRequest));
    }

    /**
     * Test create/change active directory with invalid user input.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testActiveDirectoryInputValidation() throws Exception {
        logon("admin");
        setupGetAdGroup();
        Mockito.when(restTemplate.exchange(Matchers.eq(AD_REQUEST),
                Matchers.eq(HttpMethod.POST),
                Matchers.<HttpEntity>any(),
                Matchers.<Class<ActiveDirectoryDTO>>any()))
                .thenReturn(new ResponseEntity<>(new ActiveDirectoryDTO(CORP_VMTURBO_COM, LDAP_DELL_1_VMTURBO_COM, true), HttpStatus.OK));
        ActiveDirectoryApiDTO inputDto = new ActiveDirectoryApiDTO();

        try {
            usersService.createActiveDirectory(inputDto);
            fail("IllegalArgumentException should have thrown.");
        } catch (IllegalArgumentException e) {
        }

        // set domain name
        inputDto.setDomainName(CORP_VMTURBO_COM);
        // it should pass since we only need either domain name or login provider URL
        usersService.createActiveDirectory(inputDto);

        // unset domain name
        inputDto.setDomainName(null);

        // set login provider URL only
        inputDto.setLoginProviderURI(LDAP_DELL_1_VMTURBO_COM);
        // it should also pass
        usersService.createActiveDirectory(inputDto);
    }

    /**
     * Test input validation on deleting active directory group.
     *
     * @throws Exception if somethings happens
     */
    @Test(expected = IllegalArgumentException.class)
    public void testDeleteActiveDirectoryGroupInputValidation() throws Exception {
        logon("admin");
        Mockito.when(restTemplate.exchange(Matchers.eq(UriComponentsBuilder.newInstance()
                        .scheme("http")
                        .host("")
                        .port(0)
                        .path("/users/ad/groups/" + AD_GROUP_NAME)
                        .build().toUriString()),
                Matchers.eq(HttpMethod.DELETE),
                Matchers.<HttpEntity>any(),
                Matchers.<Class<Boolean>>any())).thenReturn(new ResponseEntity<>(new Boolean(true), HttpStatus.OK));
        usersService.deleteActiveDirectoryGroup("");
    }

    /**
     * Test delete user will also invoke expiring user's active sessions.
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
        verify(widgetSetsService).transferWidgetsets(TEST_USER, "");
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
        headers.set(AUTH_HEADER_NAME, "token");
        HttpEntity<List> entity = new HttpEntity<>(headers);

        final String authRequest = UriComponentsBuilder.newInstance()
            .scheme("http")
            .host("")
            .port(0)
            .path("/users/ad/groups")
            .build().toUriString();

        ResponseEntity<List> response = new ResponseEntity<>(ImmutableList.of(
            new SecurityGroupDTO(adGroupName, adGroupType, adGroupRoleName)), HttpStatus.OK);
        Mockito.when(restTemplate.exchange(authRequest, HttpMethod.GET, entity, List.class)).thenReturn(response);

        // GET and verify results
        List<ActiveDirectoryGroupApiDTO> adGroups = usersService.getActiveDirectoryGroups();
        assertEquals(1, adGroups.size());

        // check uuid and other fields are set
        assertEquals(adGroupName, adGroups.get(0).getUuid());
        assertEquals(adGroupName, adGroups.get(0).getDisplayName());
        assertEquals(adGroupType, adGroups.get(0).getType());
        assertEquals(adGroupRoleName, adGroups.get(0).getRoleName());
    }

    /**
     * Testing that when the api input has an empty user name, the service will
     * throw an IllegalArgumentException
     * @throws Exception when the service fails to create the user
     */
    @Test
    public void testEmptyUserName() throws Exception {
        final String userName = "";
        final String userType = "DedicatedCustomer";
        final String userRole = "observer";
        final String userLoginProvider = "LOCAL";

        UserApiDTO userApiDTO = new UserApiDTO();
        userApiDTO.setUsername(userName);
        userApiDTO.setType(userType);
        userApiDTO.setRoles(makeRole(userRole));
        userApiDTO.setLoginProvider(userLoginProvider);

        // This should throw an illegal argument exception
        expectedException.expect(IllegalArgumentException.class);
        UserApiDTO resultUser = usersService.createUser(userApiDTO);
    }

    /**
     * Testing that when the api input has an empty user type, the service will
     * throw an IllegalArgumentException
     * @throws Exception when the service fails to create the user
     */
    @Test
    public void testEmptyUserType() throws Exception {
        final String userName = "testUser2";
        final String userType = "";
        final String userRole = "observer";
        final String userLoginProvider = "LOCAL";

        UserApiDTO userApiDTO = new UserApiDTO();
        userApiDTO.setUsername(userName);
        userApiDTO.setType(userType);
        userApiDTO.setRoles(makeRole(userRole));
        userApiDTO.setLoginProvider(userLoginProvider);

        // This should throw an illegal argument exception
        expectedException.expect(IllegalArgumentException.class);
        UserApiDTO resultUser = usersService.createUser(userApiDTO);
    }

    /**
     * Testing that when the api input has an empty user role, the service will
     * throw an IllegalArgumentException
     * @throws Exception when the service fails to create the user
     */
    @Test
    public void testEmptyRole() throws Exception {
        final String userName = "testUser2";
        final String userType = "DedicatedCustomer";
        final String userRole = "";
        final String userLoginProvider = "LOCAL";

        UserApiDTO userApiDTO = new UserApiDTO();
        userApiDTO.setUsername(userName);
        userApiDTO.setType(userType);
        userApiDTO.setRoles(makeRole(userRole));
        userApiDTO.setLoginProvider(userLoginProvider);

        // This should throw an illegal argument exception
        expectedException.expect(IllegalArgumentException.class);
        UserApiDTO resultUser = usersService.createUser(userApiDTO);
    }

    /**
     * Testing that when the api input has an empty user name, the service will
     * throw an IllegalArgumentException
     *
     * @throws Exception when the service fails to edit the user
     */
    @Test
    public void testEditEmptyUserName() throws Exception {
        final String userId = "1234";
        final String userName = "";
        final String userType = "DedicatedCustomer";
        final String userRole = "observer";
        final String userLoginProvider = "LOCAL";

        UserApiDTO userApiDTO = new UserApiDTO();
        userApiDTO.setUsername(userName);
        userApiDTO.setType(userType);
        userApiDTO.setRoles(makeRole(userRole));
        userApiDTO.setLoginProvider(userLoginProvider);

        // This should throw an illegal argument exception
        expectedException.expect(IllegalArgumentException.class);
        UserApiDTO resultUser = usersService.editUser(userId, userApiDTO);
    }

    /**
     * Testing that when the api input has an empty user type, the service will
     * throw an IllegalArgumentException
     * @throws Exception when the service fails to edit the user
     */
    @Test
    public void testEditEmptyUserType() throws Exception {
        final String userId = "1234";
        final String userName = "testUser2";
        final String userType = "";
        final String userRole = "observer";
        final String userLoginProvider = "LOCAL";

        UserApiDTO userApiDTO = new UserApiDTO();
        userApiDTO.setUsername(userName);
        userApiDTO.setType(userType);
        userApiDTO.setRoles(makeRole(userRole));
        userApiDTO.setLoginProvider(userLoginProvider);

        // This should throw an illegal argument exception
        expectedException.expect(IllegalArgumentException.class);
        UserApiDTO resultUser = usersService.editUser(userId, userApiDTO);
    }

    /**
     * Testing that when the api input has an empty user role, the service will
     * throw an IllegalArgumentException
     * @throws Exception when the service fails to edit the user
     */
    @Test
    public void testEditEmptyRole() throws Exception {
        final String userId = "1234";
        final String userName = "testUser2";
        final String userType = "DedicatedCustomer";
        final String userRole = "";
        final String userLoginProvider = "LOCAL";

        UserApiDTO userApiDTO = new UserApiDTO();
        userApiDTO.setUsername(userName);
        userApiDTO.setType(userType);
        userApiDTO.setRoles(makeRole(userRole));
        userApiDTO.setLoginProvider(userLoginProvider);

        // This should throw an illegal argument exception
        expectedException.expect(IllegalArgumentException.class);
        UserApiDTO resultUser = usersService.editUser(userId, userApiDTO);
    }

    /**
     * Testing that when the api input has no user role, the service will
     * throw an IllegalArgumentException.
     *
     * @throws Exception when the service fails to edit the user
     */
    @Test
    public void testEditNoRole() throws Exception {
        final String userId = "1234";
        final String userName = "testUser2";
        final String userType = "DedicatedCustomer";
        final String userLoginProvider = "LOCAL";

        UserApiDTO userApiDTO = new UserApiDTO();
        userApiDTO.setUsername(userName);
        userApiDTO.setType(userType);
        userApiDTO.setLoginProvider(userLoginProvider);

        // This should throw an illegal argument exception
        expectedException.expect(IllegalArgumentException.class);
        UserApiDTO resultUser = usersService.editUser(userId, userApiDTO);
    }

    /**
     * Testing that when the api input has an empty password, the service will
     * throw an IllegalArgumentException.
     * @throws Exception when the service fails to create or edit the user.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testEmptyPassword() throws Exception {
        final String userName = "testUser2";
        final String userType = "DedicatedCustomer";
        final String password = "";
        final String userRole = "ADMINISTRATOR";
        final String userLoginProvider = "LOCAL";

        UserApiDTO userApiDTO = new UserApiDTO();
        userApiDTO.setUsername(userName);
        userApiDTO.setType(userType);
        userApiDTO.setRoles(makeRole(userRole));
        userApiDTO.setPassword(password);
        userApiDTO.setLoginProvider(userLoginProvider);
        // create user case.
        usersService.createUser(userApiDTO);
    }

    /**
     * This test is only very slightly different than the previous test.
     * It uses a userRole of Local instead of LOCAL.  These should be treated in
     * the same way.
     *
     * @throws Exception when the edit of a user fails, and this is expected.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testEmptyPassword2() throws Exception {
        final String userId = "1234";
        final String userName = "testUser2";
        final String userType = "DedicatedCustomer";
        final String password = "";
        final String userRole = "ADMINISTRATOR";
        final String userLoginProvider = "Local";

        UserApiDTO userApiDTO = new UserApiDTO();
        userApiDTO.setUsername(userName);
        userApiDTO.setType(userType);
        userApiDTO.setRoles(makeRole(userRole));
        userApiDTO.setPassword(password);
        userApiDTO.setLoginProvider(userLoginProvider);
        // create user case.
        usersService.createUser(userApiDTO);
    }

    /**
     * Testing that when the api the uuid in the input dot does not match the uuid
     * passed as a parameter, the service will
     * throw an IllegalArgumentException
     * @throws Exception when the service fails to edit the user
     */
    @Test
    public void testEditUserUuidMismatch() throws Exception {
        final String userId = "1234";
        final String userName = "";
        final String userType = "DedicatedCustomer";
        final String userRole = "observer";
        final String userLoginProvider = "LOCAL";

        UserApiDTO userApiDTO = new UserApiDTO();
        userApiDTO.setUsername(userName);
        userApiDTO.setType(userType);
        userApiDTO.setRoles(makeRole(userRole));
        userApiDTO.setLoginProvider(userLoginProvider);
        userApiDTO.setUuid("not1234");

        // This should throw an illegal argument exception
        expectedException.expect(IllegalArgumentException.class);
        UserApiDTO resultUser = usersService.editUser(userId, userApiDTO);
    }

    /**
     * Testing that the creation of a user is successful, comparing
     * the data passed into the create with the result data returned.
     * @throws Exception when the service fails to create the user
     */
    @Test
    public void testCreateUser() throws Exception {
        final String userName = "test1";
        final String userType = "DedicatedCustomer";
        final String userRole = "observer";
        final String userLoginProvider = "LOCAL";

        logon("admin");
        UserApiDTO userApiDTO = new UserApiDTO();
        userApiDTO.setUsername(userName);
        userApiDTO.setPassword(userName);
        userApiDTO.setType(userType);
        userApiDTO.setRoles(makeRole(userRole));
        userApiDTO.setLoginProvider(userLoginProvider);

        final HttpEntity<AuthUserDTO> entity = new HttpEntity<>(composeHttpHeaders());
        final ResponseEntity<String> responseEntity = new ResponseEntity<>( "234256", HttpStatus.OK);
        when(restTemplate.exchange(eq("http://:0/users/add"), any(), any(), eq(String.class)))
                .thenReturn(responseEntity);
        UserApiDTO resultUser = usersService.createUser(userApiDTO);

        // Verify that the data in the result user is the same as the input
        assertEquals(userApiDTO.getUsername(), resultUser.getUsername());
        assertEquals(userApiDTO.getType(), resultUser.getType());
        assertEquals(userApiDTO.getLoginProvider(), resultUser.getLoginProvider());
        assertEquals(userApiDTO.getRoleName(), resultUser.getRoleName());
        assertEquals(userApiDTO.getUsername(), resultUser.getDisplayName());
        assertFalse(resultUser.getUuid().isEmpty());
    }

    /**
     * Testing that editing a user with updated password returns the modified user
     * data back.
     * @throws Exception when the service fails to edit the user
     */
    @Test
    public void testEditUserWithPasswordUpdated() throws Exception {
        verifyEditUser(true);
    }

    /**
     * Testing that editing a user without updating password returns the modified user
     * data back.
     * @throws Exception when the service fails to edit the user
     */
    @Test
    public void testEditUserWithoutUpdatingPassword() throws Exception {
        verifyEditUser(false);
    }

    private List<RoleApiDTO> makeRole(String roleName) {
        RoleApiDTO role = new RoleApiDTO();
        role.setName(roleName);
        return Collections.singletonList(role);
    }

    private void verifyEditUser(boolean passwordChanged) throws Exception {
        final String userName = "test2";
        final String userId = "123456";
        final String userType = "DedicatedCustomer";
        final String userRole = "observer";
        final String userLoginProvider = "LOCAL";

        logon("admin");
        UserApiDTO userApiDTO = new UserApiDTO();
        userApiDTO.setUsername(userName);
        userApiDTO.setType(userType);
        userApiDTO.setRoles(makeRole(userRole));
        userApiDTO.setLoginProvider(userLoginProvider);
        if (passwordChanged) {
            userApiDTO.setPassword(userName);
        }

        final ResponseEntity<String> responseEntity = new ResponseEntity<>( "234256", HttpStatus.OK);
        when(restTemplate.exchange(eq("http://:0/users/setroles"), eq(HttpMethod.PUT), any(), eq(String.class)))
                .thenReturn(responseEntity);
        when(restTemplate.exchange(eq("http://:0/users/setpassword"), eq(HttpMethod.PUT), any(),
                eq(Void.class))).thenReturn(new ResponseEntity<Void>(HttpStatus.OK));
        UserApiDTO resultUser = usersService.editUser(userId, userApiDTO);

        // Verify that the data in the result user is the same as the input
        // including the id
        assertEquals(userApiDTO.getUsername(), resultUser.getUsername());
        assertEquals(userApiDTO.getType(), resultUser.getType());
        assertEquals(userApiDTO.getLoginProvider(), resultUser.getLoginProvider());
        assertEquals(userApiDTO.getRoleName(), resultUser.getRoleName());
        assertEquals(userApiDTO.getUsername(), resultUser.getDisplayName());
        assertEquals(userId, resultUser.getUuid());
    }

    private HttpHeaders composeHttpHeaders() {
        HttpHeaders headers = new HttpHeaders();
        headers.setAccept(HTTP_ACCEPT);
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.set(AUTH_HEADER_NAME,
            geJwtTokenFromSpringSecurityContext().orElseThrow(() ->
                new SecurityException("Invalid JWT token")));
        return headers;
    }

    private static Optional<String> geJwtTokenFromSpringSecurityContext() {
        return SAMLUserUtils
            .getAuthUserDTO()
            .map(AuthUserDTO::getToken);
    }

    private void setupGetAd() {
        final String request = UriComponentsBuilder.newInstance()
                .scheme("http")
                .host("")
                .port(0)
                .path("/users/ad")
                .build().toUriString();
        ResponseEntity<List<ActiveDirectoryDTO>> expectedPesponse = new ResponseEntity<>(ImmutableList.of(new ActiveDirectoryDTO(CORP_VMTURBO_COM, LDAP_DELL_1_VMTURBO_COM, true)), HttpStatus.OK);
        Mockito.when(restTemplate.exchange(Matchers.eq(request),
                Matchers.eq(HttpMethod.GET),
                Matchers.<HttpEntity>any(),
                Matchers.<Class<List<ActiveDirectoryDTO>>>any())).thenReturn(expectedPesponse);
    }

    private void verifyAdResponse(ActiveDirectoryApiDTO responseDto) {
        assertEquals(CORP_VMTURBO_COM, responseDto.getDomainName());
        assertEquals(LDAP_DELL_1_VMTURBO_COM, responseDto.getLoginProviderURI());
        assertEquals(true, responseDto.getIsSecure());
        assertEquals(1, responseDto.getGroups().size());
    }

    private ActiveDirectoryApiDTO getCreateAdInputDto() throws Exception {
        ActiveDirectoryApiDTO inputDto = new ActiveDirectoryApiDTO();
        inputDto.setDomainName(CORP_VMTURBO_COM);
        inputDto.setGroups(ImmutableList.of(getActiveDirectoryGroupApiDTO(AD_GROUP_NAME, AD_GROUP_TYPE, AD_GROUP_ROLE_NAME)));
        inputDto.setIsSecure(true);
        inputDto.setLoginProviderURI(LDAP_DELL_1_VMTURBO_COM);
        return inputDto;
    }

    private void setupGetAdGroup() {
        final String adGroupName = "VPNUsers";
        final String adGroupType = "DedicatedCustomer";
        final String adGroupRoleName = "observer";

        HttpHeaders headers = new HttpHeaders();
        headers.setAccept(HTTP_ACCEPT);
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.set(AUTH_HEADER_NAME, "token");
        HttpEntity<List> entity = new HttpEntity<>(headers);

        final String authRequest = UriComponentsBuilder.newInstance()
                .scheme("http")
                .host("")
                .port(0)
                .path("/users/ad/groups")
                .build().toUriString();

        ResponseEntity<List> adGroupResponse = new ResponseEntity<>(ImmutableList.of(
                new SecurityGroupDTO(adGroupName, adGroupType, adGroupRoleName)), HttpStatus.OK);
        Mockito.when(restTemplate.exchange(authRequest, HttpMethod.GET, entity, List.class)).thenReturn(adGroupResponse);
    }

    private ResponseEntity<SecurityGroupDTO> getResponse() {
        return new ResponseEntity<>(new SecurityGroupDTO(AD_GROUP_NAME, AD_GROUP_TYPE, AD_GROUP_ROLE_NAME), HttpStatus.OK);
    }

    private ActiveDirectoryGroupApiDTO getActiveDirectoryGroupApiDTO(String adGroupName, String adGroupType, String adGroupRoleName) {
        ActiveDirectoryGroupApiDTO adGroupFromRequest = new ActiveDirectoryGroupApiDTO();
        adGroupFromRequest.setRoleName(adGroupRoleName);
        adGroupFromRequest.setScope(Collections.EMPTY_LIST);
        adGroupFromRequest.setType(adGroupType);
        adGroupFromRequest.setDisplayName(adGroupName);
        return adGroupFromRequest;
    }

    private void verifyResponse(String adGroupName, String uuid, String adGroupName2, String displayName,
                                String adGroupType, String type, String adGroupRoleName, String roleName) {
        // check uuid and other fields are set
        assertEquals(adGroupName, uuid);
        assertEquals(adGroupName2, displayName);
        assertEquals(adGroupType, type);
        assertEquals(adGroupRoleName, roleName);
    }

    private void verifyAdUser(UserApiDTO resultUser) {
        assertEquals(TEST_USER, resultUser.getDisplayName());
        assertEquals(OBSERVER, resultUser.getRoleName());
        assertEquals(DEDICATED_CUSTOMER, resultUser.getType());
        assertEquals(LDAP, resultUser.getLoginProvider());
        assertFalse(resultUser.getUuid().isEmpty());
    }

    private UserApiDTO setupUserApiDTO() {
        UserApiDTO userApiDTO = new UserApiDTO();
        userApiDTO.setUsername(TEST_USER);
        userApiDTO.setType(DEDICATED_CUSTOMER);
        userApiDTO.setRoles(makeRole(OBSERVER));
        userApiDTO.setLoginProvider(LDAP);
        final HttpEntity<AuthUserDTO> entity = new HttpEntity<>(composeHttpHeaders());
        final ResponseEntity<String> responseEntity = new ResponseEntity<>("234256", HttpStatus.OK);
        when(restTemplate.exchange(eq("http://:0/users/add"), any(), any(), eq(String.class)))
                .thenReturn(responseEntity);
        return userApiDTO;
    }

    private void validateADInputs(Function<ActiveDirectoryGroupApiDTO, ActiveDirectoryGroupApiDTO> function) {
        ActiveDirectoryGroupApiDTO adGroupFromRequest = new ActiveDirectoryGroupApiDTO();
        try {
            function.apply(adGroupFromRequest);
            fail("IllegalArgumentException should have thrown.");
        } catch (IllegalArgumentException e) {
        }
        adGroupFromRequest.setRoleName(AD_GROUP_ROLE_NAME);
        try {
            function.apply(adGroupFromRequest);
            fail("IllegalArgumentException should have thrown.");
        } catch (IllegalArgumentException e) {
        }
        adGroupFromRequest.setType(DEDICATED_CUSTOMER);
        try {
            function.apply(adGroupFromRequest);
            fail("IllegalArgumentException should have thrown.");
        } catch (IllegalArgumentException e) {
        }
        adGroupFromRequest.setDisplayName(AD_GROUP_NAME);
        // it should pass now.
        function.apply(adGroupFromRequest);
    }

    /**
     * Verify "scope" is invalid when request provide "entities" instead of group. E.g.:
     *
     * @throws IllegalArgumentException if JSON string doesn't match Java object.
     *
     *         Scope list is not empty, uuids are valid, but 1 or more are entities instead of
     *         groups
     */
    //test
    private void validateInvalidADInputsNonEmptyEntities(
            Function<ActiveDirectoryGroupApiDTO, ActiveDirectoryGroupApiDTO> function) {
        ActiveDirectoryGroupApiDTO adGroupFromRequest = new ActiveDirectoryGroupApiDTO();
        adGroupFromRequest.setRoleName(AD_GROUP_ROLE_NAME);
        adGroupFromRequest.setType(DEDICATED_CUSTOMER);
        adGroupFromRequest.setRoleName(SHARED_ADVISOR);
        List scope = new ArrayList<>();
        GroupApiDTO groupApiDTO = new GroupApiDTO();
        GroupApiDTO groupApiDTO1 = new GroupApiDTO();
        groupApiDTO.setUuid(VALID_GROUP1);
        groupApiDTO1.setUuid(VALID_ENTITY);
        scope.add(groupApiDTO);
        scope.add(groupApiDTO1);
        ApiTestUtils.mockGroupId(VALID_GROUP1, uuidMapper);//is passed as string
        ApiTestUtils.mockEntityId(VALID_ENTITY, uuidMapper);//is passed as string
        adGroupFromRequest.setScope(scope);
        adGroupFromRequest.setDisplayName(AD_GROUP_NAME);
        function.apply(adGroupFromRequest);
    }

    /**
     * Verify "scope" is invalid when request provide "entities" instead of group. E.g.:
     *
     * @throws IllegalArgumentException if JSON string doesn't match Java object.
     *
     *         Scope list is not empty, uuids are valid, but 1 or more are entities instead of
     *         groups
     */

    private void validateInvalidADInputsInvalidUUID(
            Function<ActiveDirectoryGroupApiDTO, ActiveDirectoryGroupApiDTO> function)
            throws OperationFailedException {
        ActiveDirectoryGroupApiDTO adGroupFromRequest = new ActiveDirectoryGroupApiDTO();
        adGroupFromRequest.setRoleName(AD_GROUP_ROLE_NAME);
        adGroupFromRequest.setType(DEDICATED_CUSTOMER);
        adGroupFromRequest.setRoleName(SHARED_ADVISOR);
        List scope = new ArrayList<>();
        GroupApiDTO groupApiDTO = new GroupApiDTO();
        groupApiDTO.setUuid(INVALID_UUID);
        when(uuidMapper.fromUuid(INVALID_UUID)).thenThrow(IllegalArgumentException.class);
        scope.add(groupApiDTO);
        adGroupFromRequest.setScope(scope);
        adGroupFromRequest.setDisplayName(AD_GROUP_NAME);
        function.apply(adGroupFromRequest);
    }

    /**
     * Verify "scope" is invalid when request provide "empty List".
     *
     * @throws IllegalArgumentException if JSON string doesn't match Java object.
     */
    private void validateInvalidADInputsEmptyScopeList(
            Function<ActiveDirectoryGroupApiDTO, ActiveDirectoryGroupApiDTO> function) {
        ActiveDirectoryGroupApiDTO adGroupFromRequest = new ActiveDirectoryGroupApiDTO();
        adGroupFromRequest.setRoleName(AD_GROUP_ROLE_NAME);
        adGroupFromRequest.setType(DEDICATED_CUSTOMER);
        adGroupFromRequest.setRoleName(SHARED_OBSERVER);
        List scope = new ArrayList<>();
        adGroupFromRequest.setScope(scope);
        adGroupFromRequest.setDisplayName(AD_GROUP_NAME);
        function.apply(adGroupFromRequest);
    }

    /**
     * Verify "scope" is invalid when request provide "one empty UUID".
     *
     * @throws IllegalArgumentException if JSON string doesn't match Java object.
     */
    //check
    private void validateInvalidADInputsOneEmptyUuid(
            Function<ActiveDirectoryGroupApiDTO, ActiveDirectoryGroupApiDTO> function) {
        ActiveDirectoryGroupApiDTO adGroupFromRequest = new ActiveDirectoryGroupApiDTO();
        adGroupFromRequest.setRoleName(AD_GROUP_ROLE_NAME);
        adGroupFromRequest.setType(DEDICATED_CUSTOMER);
        adGroupFromRequest.setRoleName(SHARED_ADVISOR);
        List scope = new ArrayList<>();
        GroupApiDTO groupApiDTO = new GroupApiDTO();
        GroupApiDTO groupApiDTO1 = new GroupApiDTO();
        groupApiDTO.setUuid(VALID_GROUP1);
        //Empty UUID
        scope.add(groupApiDTO);
        scope.add(groupApiDTO1);
        ApiTestUtils.mockGroupId(VALID_GROUP1, uuidMapper);
        adGroupFromRequest.setScope(scope);
        adGroupFromRequest.setDisplayName(AD_GROUP_NAME);
        function.apply(adGroupFromRequest);
    }

    /**
     * Verify "scope" is invalid when request provide "empty uuids".
     *
     * @throws IllegalArgumentException if JSON string doesn't match Java object.
     */
    private void validateInvalidADInputsEmptyUuids(
            Function<ActiveDirectoryGroupApiDTO, ActiveDirectoryGroupApiDTO> function) {
        ActiveDirectoryGroupApiDTO adGroupFromRequest = new ActiveDirectoryGroupApiDTO();
        adGroupFromRequest.setRoleName(AD_GROUP_ROLE_NAME);
        adGroupFromRequest.setType(DEDICATED_CUSTOMER);
        adGroupFromRequest.setRoleName(SHARED_ADVISOR);
        List scope = new ArrayList<>();
        GroupApiDTO groupApiDTO = new GroupApiDTO();
        GroupApiDTO groupApiDTO1 = new GroupApiDTO();
        scope.add(groupApiDTO);
        scope.add(groupApiDTO1);
        adGroupFromRequest.setScope(scope);
        adGroupFromRequest.setDisplayName(AD_GROUP_NAME);
        function.apply(adGroupFromRequest);
    }

    /**
     * Verify "scope" is invalid when request provide "empty uuids".
     *
     * @throws IllegalArgumentException if JSON string doesn't match Java object.
     */
    private void validateInvalidADInputsEmptyStringUuid(
            Function<ActiveDirectoryGroupApiDTO, ActiveDirectoryGroupApiDTO> function) {
        ActiveDirectoryGroupApiDTO adGroupFromRequest = new ActiveDirectoryGroupApiDTO();
        adGroupFromRequest.setRoleName(AD_GROUP_ROLE_NAME);
        adGroupFromRequest.setType(DEDICATED_CUSTOMER);
        adGroupFromRequest.setRoleName(SHARED_ADVISOR);
        List scope = new ArrayList<>();
        GroupApiDTO groupApiDTO = new GroupApiDTO();
        GroupApiDTO groupApiDTO1 = new GroupApiDTO();
        groupApiDTO.setUuid(VALID_GROUP1);
        groupApiDTO1.setUuid("");
        scope.add(groupApiDTO);
        scope.add(groupApiDTO1);
        //check if its a valid group
        ApiTestUtils.mockGroupId(VALID_GROUP1, uuidMapper);
        adGroupFromRequest.setScope(scope);
        adGroupFromRequest.setDisplayName(AD_GROUP_NAME);
        function.apply(adGroupFromRequest);
    }

    /**
     * Verify "scope" is invalid when request provide "valid group uuids".
     */
    private void validateValidADInputs(
            Function<ActiveDirectoryGroupApiDTO, ActiveDirectoryGroupApiDTO> function)
            throws OperationFailedException {
        ActiveDirectoryGroupApiDTO adGroupFromRequest = new ActiveDirectoryGroupApiDTO();
        adGroupFromRequest.setRoleName(AD_GROUP_ROLE_NAME);
        adGroupFromRequest.setType(DEDICATED_CUSTOMER);
        adGroupFromRequest.setRoleName(SHARED_ADVISOR);
        List scope = new ArrayList<>();
        GroupApiDTO groupApiDTO = new GroupApiDTO();
        GroupApiDTO groupApiDTO1 = new GroupApiDTO();
        groupApiDTO.setUuid(VALID_GROUP);
        groupApiDTO1.setUuid(VALID_GROUP1);
        scope.add(groupApiDTO);
        scope.add(groupApiDTO1);
        ApiTestUtils.mockGroupId(VALID_GROUP, uuidMapper);//is passed as string
        ApiTestUtils.mockGroupId(VALID_GROUP1, uuidMapper);//is passed as string
        adGroupFromRequest.setScope(scope);
        adGroupFromRequest.setDisplayName(AD_GROUP_NAME);
        function.apply(adGroupFromRequest);
    }

    /**
     * Verify "rolename" is in the response when request only provide "rolename". E.g.:
     * "username":"test","password":"1","type":"DedicatedCustomer","loginProvider":"LOCAL","roleName":"site_admin"}
     *
     * @throws JsonProcessingException if JSON string doesn't match Java object.
     */
    @Test
    public void testPopulateResultUserApiDTOFromInput() throws JsonProcessingException {
        final ObjectMapper objectMapper = new ObjectMapper();
        final String newUserJson =
                "{\"username\":\"test\",\"password\":\"1\",\"type\":\"DedicatedCustomer\"," +
                        "\"loginProvider\":\"LOCAL\",\"roleName\":\"site_admin\"}";
        final UserApiDTO dto = objectMapper.readValue(newUserJson, UserApiDTO.class);
        final UserApiDTO responseDto = usersService.populateResultUserApiDTOFromInput(dto);
        assertEquals("site_admin", responseDto.getRoleName());
    }

    private void logout() {
        SecurityContextHolder.getContext().setAuthentication(null);
    }
}
