package com.vmturbo.api.component.external.api.service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.tools.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.session.SessionInformation;
import org.springframework.security.core.session.SessionRegistry;
import org.springframework.validation.Errors;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import com.vmturbo.api.component.communication.RestAuthenticationProvider;
import com.vmturbo.api.component.external.api.mapper.LoginProviderMapper;
import com.vmturbo.api.component.external.api.mapper.UserMapper;
import com.vmturbo.api.component.external.api.util.ReportingUserCalculator;
import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.group.GroupApiDTO;
import com.vmturbo.api.dto.user.ActiveDirectoryApiDTO;
import com.vmturbo.api.dto.user.ActiveDirectoryGroupApiDTO;
import com.vmturbo.api.dto.user.ChangePasswordApiDTO;
import com.vmturbo.api.dto.user.PropertyValueApiDTO;
import com.vmturbo.api.dto.user.SAMLIdpApiDTO;
import com.vmturbo.api.dto.user.UserApiDTO;
import com.vmturbo.api.exceptions.ConversionException;
import com.vmturbo.api.exceptions.UnauthorizedObjectException;
import com.vmturbo.api.serviceinterfaces.IUsersService;
import com.vmturbo.auth.api.auditing.AuditAction;
import com.vmturbo.auth.api.auditing.AuditLog;
import com.vmturbo.auth.api.authentication.credentials.SAMLUserUtils;
import com.vmturbo.auth.api.usermgmt.ActiveDirectoryDTO;
import com.vmturbo.auth.api.usermgmt.AuthUserDTO;
import com.vmturbo.auth.api.usermgmt.AuthUserDTO.PROVIDER;
import com.vmturbo.auth.api.usermgmt.AuthUserModifyDTO;
import com.vmturbo.auth.api.usermgmt.SecurityGroupDTO;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupFilter;
import com.vmturbo.common.protobuf.topology.ApiEntityType;

/**
 * Users management service implementation.
 * Provides the implementation for the local users and Active Directory management:
 * <ul>
 * <li>Define different Active Directory domains</li>
 * <li>Add/Modify/Remove local users</li>
 * <li>Add/Modify/Remove Active Directory users</li>
 * <li>Add/Modify/Remove Active Directory groups</li>
 * </ul>
 */
public class UsersService implements IUsersService {

    /**
     * The HTTP accept header.
     */
    public static final List<MediaType> HTTP_ACCEPT = ImmutableList.of(MediaType.APPLICATION_JSON);
    private static final String SAML_IDP_ENTITY_NAME = "SAML IDP entity name: ";
    private static final String NOT_ASSIGNED = "Not assigned";
    private static final String PERMISSION_CHANGED = "Permission changed, current role is %s, scope is %s";
    private final Set<String> invalidScopes = new HashSet<>();

    /**
     * The logger.
     */
    private final Logger logger_ = LogManager.getLogger(UsersService.class);

    /**
     * The synchronous client-side HTTP access.
     */
    private final RestTemplate restTemplate_;

    /**
     * The auth service host.
     */
    private final String authHost_;

    /**
     * The auth service port.
     */
    private final int authPort_;

    /**
     * The GSON parser/builder.
     */
    private static final Gson GSON_ = new GsonBuilder().create();
    private final String registrationId;
    private final boolean samlEnabled;

    // Uses to expire deleted user active sessions
    @Autowired
    private SessionRegistry sessionRegistry;

    private final GroupsService groupsService;

    private final WidgetSetsService widgetsetsService;

    private final ReportingUserCalculator reportingUserCalculator;

    /**
     * Constructs the users service.
     * @param authHost     The authentication host.
     * @param authPort     The authentication port.
     * @param restTemplate The synchronous client-side HTTP access.
     * @param samlRegistrationID       The SAML registration ID.
     * @param samlEnabled  is SAML enabled
     * @param groupsService The group service is used when translating scope groups back to API groups
     * @param widgetsetsService the widgetset service service to transfer the widget ownership
     * @param reportingUserCalculator The utility class to use to figure out the reports-specific user.
     */
    public UsersService(final @Nonnull String authHost,
                        final int authPort,
                        final @Nonnull RestTemplate restTemplate,
                        final @Nonnull String samlRegistrationID,
                        final boolean samlEnabled,
                        final @Nonnull GroupsService groupsService,
                        final @Nonnull WidgetSetsService widgetsetsService,
                        final @Nonnull ReportingUserCalculator reportingUserCalculator) {
        authHost_ = Objects.requireNonNull(authHost);
        authPort_ = authPort;
        if (authPort_ < 0 || authPort_ > 65535) {
            throw new IllegalArgumentException("Invalid AUTH port.");
        }
        restTemplate_ = Objects.requireNonNull(restTemplate);
        this.groupsService = groupsService;
        this.widgetsetsService = widgetsetsService;
        // users cannot be created with the following group scopes
        this.invalidScopes.add(ApiEntityType.BUSINESS_ACCOUNT.displayName());
        this.invalidScopes.add(ApiEntityType.REGION.displayName());
        this.invalidScopes.add(ApiEntityType.AVAILABILITY_ZONE.displayName());
        this.invalidScopes.add("ResourceGroup");
        this.registrationId = Objects.requireNonNull(samlRegistrationID);
        this.samlEnabled = samlEnabled;
        this.reportingUserCalculator = reportingUserCalculator;
    }

    /**
     * Composes the HTTP headers for REST calls.
     *
     * @return The HTTP headers.
     */
    private HttpHeaders composeHttpHeaders() {
        HttpHeaders headers = new HttpHeaders();
        headers.setAccept(HTTP_ACCEPT);
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.set(RestAuthenticationProvider.AUTH_HEADER_NAME,
                geJwtTokenFromSpringSecurityContext().orElseThrow(() ->
                        new SecurityException("Invalid JWT token")));
        return headers;
    }

    /**
     * Parses the not fully parsed JSON object and creates the object of the class T.
     *
     * @param o The object to extract from
     * @param clazz The class of the return type
     * @param <T> the type of the return object
     * @return The fully parsed JSON object.
     */
    private <T> T parse(final @Nonnull Object o, final @Nonnull Class<T> clazz) {
        return GSON_.fromJson(GSON_.toJson(o), clazz);
    }

    /**
     * Returns list of all users.
     *
     * @return The list of all users.
     * @throws Exception In case of any error.
     */
    @Override
    public @Nonnull List<UserApiDTO> getUsers() throws Exception {
        // Ask for the list.
        final String request = baseRequest().path("/users").build().toUriString();
        HttpHeaders headers = composeHttpHeaders();
        HttpEntity<List> entity = new HttpEntity<>(headers);
        ResponseEntity<List> result = restTemplate_.exchange(request, HttpMethod.GET, entity,
                                                             List.class);

        // assemble a list of auth users to convert
        List<AuthUserDTO> authUserDTOS = new ArrayList<>();
        // also gather a set of the group oids we need to fetch from the group service
        Set<Long> groupOids = new HashSet<>();
        for (Object o : result.getBody()) {
            // We do the conversion manually here from the result, as we can't specify the
            // exact class for automatic JSON generation.
            AuthUserDTO dto = parse(o, AuthUserDTO.class);
            authUserDTOS.add(dto);
            // if there are any user scopes, add the groups to the group request.
            if (CollectionUtils.isNotEmpty(dto.getScopeGroups())) {
                groupOids.addAll(dto.getScopeGroups());
            }
        }

        // oid -> group map for looking up API Group objects by oid. We will use this when mapping
        // between API and Auth user objects later.
        Map<Long, GroupApiDTO> apiGroupsByOid = getApiGroupMap(groupOids);

        // Assemble the final results.
        List<UserApiDTO> list = new ArrayList<>();
        for (AuthUserDTO authUserDTO : authUserDTOS) {
            // We do the conversion manually here from the result, as we can't specify the
            // exact class for automatic JSON generation.
            UserApiDTO user = UserMapper.toUserApiDTO(authUserDTO, apiGroupsByOid);
            list.add(user);
        }
        return list;
    }

    /**
     * Get JWT token from Spring context.
     *
     * @return JWT token
     */
    private static Optional<String> geJwtTokenFromSpringSecurityContext() {
        return SAMLUserUtils
                .getAuthUserDTO()
                .map(AuthUserDTO::getToken);
    }

    @Override
    public @Nonnull LoggedInUserInfo getLoggedInUser() throws Exception {
        UserApiDTO me = SAMLUserUtils.getAuthUserDTO()
                .map(UserMapper::toUserApiDTO)
                .orElseThrow(() -> new UnauthorizedObjectException("No user logged in!"));
        return reportingUserCalculator.getMe(me);
    }

    /**
     * Supposed to retrieve an user information.
     *
     * @param uuid The UUID.
     * @return The user.
     * @throws Exception Throws the {@link UnsupportedOperationException} always.
     */
    @Override
    public UserApiDTO getUser(String uuid) throws Exception {
        throw new UnsupportedOperationException("Doesn't appear to be invoked.");
    }

    /**
     * Creates the user.
     *
     * @param userApiDTO The user creation data.
     * @return The created user information.
     * @throws Exception In case of any error creating the user.
     */
    @Override
    public UserApiDTO createUser(UserApiDTO userApiDTO) throws Exception {
        try {
            AuthUserDTO dto = UserMapper.toAuthUserDTO(userApiDTO);
            validateUserInput(userApiDTO);
            // Only check the password if this is a local users.  Other user types do not require
            // a password.
            if (userApiDTO.getLoginProvider() != null &&
                    LoginProviderMapper.fromApi(userApiDTO.getLoginProvider()).equals(PROVIDER.LOCAL) &&
                    StringUtils.isBlank(userApiDTO.getPassword())) {
                throw new IllegalArgumentException("User password is empty.");
            }

            // Perform the call.
            // Make sure that the currently authenticated user's token is present.
            HttpHeaders headers = composeHttpHeaders();
            HttpEntity<AuthUserDTO> entity = new HttpEntity<>(dto, headers);
            final ResponseEntity<String> result = restTemplate_.exchange(baseRequest().path("/users/add").build().toUriString(),
                HttpMethod.POST, entity, String.class);
            // Return data.
            UserApiDTO user = populateResultUserApiDTOFromInput(userApiDTO);
            user.setUuid(result.getBody());
            AuditLog.newEntry(AuditAction.CREATE_USER,
                String.format("Created new user %s", userApiDTO.getUsername()), true)
                .targetName(userApiDTO.getUsername())
                .audit();
            return user;
        } catch (RuntimeException e) {
            // Intercept the possible exception for auditing
            AuditLog.newEntry(AuditAction.CREATE_USER,
                String.format("Failed to create user %s", userApiDTO.getUsername()), false)
                .targetName(userApiDTO.getUsername())
                .audit();
            // rethrowing it
            throw e;
        }
    }

    /**
     * Replaces user's roles.
     *
     * @param userApiDTO The User's data object.
     * @return The user's object with bare minimum of information.
     * @throws Exception In the case of any error performing the user's data modification.
     */
    private UserApiDTO setUserRoles(final @Nonnull UserApiDTO userApiDTO) throws Exception {
        try {
            UriComponentsBuilder builder = baseRequest().path("/users/setroles");
            AuthUserDTO dto = UserMapper.toAuthUserDTONoPassword(userApiDTO);
            // Call AUTH component to perform the action.
            // Make sure that the currently authenticated user's token is present.
            HttpHeaders headers = composeHttpHeaders();
            HttpEntity<AuthUserDTO> entity = new HttpEntity<>(dto, headers);
            // spring throws exception for error HttpStatus code like 4xx and 5xx, which will be
            // handled in GlobalExceptionHandler
            restTemplate_.exchange(builder.build().toUriString(), HttpMethod.PUT, entity, String.class);
            // Return data.
            UserApiDTO user = populateResultUserApiDTOFromInput(userApiDTO);

            final String details = String.format(PERMISSION_CHANGED,
                userApiDTO.getRoleName() != null ? userApiDTO.getRoleName().toLowerCase() : "",
                userApiDTO.getScope() != null && !userApiDTO.getScope().isEmpty() ?
                    userApiDTO.getScope().toString().toLowerCase() : NOT_ASSIGNED);
            AuditLog.newEntry(AuditAction.CHANGE_ROLE,
                details, true)
                .targetName(userApiDTO.getUsername())
                .audit();
            return user;
        } catch (RuntimeException e) {
            // Intercept the possible exception for auditing
            final String details = "Failed to change permissions";
            AuditLog.newEntry(AuditAction.CHANGE_ROLE,
                details, false)
                .targetName(userApiDTO.getUsername())
                .audit();
            // rethrowing it
            throw e;
        }
    }

    /**
     * Edits user's password.
     * It is only applicable to the local users, and AUTH component will ensure that.
     *
     * @param userApiDTO The User's data object.
     * @return The user's object with bare minimum of information.
     * @throws Exception In the case of any error performing the user's data modification.
     */
    private UserApiDTO setLocalUserPassword(final @Nonnull UserApiDTO userApiDTO) throws Exception {
        try {
            UriComponentsBuilder builder = baseRequest().path("/users/setpassword");
            // Call AUTH component to perform the action.
            AuthUserModifyDTO dto = new AuthUserModifyDTO(UserMapper.toAuthUserDTONoPassword(userApiDTO),
                userApiDTO.getPassword());

            // Perform the call.
            // Make sure that the currently authenticated user's token is present.
            HttpHeaders headers = composeHttpHeaders();
            HttpEntity<AuthUserModifyDTO> entity = new HttpEntity<>(dto, headers);
            restTemplate_.exchange(builder.build().toUriString(), HttpMethod.PUT, entity,
                Void.class);
            // Return data.
            UserApiDTO user = populateResultUserApiDTOFromInput(userApiDTO);
            final String details = String.format("User %s password changed", userApiDTO.getUsername());
            AuditLog.newEntry(AuditAction.CHANGE_PASSWORD,
                details, true)
                .targetName(userApiDTO.getUsername())
                .audit();
            return user;
        } catch (RuntimeException e) {
            final String details = "Failed to change user password";
            AuditLog.newEntry(AuditAction.CHANGE_PASSWORD,
                details, false)
                .targetName(userApiDTO.getUsername())
                .audit();
            throw e;
        }
    }

    /**
     * When creating and editing a user, the result api dto is formed from the
     * input dto and then fields are modified.  This method copies basic data
     * from the input dto to the output.
     * @param inputDto input data sent from the user to add or update a user
     * @return a UserApiDTO with basic fields populated from the inputDto
     */
    private UserApiDTO populateResultUserApiDTOFromInput(@Nonnull UserApiDTO inputDto) {
        UserApiDTO user = new UserApiDTO();
        user.setUsername(inputDto.getUsername());
        user.setRoleName(inputDto.getRoleName());
        user.setScope(inputDto.getScope());
        user.setUuid(inputDto.getUuid());
        user.setLoginProvider(inputDto.getLoginProvider());
        user.setDisplayName(inputDto.getUsername());
        user.setType(inputDto.getType());
        return user;
    }


    /**
     * Edits user information.
     * We always receive the role in the userApiDTO.
     * The password in the userApiDTO will be non-{@code null} if the password modification is
     * required.
     *
     * @param uuid       The user's UUID.
     * @param userApiDTO The User's data object.
     * @return The user's object with bare minimum of information.
     * @throws Exception In the case of any error performing the user's data modification.
     */
    @Override
    public UserApiDTO editUser(String uuid, UserApiDTO userApiDTO) throws Exception {
        // make sure there is a valid input before updating.
        validateUserInput(userApiDTO);
        if (!StringUtils.isBlank(userApiDTO.getUuid()) && !userApiDTO.getUuid().equals(uuid)) {
            throw new IllegalArgumentException("The user id passed as a parmaeter and in the input data must match.");
        }
        userApiDTO.setUuid(uuid);
        UserApiDTO dto = setUserRoles(userApiDTO);
        if (LoginProviderMapper.fromApi(userApiDTO.getLoginProvider()).equals(PROVIDER.LOCAL)) {
            // We change the password only if requested.
            if (userApiDTO.getPassword() != null) {
                dto = setLocalUserPassword(userApiDTO);
            }
        }
        return dto;
    }

    /**
     * Validate that the user api dto specified has a valid user name, type, and roleName
     *
     * @param userApiDTO dto representing the user
     */
    private void validateUserInput(UserApiDTO userApiDTO) {
        if (StringUtils.isBlank(userApiDTO.getUsername())) {
            throw new IllegalArgumentException("No user name specified for user.");
        }
        if (StringUtils.isBlank(userApiDTO.getRoleName())) {
            throw new IllegalArgumentException("No role specified for user.");
        }
        if (StringUtils.isBlank(userApiDTO.getType())) {
            throw new IllegalArgumentException("No type specified for user.");
        }

        if (userApiDTO.getScope() != null && !userApiDTO.getScope().isEmpty()) {
            if (!isUserScopeAllowed(userApiDTO.getScope())) {
                throw new IllegalArgumentException("Scope not allowed for user.");
            }
        }
    }

    /**
     * Check if scope set for an user account is valid.
     * @param scope - List of scopes set for an user account
     * @return - If the scope is allowed
     */
    public Boolean isUserScopeAllowed(List<GroupApiDTO> scope) {
        for (GroupApiDTO dto : scope) {
            if (this.invalidScopes.contains(dto.getGroupType())) {
                return false;
            }
        }
        return true;
    }

    /**
     * Deletes the user.
     *
     * @param uuid The UUID.
     * @return {@code true} iff the user has been deleted successfully.
     */
    @Override
    public Boolean deleteUser(String uuid) {
        try {
            String request = baseRequest().path("/users/remove/" + uuid).build().toUriString();
            HttpEntity<AuthUserDTO> entity = new HttpEntity<>(composeHttpHeaders());
            String userName;
            try {
                final ResponseEntity<AuthUserDTO> result = restTemplate_.exchange(request,
                    HttpMethod.DELETE, entity, AuthUserDTO.class);
                userName = result.getBody() != null ? result.getBody().getUser() : "";
                final String details = String.format("Deleted user %s", userName);
                AuditLog.newEntry(AuditAction.DELETE_USER,
                    details, true)
                    .targetName(userName)
                    .audit();
            } catch (Exception e) {
                logger_.error("Unable to remove user {}", uuid, e.getCause());
                throw new IllegalArgumentException("Unable to remove user " + uuid, e.getCause());
            }
            widgetsetsService.transferWidgetsets(uuid, userName);
            expireActiveSessions(uuid);

            return Boolean.TRUE;
        } catch (RuntimeException e) {
            final String details = String.format("Failed to delete user %s", uuid);
            AuditLog.newEntry(AuditAction.DELETE_USER, details, false)
                .targetName(uuid)
                .audit();
            throw e;
        }
    }

    // Expire active sessions
    private void expireActiveSessions(@Nonnull final String uuid) {
        for (Object principal : sessionRegistry.getAllPrincipals()) {
            if (principal instanceof String) {
                final String userUuid = (String)principal;
                if (uuid.equals(userUuid)) {
                    sessionRegistry.getAllSessions(principal, false)
                            .forEach(SessionInformation::expireNow);
                    if (logger_.isDebugEnabled()) {
                        logger_.debug("Expired active sessions for user with UUID: " + uuid);
                    }
                }
            }
        }
    }

    /**
     * Returns {@link Boolean#FALSE} if the value is {@code null}, or the value itself if the
     * value is non-{@code null}.
     *
     * @param value The value.
     * @return The {@link Boolean#FALSE} if the value is {@code null}, or the value itself if the
     * value is non-{@code null}.
     */
    private @Nonnull Boolean ensureNonNull(final @Nullable Boolean value) {
        if (value == null) {
            return Boolean.FALSE;
        }
        return value;
    }

    /**
     * Converts the AD DTO from API to internal format.
     * We use internal in the AUTH component because we want to avoid the dependency on the API
     * layer.
     *
     * @param adDTO The API format DTO.
     * @return The internal format DTO.
     */
    @Nonnull
    @VisibleForTesting
    ActiveDirectoryDTO convertADInfoToAuth(final @Nonnull ActiveDirectoryApiDTO adDTO) {
        ActiveDirectoryDTO dto = new ActiveDirectoryDTO(
                StringUtils.isBlank(adDTO.getDomainName()) ? null : adDTO.getDomainName(),
                StringUtils.isBlank(adDTO.getLoginProviderURI()) ? null
                        : adDTO.getLoginProviderURI(), ensureNonNull(adDTO.getIsSecure()));
        if (adDTO.getGroups() != null) {
            List<SecurityGroupDTO> groups = new ArrayList<>();
            for (ActiveDirectoryGroupApiDTO grp : adDTO.getGroups()) {
                groups.add(convertGroupInfoToAuth(grp));
            }
            dto.setGroups(groups);
        }
        return dto;
    }

    /**
     * Converts the AD DTO from internal to API format.
     * We use internal in the AUTH component because we want to avoid the dependency on the API
     * layer.
     *
     * @param adDTO The internal format DTO.
     * @return The API format DTO.
     * @throws ConversionException if error faced converting objects to API DTOs
     * @throws InterruptedException if current thread has been interrupted
     */
    private @Nonnull
    ActiveDirectoryApiDTO convertADInfoFromAuth(final @Nonnull ActiveDirectoryDTO adDTO)
            throws ConversionException, InterruptedException {
        ActiveDirectoryApiDTO dto = new ActiveDirectoryApiDTO();
        dto.setDomainName(adDTO.getDomainName());
        dto.setLoginProviderURI(adDTO.getLoginProviderURI());
        dto.setIsSecure(adDTO.isSecure());

        // External group is independent entity which is shared by AD and SAML
        List<ActiveDirectoryGroupApiDTO> groupsApi = getActiveDirectoryGroups();
        dto.setGroups(groupsApi);
        return dto;
    }

    /**
     * Converts the AD Group DTO between internal and API formats.
     * We use internal in the AUTH component because we want to avoid the dependency on the API
     * layer.
     *
     * @param dto The internal format DTO.
     * @param groupApiDTOMap a map of group oid -> {@link GroupApiDTO}. This will be used to convert
     *                       scope groups, if passed in. Otherwise, simple groups containing just the
     *                       group oid will be created instead.
     * @return The API format DTO.
     */
    private @Nonnull ActiveDirectoryGroupApiDTO convertGroupInfoFromAuth(
            final @Nonnull SecurityGroupDTO dto, Map<Long, GroupApiDTO> groupApiDTOMap) {
        ActiveDirectoryGroupApiDTO gad = new ActiveDirectoryGroupApiDTO();
        gad.setType(dto.getType());
        gad.setRoleName(dto.getRoleName());
        gad.setDisplayName(dto.getDisplayName());
        // get the set of group oids in scope
        gad.setScope(UserMapper.groupOidsToGroupApiDTOs(dto.getScopeGroups(), groupApiDTOMap));
        // use displayName as uuid of AD group (since it should be unique for one domain), and UI
        // is expecting this uuid to perform DELETE or other operations on this group
        gad.setUuid(dto.getDisplayName());
        return gad;
    }

    /**
     * Given a set of auth user or group objects, build a map of group id -> {@link GroupApiDTO}
     * objects so that we can provide the UI the group name and type information. Since we are only
     * starting with a group oid, this requires a fetch to the group service component.
     *
     * @param groupOids a set of OIDs for groups to be mapped
     * @return a map of group oid -> {@link GroupApiDTO} based on the input group oids.
     * @throws ConversionException if error faced converting objects to API DTOs
     * @throws InterruptedException if current thread has been interrupted
     */
    private Map<Long, GroupApiDTO> getApiGroupMap(Set<Long> groupOids)
            throws ConversionException, InterruptedException {
        if (CollectionUtils.isEmpty(groupOids)) {
            return Collections.emptyMap();
        }

        // get the groups from the group service and populate the oid -> group map
        Map<Long, GroupApiDTO> apiGroupsByOid = new HashMap<>();
        if (groupOids.size() > 0) {
            // We don't need the severities here.
            // We actually (probably) don't need the list of members here either - consider replacing
            // with some more minimal call.
            List<GroupApiDTO> groupApiDTOS = groupsService.getGroupApiDTOS(
                    GetGroupsRequest.newBuilder()
                            .setGroupFilter(GroupFilter.newBuilder()
                                            .addAllId(groupOids))
                            .build(), false);
            if (groupApiDTOS.size() > 0) {
                groupApiDTOS.forEach(
                        group -> apiGroupsByOid.put(Long.valueOf(group.getUuid()), group));
            }
        }
        return apiGroupsByOid;
    }

    /**
     * Converts the Group DTO between internal and API formats.
     * We use internal in the AUTH component because we want to avoid the dependency on the API
     * layer.
     *
     * @param dto The API format DTO.
     * @return The internal format DTO.
     */
    private @Nonnull
    SecurityGroupDTO convertGroupInfoToAuth(final @Nonnull ActiveDirectoryGroupApiDTO dto) {
        return new SecurityGroupDTO(dto.getDisplayName(), dto.getType(), dto.getRoleName(),
                UserMapper.groupApiDTOsToOids(dto.getScope()));
    }

    /**
     * Returns the list of active directories.
     *
     * @return The list of active directories.
     * @throws ConversionException if error faced converting objects to API DTOs
     * @throws InterruptedException if current thread has been interrupted
     */
    @Override
    public List<ActiveDirectoryApiDTO> getActiveDirectories()
            throws ConversionException, InterruptedException {
        UriComponentsBuilder builder = baseRequest().path("/users/ad");
        ResponseEntity<List> result;
        result = restTemplate_.exchange(builder.build().toUriString(), HttpMethod.GET,
                                        new HttpEntity<>(composeHttpHeaders()), List.class);
        List<ActiveDirectoryApiDTO> list = new ArrayList<>();
        for (Object o : result.getBody()) {
            list.add(convertADInfoFromAuth(parse(o, ActiveDirectoryDTO.class)));
        }
        // currently UI is getting the external group from LDAP, unless UI is updated, we need to
        // add groups to empty LDAP as in Legacy.
        if (list.size() == 0) {
            ActiveDirectoryApiDTO activeDirectoryApiDTO = new ActiveDirectoryApiDTO();
            activeDirectoryApiDTO.setGroups(getActiveDirectoryGroups());
            list.add(activeDirectoryApiDTO);
        }
        return list;
    }

    /**
     * Create the Active Directory representation.
     *
     * @param inputDTO The Active Directory description.
     * @return The Active Directory representation object.
     * @throws ConversionException if error faced converting objects to API DTOs
     * @throws InterruptedException if current thread has been interrupted
     */
    @Override
    public ActiveDirectoryApiDTO createActiveDirectory(final ActiveDirectoryApiDTO inputDTO)
            throws ConversionException, InterruptedException {
        validateActiveDirectoryInput(inputDTO);
        try {
            final String request = baseRequest().path("/users/ad").build().toUriString();
            HttpEntity<ActiveDirectoryDTO> entity = new HttpEntity<>(convertADInfoToAuth(inputDTO),
                composeHttpHeaders());
            Class<ActiveDirectoryDTO> clazz = ActiveDirectoryDTO.class;
            final String details = String.format("Set LDAP domain to: %s", inputDTO.getDomainName());
            AuditLog.newEntry(AuditAction.SET_LDAP,
                details, true)
                .targetName("LDAP SERVICE")
                .audit();
            return convertADInfoFromAuth(restTemplate_.exchange(request, HttpMethod.POST,
                entity, clazz).getBody());
        } catch (RuntimeException e) {
            final String details = String.format("Failed to set LDAP domain to: %s", inputDTO.getDomainName());
            AuditLog.newEntry(AuditAction.SET_LDAP,
                details, false)
                .targetName("LDAP SERVICE")
                .audit();
            throw e;
        }
    }

    /**
     * Returns the list of AD group objects.
     *
     * @return The list of AD group objects.
     * @throws ConversionException if error faced converting objects to API DTOs
     * @throws InterruptedException if current thread has been interrupted
     */
    @Override
    public List<ActiveDirectoryGroupApiDTO> getActiveDirectoryGroups()
            throws ConversionException, InterruptedException {
        String request = baseRequest().path("/users/ad/groups").build().toUriString();
        HttpHeaders headers = composeHttpHeaders();
        ResponseEntity<List> result;
        result = restTemplate_.exchange(request, HttpMethod.GET, new HttpEntity<>(headers),
                                        List.class);
        // get a list of API group objects and a set of any scope group oids they contain
        List<SecurityGroupDTO> apiGroups = new ArrayList<>();
        Set<Long> groupOids = new HashSet<>();
        for (Object o : result.getBody()) {
            SecurityGroupDTO group = parse(o, SecurityGroupDTO.class);
            apiGroups.add(group);
            if (CollectionUtils.isNotEmpty(group.getScopeGroups())) {
                groupOids.addAll(group.getScopeGroups());
            }
        }
        Map<Long, GroupApiDTO> groupApiDTOMap = getApiGroupMap(groupOids);
        List<ActiveDirectoryGroupApiDTO> list = new ArrayList<>();
        for (SecurityGroupDTO securityGroupDTO : apiGroups) {
            list.add(convertGroupInfoFromAuth(securityGroupDTO, groupApiDTOMap));
        }
        return list;
    }

    /**
     * Creates the Active Directory group.
     * Also changes it, as {@link #changeActiveDirectoryGroup(ActiveDirectoryGroupApiDTO)} is not
     * invoked by the UI.
     *
     * @param adGroupInputDto The Active Directory group creation request.
     * @return The {@link ActiveDirectoryGroupApiDTO} indicating success.
     */
    @Override
    public ActiveDirectoryGroupApiDTO createActiveDirectoryGroup(
            final ActiveDirectoryGroupApiDTO adGroupInputDto) {
        validateActiveDirectoryGroupInput(adGroupInputDto);
        try {
            HttpHeaders headers = composeHttpHeaders();
            HttpEntity<SecurityGroupDTO> entity;
            entity = new HttpEntity<>(convertGroupInfoToAuth(adGroupInputDto), headers);
            Class<SecurityGroupDTO> clazz = SecurityGroupDTO.class;
            final Map<Long, GroupApiDTO> groupApiDTOMap = buildGroupApiDtoMap(adGroupInputDto);
            String request = baseRequest().path("/users/ad/groups").build().toUriString();
            final String details = String.format("Created external group: %s with role: %s",
                    adGroupInputDto.getDisplayName(), adGroupInputDto.getRoleName());
            AuditLog.newEntry(AuditAction.CREATE_GROUP,
                    details, true)
                    .targetName("EXTERNAL GROUP")
                    .audit();
            return convertGroupInfoFromAuth(
                restTemplate_.exchange(request, HttpMethod.POST, entity, clazz).getBody(),
                groupApiDTOMap);
        } catch (RuntimeException e) {
            final String details = String.format("Failed to create external group: %s with role: %s",
                adGroupInputDto.getDisplayName(), adGroupInputDto.getRoleName());
            AuditLog.newEntry(AuditAction.CREATE_GROUP,
                details, false)
                .targetName("EXTERNAL GROUP")
                .audit();
            throw e;
        }
    }

    /**
     * Supposed to change the Active Directory group.
     *
     * @param adGroupInputDto The Active Directory group creation request.
     * @return The {@link ActiveDirectoryGroupApiDTO} indicating success.
     */
    @Override
    public ActiveDirectoryGroupApiDTO changeActiveDirectoryGroup(
            final ActiveDirectoryGroupApiDTO adGroupInputDto) {
        validateActiveDirectoryGroupInput(adGroupInputDto);
        try {
            HttpEntity<SecurityGroupDTO> entity = new HttpEntity<>(
                convertGroupInfoToAuth(adGroupInputDto), composeHttpHeaders());
            final Map<Long, GroupApiDTO> groupApiDTOMap = buildGroupApiDtoMap(adGroupInputDto);
            String request = baseRequest().path("/users/ad/groups").build().toUriString();
            final String details = String.format("Changed external group: %s with role: %s",
                    adGroupInputDto.getDisplayName(), adGroupInputDto.getRoleName());
            AuditLog.newEntry(AuditAction.CHANGE_GROUP,
                    details, true)
                    .targetName("EXTERNAL GROUP")
                    .audit();
            return convertGroupInfoFromAuth(
                restTemplate_.exchange(request, HttpMethod.PUT, entity, SecurityGroupDTO.class).getBody(),
                groupApiDTOMap);
        } catch (RuntimeException e) {
            final String details = String.format("Failed to create external group: %s with role: %s",
                adGroupInputDto.getDisplayName(), adGroupInputDto.getRoleName());
            AuditLog.newEntry(AuditAction.CHANGE_GROUP,
                details, false)
                .targetName("EXTERNAL GROUP")
                .audit();
            throw e;
        }
    }

    /**
     * Create a group oid -> object map for the conversion on the way back.
     *
     * @param adGroupInputDto set of AD groups to look up
     * @return a map from group OID -> GroupApiDTO
     */
    private Map<Long, GroupApiDTO> buildGroupApiDtoMap(final ActiveDirectoryGroupApiDTO adGroupInputDto) {
        Map<Long, GroupApiDTO> groupApiDTOMap = new HashMap<>();
        if (adGroupInputDto.getScope() != null) {
            adGroupInputDto.getScope().forEach(groupApiDTO ->
                groupApiDTOMap.put(Long.valueOf(groupApiDTO.getUuid()), groupApiDTO));
        }
        return groupApiDTOMap;
    }

    /**
     * Deletes the group.
     *
     * @param groupName The group name.
     * @return {@code true} iff the group existed before this call.
     */
    @Override
    public Boolean deleteActiveDirectoryGroup(final String groupName) {
        Preconditions.checkArgument(!StringUtils.isBlank(groupName), "Group name cannot be empty");
        try {
            UriComponentsBuilder builder = baseRequest().path("/users/ad/groups/" + groupName);
            final String request = builder.build().toUriString();
            HttpEntity<Boolean> entity = new HttpEntity<>(composeHttpHeaders());
            final String details = String.format("Delete external group: %s", groupName);
            AuditLog.newEntry(AuditAction.DELETE_GROUP,
                details, true)
                .targetName("EXTERNAL GROUP")
                .audit();
            return restTemplate_.exchange(request, HttpMethod.DELETE, entity, Boolean.class).getBody();
        } catch (RuntimeException e) {
            final String details = String.format("Failed to delete external group: %s", groupName);
            AuditLog.newEntry(AuditAction.DELETE_GROUP,
                details, false)
                .targetName("EXTERNAL GROUP")
                .audit();
            throw e;
        }
    }

    @Override
    public void validateInput(final Object o, final Errors errors) {
    }

    /**
     * Builds base AUTH REST request.
     *
     * @return The base AUTH REST request.
     */
    private @Nonnull UriComponentsBuilder baseRequest() {
        return UriComponentsBuilder.newInstance().scheme("http").host(authHost_).port(authPort_);
    }

    /**
     * Unsupported for XL.
     *
     * @param uuid The UUID.
     * @return Nothing.
     * @throws Exception - UnsupportedOperationException always.
     */
    @Override
    public List<BaseApiDTO> getFavoriteScopesByUser(String uuid) throws Exception {
        return Collections.emptyList();
    }

    /**
     * Unsupported for XL.
     *
     * @param uuid      The UUID.
     * @param scopeUuid The Scope UUID.
     * @return Nothing.
     * @throws Exception - UnsupportedOperationException always.
     */
    @Override
    public List<BaseApiDTO> addFavoriteScopes(String uuid, String scopeUuid) throws Exception {
        throw new UnsupportedOperationException();
    }

    /**
     * Unsupported for XL.
     *
     * @param uuid      The UUID.
     * @param scopeUuid The Scope UUID.
     * @return Nothing.
     * @throws Exception - UnsupportedOperationException always.
     */
    @Override
    public List<BaseApiDTO> deleteFavoriteScopesByUser(String uuid, String scopeUuid)
            throws Exception {
        throw new UnsupportedOperationException();
    }

    /**
     * Get user preference properties by userUuid.
     * Unsupported for XL.
     *
     * @param userUuid The UUID.
     * @return List of PropertyValueApiDTO.
     * @throws Exception - UnsupportedOperationException always.
     */
    @Override
    public List<PropertyValueApiDTO> getPreferencePropertiesByUser(String userUuid) throws Exception {
        throw new UnsupportedOperationException();
    }

    /**
     * Set a user preference property by userUuid.
     * Unsupported for XL.
     *
     * @param userUuid      The UUID.
     * @param property property key.
     * @param value property value
     * @return PropertyValueApiDTO Containing the property that was updated
     * @throws Exception - UnsupportedOperationException always.
     */
    @Override
    public PropertyValueApiDTO setPreferencePropertyByUser(String userUuid, String property, String value) throws Exception {
        throw new UnsupportedOperationException();
    }

    /**
     * Delete a user preference property by userUuid.
     * Unsupported for XL.
     *
     * @param userUuid      The UUID.
     * @param property property key.
     * @throws Exception - UnsupportedOperationException always.
     */
    @Override
    public void deletePreferencePropertyByUser(String userUuid, String property) throws Exception {
        throw new UnsupportedOperationException();
    }

    /**
     * Supposed to change the password.
     * It doesn't get called.
     *
     * @param uuid                 The user UUID.
     * @param changePasswordApiDTO The change password request object.
     * @return The response indicating success {@link UserApiDTO}.
     * @throws Exception The UnsupportedOperationException gets thrown always, as this operation
     *                   is a no-op.
     */
    @Override
    public UserApiDTO changeUserPassword(String uuid, ChangePasswordApiDTO changePasswordApiDTO)
            throws Exception {
        throw new UnsupportedOperationException();
    }

    /**
     * Get a User one-time password. This method is a fake implementation right now.
     * TODO: should be implemented in OM-29255
     *
     * @return user includes one-time password
     * @throws UnauthorizedObjectException when user is not logged in
     */
    @Override
    public UserApiDTO getUserOneTimePassword() throws UnauthorizedObjectException {
        final UserApiDTO userDto = new UserApiDTO();
        userDto.setUsername("fake user");
        userDto.setAuthToken("fake token");
        userDto.setPassword("fake password");
        userDto.setUuid(UUID.randomUUID().toString());
        return userDto;
    }

    @Override
    public UserApiDTO resetAdministratorPassword(final ChangePasswordApiDTO changePasswordApiDTO) throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<SAMLIdpApiDTO> getSAMLIdp() {
        if (samlEnabled) {
            logger_.info(SAML_IDP_ENTITY_NAME + registrationId);
            SAMLIdpApiDTO samlIdpApiDTO = new SAMLIdpApiDTO();
            samlIdpApiDTO.setIdpURL(registrationId);
            samlIdpApiDTO.setSAMLOnly(true);
            samlIdpApiDTO.setSingleLogoutEnabled(false);
            return Optional.of(samlIdpApiDTO);
        }
        return Optional.empty();
    }

    /**
     * Validate that the active directory group api dto specified has a valid name, type, and roleName.
     *
     * @param apiDTO dto representing the active directory group.
     */
    private void validateActiveDirectoryGroupInput(@Nonnull final ActiveDirectoryGroupApiDTO apiDTO) {
        if (StringUtils.isBlank(apiDTO.getDisplayName())) {
            throw new IllegalArgumentException("No name specified for active directory group.");
        }
        if (StringUtils.isBlank(apiDTO.getRoleName())) {
            throw new IllegalArgumentException("No role specified for active directory group.");
        }
        if (StringUtils.isBlank(apiDTO.getType())) {
            throw new IllegalArgumentException("No type specified for active directory group.");
        }
    }

    /**
     * Validate that the active directory api dto specified has a valid domain name or login provider url.
     *
     * @param apiDTO dto representing the active directory,
     */
    private void validateActiveDirectoryInput(@Nonnull final ActiveDirectoryApiDTO apiDTO) {
        if (StringUtils.isBlank(apiDTO.getDomainName()) &&
                StringUtils.isBlank(apiDTO.getLoginProviderURI())) {
            throw new IllegalArgumentException("Both domain name and login provider URL are" +
                    " not specified for active directory.");
        }
    }
}
