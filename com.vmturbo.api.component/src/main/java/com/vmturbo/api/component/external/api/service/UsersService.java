package com.vmturbo.api.component.external.api.service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.Errors;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import com.vmturbo.api.component.communication.RestAuthenticationProvider;
import com.vmturbo.api.component.external.api.SAML.SAMLUtils;
import com.vmturbo.api.component.external.api.mapper.LoginProviderMapper;
import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.user.ActiveDirectoryApiDTO;
import com.vmturbo.api.dto.user.ActiveDirectoryGroupApiDTO;
import com.vmturbo.api.dto.user.ChangePasswordApiDTO;
import com.vmturbo.api.dto.user.SAMLIdpApiDTO;
import com.vmturbo.api.dto.user.UserApiDTO;
import com.vmturbo.api.exceptions.UnauthorizedObjectException;
import com.vmturbo.api.serviceinterfaces.IUsersService;
import com.vmturbo.auth.api.Base64CodecUtils;
import com.vmturbo.auth.api.authentication.credentials.SAMLUserUtils;
import com.vmturbo.auth.api.usermgmt.ActiveDirectoryDTO;
import com.vmturbo.auth.api.usermgmt.ActiveDirectoryGroupDTO;
import com.vmturbo.auth.api.usermgmt.AuthUserDTO;
import com.vmturbo.auth.api.usermgmt.AuthUserDTO.PROVIDER;
import com.vmturbo.auth.api.usermgmt.AuthUserModifyDTO;

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
    public static final String ENTITY_ID = "entityID";
    public static final String SAML_IDP_ENTITY_NAME = "SAML IDP entity name: ";
    public static final String MD_SINGLE_LOGOUT_SERVICE = "md:SingleLogoutService";

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
    private final Gson GSON_ = new GsonBuilder().create();
    private final String idpURL;
    private final boolean samlEnabled, isSingleLogoutEnabled;

    /**
     * Constructs the users service.
     * @param authHost     The authentication host.
     * @param authPort     The authentication port.
     * @param restTemplate The synchronous client-side HTTP access.
     * @param samlIdpMetadata The SAML IDP metadata
     * @param samlEnabled  is SAML enabled
     */
    public UsersService(final @Nonnull String authHost,
                        final int authPort,
                        final @Nonnull RestTemplate restTemplate,
                        final @Nonnull String samlIdpMetadata,
                        final boolean samlEnabled) {
        authHost_ = Objects.requireNonNull(authHost);
        authPort_ = authPort;
        if (authPort_ < 0 || authPort_ > 65535) {
            throw new IllegalArgumentException("Invalid AUTH port.");
        }
        restTemplate_ = Objects.requireNonNull(restTemplate);
        if (samlEnabled && samlIdpMetadata != null) {
            final String idpMetadata = geIdpMetadataXML(samlIdpMetadata);
            this.idpURL = getIdpEntityName(idpMetadata).orElse("");
            this.samlEnabled = true;
            this.isSingleLogoutEnabled = isSingleLogoutEnabled(idpMetadata);
        } else {
            this.idpURL = "";
            this.samlEnabled = false;
            this.isSingleLogoutEnabled = false;
        }


    }

    /**
     * Generates the UserApiDTO.
     *
     * @param dto The internal User object.
     * @return The generated UserApiDTO.
     */
    private UserApiDTO generateUserApiDTO(final @Nonnull AuthUserDTO dto) {
        UserApiDTO user = new UserApiDTO();
        user.setLoginProvider(LoginProviderMapper.toApi(dto.getProvider()));
        user.setUsername(dto.getUser());
        user.setRoleName(dto.getRoles().get(0));
        user.setUuid(dto.getUuid());
        // Mandatory fields.
        user.setFeatures(Collections.emptyList());
        user.setType("DedicatedCustomer");
        user.setDisplayName(dto.getUser());
        return user;
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
     * @param o     The object.
     * @param clazz The class.
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
        // Assemble the results.
        List<UserApiDTO> list = new ArrayList<>();
        for (Object o : result.getBody()) {
            // We do the conversion manually here from the result, as we can't specify the
            // exact class for automatic JSON generation.
            AuthUserDTO dto = parse(o, AuthUserDTO.class);
            UserApiDTO user = generateUserApiDTO(dto);
            list.add(user);
        }
        return list;
    }

    /**
     * Get JWT token from Spring context
     *
     * @return JWT token
     */
    private static Optional<String> geJwtTokenFromSpringSecurityContext() {
        return SAMLUserUtils
                .getAuthUserDTO()
                .map(AuthUserDTO::getToken);
    }

    /**
     * Returns the logged in user.
     *
     * @return The logged in user.
     * @throws Exception In case of any error.
     */
    @Override
    public @Nonnull UserApiDTO getLoggedInUser() throws Exception {
        return SAMLUserUtils.getAuthUserDTO()
                .map(authUserDTO -> generateUserApiDTO(authUserDTO))
                .orElseThrow(() -> new UnauthorizedObjectException("No user logged in!"));
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
     * Converts the role in the USER API DTO to the list used by the AUTH component.
     *
     * @param userApiDTO The User API DTO.
     * @return The list of roles.
     */
    private List<String> convertRolesToList(final @Nonnull UserApiDTO userApiDTO) {
        return ImmutableList.of(userApiDTO.getRoleName().toUpperCase());
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

        // According to UserApiDTO, the login provider values may be one of: "LOCAL", "LDAP".
        final AuthUserDTO.PROVIDER provider =
                LoginProviderMapper.fromApi(userApiDTO.getLoginProvider());
        final String password = provider.equals(PROVIDER.LOCAL) ? userApiDTO.getPassword() : null;

        // The explicitly added LDAP users will be kept in the same local database.
        AuthUserDTO dto = new AuthUserDTO(provider, userApiDTO.getUsername(), password, null,
                                          null, convertRolesToList(userApiDTO));
        // Perform the call.
        // Make sure that the currently authenticated user's token is present.
        HttpHeaders headers = composeHttpHeaders();
        HttpEntity<AuthUserDTO> entity = new HttpEntity<>(dto, headers);
        restTemplate_.exchange(baseRequest().path("/users/add").build().toUriString(),
                               HttpMethod.POST, entity, String.class);
        // Return data.
        UserApiDTO user = new UserApiDTO();
        user.setLoginProvider(userApiDTO.getLoginProvider());
        user.setUsername(userApiDTO.getUsername());
        user.setRoleName(userApiDTO.getRoleName());
        user.setUuid(userApiDTO.getUuid());
        return user;
    }

    /**
     * Replaces user's roles.
     *
     * @param userApiDTO The User's data object.
     * @return The user's object with bare minimum of information.
     * @throws Exception In the case of any error performing the user's data modification.
     */
    private UserApiDTO setUserRoles(final @Nonnull UserApiDTO userApiDTO) throws Exception {
        UriComponentsBuilder builder = baseRequest().path("/users/setroles");
        // Call AUTH component to perform the action.
        AuthUserDTO.PROVIDER provider = AuthUserDTO.PROVIDER.valueOf(userApiDTO.getLoginProvider());
        AuthUserDTO dto = new AuthUserDTO(provider,
                                          userApiDTO.getUsername(),
                                          userApiDTO.getUuid(),
                                          convertRolesToList(userApiDTO));
        // Perform the call.
        // Make sure that the currently authenticated user's token is present.
        HttpHeaders headers = composeHttpHeaders();
        HttpEntity<AuthUserDTO> entity = new HttpEntity<>(dto, headers);
        restTemplate_.exchange(builder.build().toUriString(), HttpMethod.PUT, entity,
                               String.class);
        // Return data.
        UserApiDTO user = new UserApiDTO();
        user.setUsername(userApiDTO.getUsername());
        user.setRoleName(userApiDTO.getRoleName());
        user.setUuid(userApiDTO.getUuid());
        return user;
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
        UriComponentsBuilder builder = baseRequest().path("/users/setpassword");
        // Call AUTH component to perform the action.
        AuthUserModifyDTO dto = new AuthUserModifyDTO(userApiDTO.getUsername(), null,
                                                      convertRolesToList(userApiDTO),
                                                      userApiDTO.getPassword());
        // Perform the call.
        // Make sure that the currently authenticated user's token is present.
        HttpHeaders headers = composeHttpHeaders();
        HttpEntity<AuthUserDTO> entity = new HttpEntity<>(dto, headers);
        restTemplate_.exchange(builder.build().toUriString(), HttpMethod.PUT, entity,
                               Void.class);
        // Return data.
        UserApiDTO user = new UserApiDTO();
        user.setUsername(userApiDTO.getUsername());
        user.setRoleName(userApiDTO.getRoleName());
        user.setUuid(userApiDTO.getUuid());
        return user;
    }

    /**
     * Edits user's roles and password.
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
     * Deletes the user.
     *
     * @param uuid The UUID.
     * @return {@code true} iff the user has been deleted successfully.
     */
    @Override
    public Boolean deleteUser(String uuid) {
        String request = baseRequest().path("/users/remove/" + uuid).build().toUriString();
        HttpEntity<AuthUserDTO> entity = new HttpEntity<>(composeHttpHeaders());
        try {
            restTemplate_.exchange(request, HttpMethod.DELETE, entity, Void.class);
        } catch (Exception e) {
            logger_.error("Unable to remove user {}", uuid, e.getCause());
            throw new IllegalArgumentException("Unable to remove user " + uuid, e.getCause());
        }
        return Boolean.TRUE;
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
    private @Nonnull ActiveDirectoryDTO convertADInfoToAuth(
            final @Nonnull ActiveDirectoryApiDTO adDTO) {
        ActiveDirectoryDTO dto =
                new ActiveDirectoryDTO(adDTO.getDomainName(), adDTO.getLoginProviderURI(),
                                       ensureNonNull(adDTO.getIsSecure()));
        if (adDTO.getGroups() != null) {
            List<ActiveDirectoryGroupDTO> groups = new ArrayList<>();
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
     */
    private @Nonnull ActiveDirectoryApiDTO convertADInfoFromAuth(
            final @Nonnull ActiveDirectoryDTO adDTO) {
        ActiveDirectoryApiDTO dto = new ActiveDirectoryApiDTO();
        dto.setDomainName(adDTO.getDomainName());
        dto.setLoginProviderURI(adDTO.getLoginProviderURI());
        dto.setIsSecure(adDTO.isSecure());

        if (adDTO.getGroups() != null) {
            List<ActiveDirectoryGroupApiDTO> groupsApi = new ArrayList<>();
            for (ActiveDirectoryGroupDTO grp : adDTO.getGroups()) {
                groupsApi.add(convertGroupInfoFromAuth(grp));
            }
            dto.setGroups(groupsApi);
        }
        return dto;
    }

    /**
     * Converts the AD Group DTO between internal and API formats.
     * We use internal in the AUTH component because we want to avoid the dependency on the API
     * layer.
     *
     * @param dto The internal format DTO.
     * @return The API format DTO.
     */
    private @Nonnull ActiveDirectoryGroupApiDTO convertGroupInfoFromAuth(
            final @Nonnull ActiveDirectoryGroupDTO dto) {
        ActiveDirectoryGroupApiDTO gad = new ActiveDirectoryGroupApiDTO();
        gad.setType(dto.getType());
        gad.setRoleName(dto.getRoleName());
        gad.setDisplayName(dto.getDisplayName());
        return gad;
    }

    /**
     * Converts the Group DTO between internal and API formats.
     * We use internal in the AUTH component because we want to avoid the dependency on the API
     * layer.
     *
     * @param dto The API format DTO.
     * @return The internal format DTO.
     */
    private @Nonnull ActiveDirectoryGroupDTO convertGroupInfoToAuth(
            final @Nonnull ActiveDirectoryGroupApiDTO dto) {
        return new ActiveDirectoryGroupDTO(dto.getDisplayName(), dto.getType(), dto.getRoleName());
    }

    /**
     * Returns the list of active directories.
     *
     * @return The list of active directories.
     */
    @Override
    public List<ActiveDirectoryApiDTO> getActiveDirectories() {
        UriComponentsBuilder builder = baseRequest().path("/users/ad");
        ResponseEntity<List> result;
        result = restTemplate_.exchange(builder.build().toUriString(), HttpMethod.GET,
                                        new HttpEntity<>(composeHttpHeaders()), List.class);
        List<ActiveDirectoryApiDTO> list = new ArrayList<>();
        for (Object o : result.getBody()) {
            list.add(convertADInfoFromAuth(parse(o, ActiveDirectoryDTO.class)));
        }
        return list;
    }

    /**
     * Create the Active Directory representation.
     *
     * @param inputDTO The Active Directory description.
     * @return The Active Directory representation object.
     */
    @Override
    public ActiveDirectoryApiDTO createActiveDirectory(
            final ActiveDirectoryApiDTO inputDTO) {
        final String request = baseRequest().path("/users/ad").build().toUriString();
        HttpEntity<ActiveDirectoryDTO> entity = new HttpEntity<>(convertADInfoToAuth(inputDTO),
                                                                 composeHttpHeaders());
        Class<ActiveDirectoryDTO> clazz = ActiveDirectoryDTO.class;
        return convertADInfoFromAuth(restTemplate_.exchange(request, HttpMethod.POST,
                                                            entity, clazz).getBody());
    }

    /**
     * Returns the list of AD group objects.
     *
     * @return The list of AD group objects.
     */
    @Override
    public List<ActiveDirectoryGroupApiDTO> getActiveDirectoryGroups() {
        String request = baseRequest().path("/users/ad/groups").build().toUriString();
        HttpHeaders headers = composeHttpHeaders();
        ResponseEntity<List> result;
        result = restTemplate_.exchange(request, HttpMethod.GET, new HttpEntity<>(headers),
                                        List.class);
        List<ActiveDirectoryGroupApiDTO> list = new ArrayList<>();
        for (Object o : result.getBody()) {
            list.add(convertGroupInfoFromAuth(parse(o, ActiveDirectoryGroupDTO.class)));
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
        UriComponentsBuilder builder = baseRequest().path("/users/ad/groups");
        String request = builder.build().toUriString();
        HttpHeaders headers = composeHttpHeaders();
        HttpEntity<ActiveDirectoryGroupDTO> entity;
        entity = new HttpEntity<>(convertGroupInfoToAuth(adGroupInputDto), headers);
        Class<ActiveDirectoryGroupDTO> clazz = ActiveDirectoryGroupDTO.class;
        return convertGroupInfoFromAuth(restTemplate_.exchange(request, HttpMethod.POST,
                                                               entity, clazz).getBody());
    }

    /**
     * Supposed to change the Active Directory group.
     * Does not get invoked by the UI.
     *
     * @param adGroupInputDto The Active Directory group creation request.
     * @return The {@link ActiveDirectoryGroupApiDTO} indicating success.
     */
    @Override
    public ActiveDirectoryGroupApiDTO changeActiveDirectoryGroup(
            final ActiveDirectoryGroupApiDTO adGroupInputDto) {
        throw new UnsupportedOperationException();
    }

    /**
     * Deletes the group.
     *
     * @param groupName The group name.
     * @return {@code true} iff the group existed before this call.
     */
    @Override
    public Boolean deleteActiveDirectoryGroup(final String groupName) {
        UriComponentsBuilder builder = baseRequest().path("/users/ad/groups/" + groupName);
        final String request = builder.build().toUriString();
        HttpEntity<Boolean> entity = new HttpEntity<>(composeHttpHeaders());
        return restTemplate_.exchange(request, HttpMethod.DELETE, entity, Boolean.class).getBody();
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

    /**
     * {@inheritDoc}
     * @return
     */
    @Override
    public Optional<SAMLIdpApiDTO> getSAMLIdp() {
        if (samlEnabled) {
                logger_.info(SAML_IDP_ENTITY_NAME + idpURL);
                SAMLIdpApiDTO samlIdpApiDTO = new SAMLIdpApiDTO();
                samlIdpApiDTO.setIdpURL(idpURL);
                samlIdpApiDTO.setSAMLOnly(samlEnabled);
                samlIdpApiDTO.setSingleLogoutEnabled(isSingleLogoutEnabled);
                return Optional.of(samlIdpApiDTO);
        }
        return Optional.empty();
    }

    private Optional<String> getIdpEntityName(@Nonnull final String idpMetadata) {
        try {
            Element element = SAMLUtils.loadXMLFromString(idpMetadata).getDocumentElement();
            return Optional.ofNullable(element.getAttribute(ENTITY_ID));
        } catch (SAXException e) {
            logger_.info(e);
        } catch (ParserConfigurationException e) {
            logger_.info(e);
        } catch (IOException e) {
            logger_.info(e);
            // it's called from constructor, and we don't want it throw any RuntimeException.
        } catch (RuntimeException e) {
            logger_.info(e);
        }
        return Optional.empty();
    }

    // decode the SAML IDP metadata
    private String geIdpMetadataXML(@Nonnull final String samlIdpMetadata) {
        byte[] byteArray = Base64CodecUtils.decode(samlIdpMetadata);
        return new String(byteArray);
    }

    // Check if the Single Logout is exist in IDP metadata.
    private boolean isSingleLogoutEnabled(@Nonnull final String idpMetadata) {
        try {
            final Document document = SAMLUtils.loadXMLFromString(idpMetadata);
            final NodeList nodeList =  document.getElementsByTagName(MD_SINGLE_LOGOUT_SERVICE);
            return nodeList != null && nodeList.getLength() >0;
        } catch (IOException e) {
            logger_.info(e);
        } catch (SAXException e) {
            logger_.info(e);
        } catch (ParserConfigurationException e) {
            logger_.info(e);
        } catch (RuntimeException e) {
            logger_.info(e);
        }
        return false;
    }
}
