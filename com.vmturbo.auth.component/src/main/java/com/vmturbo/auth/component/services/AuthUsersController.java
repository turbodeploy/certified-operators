package com.vmturbo.auth.component.services;

import java.net.URLDecoder;
import java.util.List;

import javax.annotation.Nonnull;

import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;

import com.vmturbo.auth.api.usermgmt.ActiveDirectoryDTO;
import com.vmturbo.auth.api.usermgmt.AuthUserDTO;
import com.vmturbo.auth.api.usermgmt.AuthUserModifyDTO;
import com.vmturbo.auth.api.usermgmt.SecurityGroupDTO;
import com.vmturbo.auth.component.store.AuthProvider;

/**
 * The AuthUsersController implements the AUTH component Users REST controller.
 */

/**
 * Every annotation that start with Api... is about Documentation
 * <p>
 * This setting controller class can handle:
 * GET /users
 * GET /users/{uuid}
 */
@RestController
@RequestMapping("/users")
@Api(value = "/users", description = "Methods for managing Users")
public class AuthUsersController {
    /**
     * The underlying store.
     */
    private final AuthProvider targetStore_;

    /**
     * Constructs AuthUsersController
     *
     * @param targetStore The implementation.
     */
    public AuthUsersController(final @Nonnull AuthProvider targetStore) {
        targetStore_ = targetStore;
    }

    /**
     * Initializes the admin the user.
     * This will only be called once.
     *
     * @param dto The request DTO.
     * @return The user resource URL if successful.
     * @throws Exception In case of an error adding user.
     */
    @ApiOperation(value = "Initialize admin user")
    @RequestMapping(path = "initAdmin",
                    method = RequestMethod.POST,
                    consumes = {MediaType.APPLICATION_JSON_VALUE},
                    produces = {MediaType.APPLICATION_JSON_VALUE})
    @ResponseBody
    public @Nonnull String initAdmin(@RequestBody AuthUserDTO dto) throws Exception {
        boolean success = targetStore_.initAdmin(dto.getUser(), dto.getPassword());
        if (success) {
            return "users://" + dto.getUser();
        }
        throw new SecurityException("Unable to initialize admin user: " + dto.getUser());
    }

    /**
     * Checks whether the admin user has been instantiated.
     *
     * @return {@code true} iff the admin user has been instantiated.
     * @throws Exception In case of an error adding user.
     */
    @ApiOperation(value = "Check admin user instantiated")
    @RequestMapping(path = "checkAdminInit",
                    method = RequestMethod.GET,
                    produces = {MediaType.APPLICATION_JSON_VALUE})
    @ResponseBody
    public @Nonnull Boolean checkAdminInit()
            throws Exception {
        return targetStore_.checkAdminInit();
    }

    /**
     * Authenticates the user.
     *
     * @param userName The user name.
     * @param password The password.
     * @return The compact representation of the Authorization Token if successful.
     * @throws Exception In case of an error adding user.
     */
    @ApiOperation(value = "Authenticate user")
    @RequestMapping(value = "/authenticate/{userName}/{password}",
                    method = RequestMethod.GET,
                    produces = {MediaType.APPLICATION_JSON_VALUE})
    @ResponseBody
    public @Nonnull String authenticate(
            @ApiParam(value = "The user name", required = true)
            @PathVariable("userName") String userName,
            @ApiParam(value = "The user password",
                      required = true)
            @PathVariable("password") String password)
            throws Exception {
        return targetStore_.authenticate(userName, URLDecoder.decode(password, "UTF-8"))
                           .getCompactRepresentation();
    }

    /**
     * Authenticates the user with IP address.
     * Nov 2018: changed this enpoint from HTTP GET to POST to avoid clear password in URL,
     * also avoid logging the password along with URL.
     *
     * @param authUserDTO The auth user DTO.
     * @return The compact representation of the Authorization Token if successful.
     * @throws Exception In case of an error authenticating user.
     */
    @ApiOperation(value = "Authenticate user")
    @RequestMapping(path = "authenticate",
                    method = RequestMethod.POST,
                    consumes = {MediaType.APPLICATION_JSON_VALUE},
                    produces = {MediaType.APPLICATION_JSON_VALUE})
    @ResponseBody
    public @Nonnull String authenticate(@RequestBody AuthUserDTO authUserDTO)
            throws Exception {
        return targetStore_.authenticate(authUserDTO.getUser(), authUserDTO.getPassword(), authUserDTO.getIpAddress())
                .getCompactRepresentation();
    }


    /**
     * Authorize the SAML user with IP address.
     * Due to bug in Spring boot, we have to use "{ipaddress:.+}", instead of "{ipaddress}"
     * {@see <a href="https://jira.springsource.org/browse/SPR-6164"/>}
     *
     * @param userName The user name.
     * @param groupName The user group.
     * @param ipAddress The user IP address.
     * @return The compact representation of the Authorization Token if successful.
     * @throws Exception In case of an error adding user.
     */
    @ApiOperation(value = "Authorize user")
    @RequestMapping(value = "/authorize/{userName}/{groupName}/{ipaddress:.+}",
            method = RequestMethod.GET,
            produces = {MediaType.APPLICATION_JSON_VALUE})
    @ResponseBody
    public @Nonnull String authorize(
            @ApiParam(value = "The user name", required = true)
            @PathVariable("userName") String userName,
            @ApiParam(value = "The user group",
                    required = true)
            @PathVariable("groupName") String groupName,
            @ApiParam(value = "The user ip address",
                    required = true)
            @PathVariable("ipaddress") String ipAddress)
            throws Exception {
        return targetStore_.authorize(URLDecoder.decode(userName, "UTF-8"),
                URLDecoder.decode(groupName, "UTF-8"), ipAddress).getCompactRepresentation();
    }

    /**
     * Authorize the SAML the user with IP address.
     * Due to bug in Spring boot, we have to use "{ipaddress:.+}", instead of "{ipaddress}"
     * {@see <a href="https://jira.springsource.org/browse/SPR-6164"/>}
     *
     * @param userName The user name.
     * @param ipAddress The user IP address.
     * @return The compact representation of the Authorization Token if successful.
     * @throws Exception In case of an error adding user.
     */
    @ApiOperation(value = "Authorize user")
    @RequestMapping(value = "/authorize/{userName}/{ipaddress:.+}",
            method = RequestMethod.GET,
            produces = {MediaType.APPLICATION_JSON_VALUE})
    @ResponseBody
    public @Nonnull String authorize(
            @ApiParam(value = "The user name", required = true)
            @PathVariable("userName") String userName,
            @PathVariable("ipaddress") String ipAddress)
            throws Exception {
        return targetStore_.authorize(URLDecoder.decode(userName, "UTF-8"),
                 ipAddress).getCompactRepresentation();
    }

    /**
     * Lists all defined users.
     *
     * @return The list of all users.
     * @throws Exception In case of an error listing users.
     */
    @ApiOperation(value = "Lists all known users")
    @RequestMapping(path = "",
                    method = RequestMethod.GET,
                    produces = {MediaType.APPLICATION_JSON_VALUE})
    @ResponseBody
    public @Nonnull List<AuthUserDTO> list() throws Exception {
        return targetStore_.list();
    }

    /**
     * Adds the user.
     *
     * @param dto The request DTO.
     * @return The user resource URL if successful.
     * @throws Exception In case of an error adding user.
     */
    @ApiOperation(value = "Add user")
    @RequestMapping(path = "add",
                    method = RequestMethod.POST,
                    consumes = {MediaType.APPLICATION_JSON_VALUE},
                    produces = {MediaType.APPLICATION_JSON_VALUE})
    @ResponseBody
    public @Nonnull ResponseEntity<String> addUser(@RequestBody AuthUserDTO dto) throws Exception {
        try {
            boolean success = targetStore_.add(dto.getProvider(), dto.getUser(),
                dto.getPassword(), dto.getRoles(), dto.getScopeGroups());
            if (success) {
                return new ResponseEntity<>("users://" + dto.getUser(), HttpStatus.OK);
            }
        } catch (Exception e) {
            return new ResponseEntity<>(e.getMessage(),
                HttpStatus.FORBIDDEN);
        }
        throw new SecurityException("Unable to add user: " + dto.getUser());
    }

    /**
     * Sets the password for a user defined in the LOCAL provider.
     *
     * @param dto The request DTO.
     * @return The user resource URL if successful.
     * @throws Exception In case of an error adding user.
     */
    @ApiOperation(value = "Sets user password")
    @RequestMapping(path = "setpassword",
                    method = RequestMethod.PUT,
                    consumes = {MediaType.APPLICATION_JSON_VALUE},
                    produces = {MediaType.APPLICATION_JSON_VALUE})
    @ResponseBody
    public @Nonnull void setPassword(@RequestBody AuthUserModifyDTO dto)
            throws Exception {
        boolean success = targetStore_.setPassword(dto.getUserToModify().getUser(),
                                                   dto.getUserToModify().getPassword(),
                                                   dto.getNewPassword());
        if (!success) {
            throw new SecurityException("Unable to set password for user: " + dto.getUserToModify().getUser());
        }
    }

    /**
     * Replaces user roles.
     *
     * @param dto The request DTO.
     * @return The user resource URL if successful.
     * @throws Exception In case of an error adding user.
     */
    @ApiOperation(value = "Sets user roles")
    @RequestMapping(path = "setroles",
                    method = RequestMethod.PUT,
                    consumes = {MediaType.APPLICATION_JSON_VALUE},
                    produces = {MediaType.APPLICATION_JSON_VALUE})
    @ResponseBody
    public @Nonnull ResponseEntity<String> setRoles(@RequestBody AuthUserDTO dto) throws Exception {
        // TODO: we will move scope into the roles into the future. But for now, we are setting
        // scope on the user level.
        return targetStore_.setRoles(dto.getProvider(), dto.getUser(), dto.getRoles(), dto.getScopeGroups());
    }

    /**
     * Removes the user.
     *
     * @param uuid The user's UUID or name.
     * @throws Exception In case of an error adding user.
     */
    @ApiOperation(value = "Remove user")
    @RequestMapping(value = "/remove/{uuid}",
                    method = RequestMethod.DELETE,
                    produces = {MediaType.APPLICATION_JSON_VALUE})
    @ResponseBody
    public void removeUser(
            @ApiParam(value = "The user UUID", required = true)
            @PathVariable("uuid") String uuid)
            throws Exception {
        boolean success = targetStore_.remove(uuid);
        if (!success) {
            throw new SecurityException("Unable to remove user: " + uuid);
        }
    }

    /**
     * Locks the user.
     *
     * @param dto The user DTO.
     * @throws Exception In case of an error adding user.
     */
    @ApiOperation(value = "Lock user")
    @RequestMapping(path = "lock",
                    method = RequestMethod.PUT,
                    consumes = {MediaType.APPLICATION_JSON_VALUE},
                    produces = {MediaType.APPLICATION_JSON_VALUE})
    @ResponseBody
    public void lock(@RequestBody AuthUserDTO dto)
            throws Exception {
        boolean success = targetStore_.lock(dto);
        if (!success) {
            throw new SecurityException("Unable to lock user: " + dto.getUser());
        }
    }

    /**
     * Unlocks the user.
     *
     * @param dto The user DTO.
     * @throws Exception In case of an error adding user.
     */
    @ApiOperation(value = "Unlock user")
    @RequestMapping(path = "unlock",
                    method = RequestMethod.PUT,
                    consumes = {MediaType.APPLICATION_JSON_VALUE},
                    produces = {MediaType.APPLICATION_JSON_VALUE})
    @ResponseBody
    public void unlock(@RequestBody AuthUserDTO dto)
            throws Exception {
        boolean success = targetStore_.unlock(dto);
        if (!success) {
            throw new SecurityException("Unable to unlock user: " + dto.getUser());
        }
    }

    /**
     * get a list of Active Directory representation objects (domain, secure flag, etc.)
     *
     * @return list of ActiveDirectoryApiDTO
     */
    @ApiOperation(value = "Get a list of Active Directories")
    @RequestMapping(value = "ad", method = RequestMethod.GET,
                    produces = {MediaType.APPLICATION_JSON_VALUE})
    @ResponseBody
    public List<ActiveDirectoryDTO> getActiveDirectories() throws Exception {
        return targetStore_.getActiveDirectories();
    }

    /**
     * Create a new Active Directory
     *
     * @param inputDto
     * @return new created ActiveDirectoryApiDTO
     * @throws Exception
     */
    @ApiOperation(value = "Create a new Active Directory")
    @RequestMapping(value = "ad",
                    method = RequestMethod.POST,
                    produces = {MediaType.APPLICATION_JSON_VALUE})
    @ResponseBody
    public ActiveDirectoryDTO createActiveDirectory(
            @ApiParam(value = "Properties to create a Active Directory", required = true)
            @RequestBody ActiveDirectoryDTO inputDto) throws Exception {
        return targetStore_.createActiveDirectory(inputDto);
    }

    /**
     * Get a list of Active Directory groups
     *
     * @return list of ActiveDirectoryGroupApiDTO
     */
    @ApiOperation(value = "Get a list of Active Directory groups")
    @RequestMapping(value = "ad/groups", method = RequestMethod.GET,
                    produces = {MediaType.APPLICATION_JSON_VALUE})
    @ResponseBody
    public List<SecurityGroupDTO> getActiveDirectoryGroups() throws Exception {
        return targetStore_.getSecurityGroups();
    }

    /**
     * Create a new Active Directory group
     *
     * @return new ActiveDirectoryGroupApiDTO
     */
    @ApiOperation(value = "Create a new Active Directory group")
    @RequestMapping(value = "ad/groups", method = RequestMethod.POST,
                    produces = {MediaType.APPLICATION_JSON_VALUE})
    @ResponseBody
    public SecurityGroupDTO createSSOGroup(
            @ApiParam(value = "Properties to create an Active Directory group", required = true)
            @RequestBody SecurityGroupDTO adGroupInputDto) throws Exception {
        return targetStore_.createSecurityGroup(adGroupInputDto);
    }

    /**
     * Replaces values in an existing Active Directory group.
     *
     * @return new ActiveDirectoryGroupApiDTO
     */
    @ApiOperation(value = "Change an existing Active Directory group")
    @RequestMapping(value = "ad/groups", method = RequestMethod.PUT,
                    produces = {MediaType.APPLICATION_JSON_VALUE})
    @ResponseBody
    public SecurityGroupDTO changeActiveDirectoryGroup(
            @ApiParam(value = "New properties for an existing Active Directory group",
                      required = false)
            @RequestBody SecurityGroupDTO adGroupInputDto) throws Exception {
        return targetStore_.updateSecurityGroup(adGroupInputDto);
    }

    /**
     * delete an existing Active Directory group
     * DELETE /users/ad/groups/{groupName}
     *
     * @return true if succeeded, false if failure
     */
    @ApiOperation(value = "Delete an existing Active Directory group")
    @RequestMapping(value = "ad/groups/{groupName}", method = RequestMethod.DELETE,
                    produces = {MediaType.APPLICATION_JSON_VALUE})
    @ResponseBody
    public Boolean deleteSSOGroup(
            @ApiParam(value = "The name of Active Directory group", required = true)
            @PathVariable("groupName") String groupName) throws Exception {
        return targetStore_.deleteSecurityGroup(groupName);
    }
}
