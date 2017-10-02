package com.vmturbo.auth.component.services;

/**
 * The SecureStorageController implements the AUTH component's Secure Storage REST controller.
 */

import java.net.URLDecoder;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nonnull;

import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;

import com.vmturbo.auth.api.authentication.AuthenticationException;
import com.vmturbo.auth.api.db.DBPasswordDTO;
import com.vmturbo.auth.component.store.ISecureStore;

/**
 * Every annotation that start with Api... is about Documentation
 * <p>
 * This setting controller class can handle:
 * <ul>
 * <li>GET /securestorage</li>
 * <li>PUT /securestorage</li>
 * <li>DELETE /securestorage</li>
 * </ul>
 * Please note that the key may be maximum 255 characters long.
 */
@RestController
@RequestMapping("/securestorage")
@Api(value = "/securestorage", description = "Methods for managing Secure Storage")
public class SecureStorageController {
    /**
     * The store.
     */
    private final ISecureStore store_;

    /**
     * Constructs the SecureStorageController
     *
     * @param store The underlying persistent store.
     */
    public SecureStorageController(final @Nonnull ISecureStore store) {
        store_ = Objects.requireNonNull(store);
    }

    /**
     * Checks whether the subject is in fact the authenticated user.
     *
     * @param subject The subject.
     * @return {@code true} if subject is a logged on user.
     */
    private boolean isCurrentlyAuthenticatedUser(final @Nonnull String subject) {
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        if (auth == null) {
            return false;
        }
        return subject.equals(auth.getPrincipal());
    }

    /**
     * Returns the logged on user.
     *
     * @param subject The subject.
     * @return {@code true} if subject is a logged on user.
     */
    private boolean hasRole(final @Nonnull String subject, final @Nonnull String role) {
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        if (auth == null) {
            return false;
        }
        for (GrantedAuthority ga : auth.getAuthorities()) {
            if (role.equals(ga.getAuthority())) {
                return true;
            }
        }

        return false;
    }

    /**
     * Retrieve secure data associated with a key.
     * The information may be retrieved only by the owner.
     *
     * @param subject The subject.
     * @param key     The secure storage key.
     * @return The secure data.
     * @throws Exception In case of an error obtaining the data associated with the key.
     */
    @ApiOperation(value = "Retrieve secure data associated with a key")
    @RequestMapping(path = "get/{subject}/{key}",
                    method = RequestMethod.GET,
                    produces = {MediaType.APPLICATION_JSON_VALUE})
    @ResponseBody
    public @Nonnull String get(@ApiParam(value = "The subject name", required = true)
                               @PathVariable("subject") @Nonnull String subject,
                               @ApiParam(value = "The key", required = true)
                               @PathVariable("key") @Nonnull String key) throws Exception {
        if (!isCurrentlyAuthenticatedUser(subject)) {
            throw new AuthenticationException("Wrong principal.");
        }
        Optional<String> result = store_.get(subject, URLDecoder.decode(key, "UTF-8"));
        if (result.isPresent()) {
            return result.get();
        }
        throw new IllegalStateException("No data for the key and owner");
    }

    /**
     * Associates secure data with a key.
     * The information is associated with the owner (subject in the token).
     * So, only the owner or administrator may delete it, and only owner
     * may retrieve and modify it.
     *
     * @param subject The subject.
     * @param key     The secure storage key.
     * @param data    The data.
     * @return The secure data URL.
     * @throws Exception In case of an error adding the data and/or the key.
     */
    @ApiOperation(value = "Associate secure data with a key")
    @RequestMapping(path = "modify/{subject}/{key}",
                    method = RequestMethod.PUT,
                    consumes = {MediaType.APPLICATION_JSON_VALUE},
                    produces = {MediaType.APPLICATION_JSON_VALUE})
    @ResponseBody
    public String modify(@ApiParam(value = "The subject name", required = true)
                         @PathVariable("subject") @Nonnull String subject,
                         @ApiParam(value = "The key", required = true)
                         @PathVariable("key") @Nonnull String key,
                         @RequestBody @Nonnull String data) throws Exception {
        if (!isCurrentlyAuthenticatedUser(subject)) {
            throw new AuthenticationException("Wrong principal.");
        }
        return store_.modify(subject, URLDecoder.decode(key, "UTF-8"), data);
    }

    /**
     * Deletes the key and the secure data associated with the key.
     * Either the owner or administrator may perform this action.
     *
     * @param subject The subject.
     * @param key     The secure storage key.
     * @throws Exception In case of an error deleting data and/or the key.
     */
    @ApiOperation(value = "Deletes the secure data associated with the key")
    @RequestMapping(path = "delete/{subject}/{key}",
                    method = RequestMethod.DELETE,
                    consumes = {MediaType.APPLICATION_JSON_VALUE})
    @ResponseBody
    public void delete(@ApiParam(value = "The subject name", required = true)
                       @PathVariable("subject") @Nonnull String subject,
                       @ApiParam(value = "The key", required = true)
                       @PathVariable("key") @Nonnull String key) throws Exception {
        if (!isCurrentlyAuthenticatedUser(subject) && !hasRole(subject, "ROLE_ADMINISTRATOR")) {
            throw new AuthenticationException("Wrong principal.");
        }
        store_.delete(subject, URLDecoder.decode(key, "UTF-8"));
    }

    /**
     * Returns DB root password.
     *
     * @return The user resource URL if successful.
     * @throws Exception In case of an error adding user.
     */
    @ApiOperation(value = "Returns DB root password")
    @RequestMapping(path = "getDBRootPassword",
                    method = RequestMethod.GET,
                    produces = {MediaType.APPLICATION_JSON_VALUE})
    @ResponseBody
    public @Nonnull String getDBRootPassword() throws Exception {
        String password = store_.getRootDBPassword();
        return password;
    }

    /**
     * Returns DB root password.
     *
     * @return The user resource URL if successful.
     * @throws Exception In case of an error adding user.
     */
    @ApiOperation(value = "Sets DB root password")
    @RequestMapping(path = "setDBRootPassword",
                    method = RequestMethod.PUT,
                    consumes = {MediaType.APPLICATION_JSON_VALUE},
                    produces = {MediaType.APPLICATION_JSON_VALUE})
    @ResponseBody
    public ResponseEntity<String> setDBRootPassword(@RequestBody final @Nonnull DBPasswordDTO dto)
            throws Exception {
        if (store_.setRootDBPassword(dto.getExistingPassword(), dto.getNewPassword())) {
            return new ResponseEntity<>("Changing DB root password succeeded.", HttpStatus.OK);
        }
        return new ResponseEntity<>("Unable to change the DB root password.",
                                    HttpStatus.UNAUTHORIZED);
    }

}
