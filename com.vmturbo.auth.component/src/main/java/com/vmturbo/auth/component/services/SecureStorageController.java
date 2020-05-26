package com.vmturbo.auth.component.services;

/**
 * The SecureStorageController implements the AUTH component's Secure Storage REST controller.
 */

import java.net.URLDecoder;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;

import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.vmturbo.auth.api.authentication.credentials.SAMLUserUtils;
import com.vmturbo.auth.api.authorization.AuthorizationException;
import com.vmturbo.auth.api.db.DBPasswordDTO;
import com.vmturbo.auth.api.db.DBPasswordUtil;
import com.vmturbo.auth.api.usermgmt.AuthUserDTO;
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
     * Checks the authorization status of the user.
     *
     * @param subject The username.
     * @return {@code HttpStatus.OK} if subject is a logged on user.
     *         {@code HttpStatus.UNAUTHORIZED} if there is no user.
     *         {@code HttpStatus.FORBIDDEN} if there is a user but that user does not have permission.
     */
    private HttpStatus checkAuthorizationStatus(final @Nonnull String subject) {
        Optional<AuthUserDTO> currentUser = SAMLUserUtils.getAuthUserDTO();
        if (!currentUser.isPresent()) {
            return HttpStatus.UNAUTHORIZED; // There is no user.
        }

        return subject.equals(currentUser.get().getUser()) ?
            HttpStatus.OK :         // The user is authorized
            HttpStatus.FORBIDDEN;   // The user is unauthorized
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
    public @Nonnull ResponseEntity<String> get(@ApiParam(value = "The subject name", required = true)
                               @PathVariable("subject") @Nonnull String subject,
                               @ApiParam(value = "The key", required = true)
                               @PathVariable("key") @Nonnull String key) throws Exception {
        switch (checkAuthorizationStatus(subject)) {
            case UNAUTHORIZED:
                return new ResponseEntity<>("Please log in.", HttpStatus.UNAUTHORIZED);
            case FORBIDDEN:
                return new ResponseEntity<>("You are not authorized to view this resource.", HttpStatus.FORBIDDEN);
        }

        Optional<String> result = store_.get(subject, URLDecoder.decode(key, "UTF-8"));
        if (result.isPresent()) {
            return new ResponseEntity<>(result.get(), HttpStatus.OK);
        } else {
            return new ResponseEntity<>("Resource " + key + " does not exist.", HttpStatus.BAD_REQUEST);
        }
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
    public ResponseEntity<String> modify(@ApiParam(value = "The subject name", required = true)
                         @PathVariable("subject") @Nonnull String subject,
                         @ApiParam(value = "The key", required = true)
                         @PathVariable("key") @Nonnull String key,
                         @RequestBody @Nonnull String data) throws Exception {
        switch (checkAuthorizationStatus(subject)) {
            case UNAUTHORIZED:
                return new ResponseEntity<>("Please log in.", HttpStatus.UNAUTHORIZED);
            case FORBIDDEN:
                return new ResponseEntity<>("You are not authorized to modify this resource.", HttpStatus.FORBIDDEN);
        }

        try {
            final String value = store_.modify(subject, URLDecoder.decode(key, "UTF-8"), data);
            return new ResponseEntity<>(value, HttpStatus.OK);
        } catch (AuthorizationException e) {
            return new ResponseEntity<>(e.getLocalizedMessage(), HttpStatus.FORBIDDEN);
        }
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
    public ResponseEntity<Void> delete(@ApiParam(value = "The subject name", required = true)
                       @PathVariable("subject") @Nonnull String subject,
                       @ApiParam(value = "The key", required = true)
                       @PathVariable("key") @Nonnull String key) throws Exception {
        switch (checkAuthorizationStatus(subject)) {
            case UNAUTHORIZED:
                return new ResponseEntity<>(HttpStatus.UNAUTHORIZED);
            case FORBIDDEN:
                return new ResponseEntity<>(HttpStatus.FORBIDDEN);
        }

        try {
            store_.delete(subject, URLDecoder.decode(key, "UTF-8"));
            return new ResponseEntity<>(HttpStatus.OK);
        } catch (AuthorizationException e) {
            return new ResponseEntity<>(HttpStatus.FORBIDDEN);
        }
    }

    /**
     * Returns (SQL) DB root password.
     *
     * @return The user resource URL if successful.
     * @throws Exception In case of an error adding user.
     */
    @ApiOperation(value = "Returns (SQL) DB root password")
    @RequestMapping(path = DBPasswordUtil.SQL_DB_ROOT_PASSWORD_PATH,
                    method = RequestMethod.GET,
                    produces = {MediaType.APPLICATION_JSON_VALUE})
    @ResponseBody
    public @Nonnull String getSqlDBRootPassword() throws Exception {
        return store_.getRootSqlDBPassword();
    }

    /**
     * Returns (SQL) DB root username.
     *
     * @return The user resource URL if successful.
     * @throws Exception In case of an error getting user.
     */
    @ApiOperation(value = "Returns (SQL) DB root username")
    @RequestMapping(path = DBPasswordUtil.SQL_DB_ROOT_USERNAME_PATH,
        method = RequestMethod.GET,
        produces = {MediaType.APPLICATION_JSON_VALUE})
    @ResponseBody
    public @Nonnull String getSqlDBRootUsername() throws Exception {
        return store_.getRootSqlDBUsername();
    }

    /**
     * Returns Postgres DB root username.
     *
     * @return The user resource URL if successful.
     * @throws Exception In case of an error adding user.
     */
    @ApiOperation(value = "Returns Postgres root username")
    @RequestMapping(path = DBPasswordUtil.POSTGRES_DB_ROOT_USERNAME_PATH,
            method = RequestMethod.GET,
            produces = {MediaType.APPLICATION_JSON_VALUE})
    @ResponseBody
    public @Nonnull String getPostgresDBRootUsername() throws Exception {
        return store_.getPostgresRootUsername();
    }

    /**
     * Sets the (SQL) DB root password.
     *
     * @return The user resource URL if successful.
     * @throws Exception In case of an error adding user.
     */
    @ApiOperation(value = "Sets (SQL) DB root password")
    @RequestMapping(path = "setSqlDBRootPassword",
                    method = RequestMethod.PUT,
                    consumes = {MediaType.APPLICATION_JSON_VALUE},
                    produces = {MediaType.APPLICATION_JSON_VALUE})
    @ResponseBody
    public ResponseEntity<String> setDBRootPassword(@RequestBody final @Nonnull DBPasswordDTO dto)
            throws Exception {
        if (store_.setRootSqlDBPassword(dto.getExistingPassword(), dto.getNewPassword())) {
            return new ResponseEntity<>("Changing DB root password succeeded.", HttpStatus.OK);
        }
        return new ResponseEntity<>("Unable to change the DB root password.",
                                    HttpStatus.UNAUTHORIZED);
    }

    /**
     * Returns Arango DB root password.
     *
     * @return The user resource URL if successful.
     * @throws Exception In case of an error adding user.
     */
    @ApiOperation(value = "Returns Arango DB root password")
    @RequestMapping(path = DBPasswordUtil.ARANGO_DB_ROOT_PASSWORD_PATH,
        method = RequestMethod.GET,
        produces = {MediaType.APPLICATION_JSON_VALUE})
    @ResponseBody
    public @Nonnull String getArangoDBRootPassword() throws Exception {
        return store_.getRootArangoDBPassword();
    }


    /**
     * Returns Influx DB root password.
     *
     * @return The user resource URL if successful.
     * @throws Exception In case of an error adding user.
     */
    @ApiOperation(value = "Returns Influx DB root password")
    @RequestMapping(path = DBPasswordUtil.INFLUX_DB_ROOT_PASSWORD_PATH,
        method = RequestMethod.GET,
        produces = {MediaType.APPLICATION_JSON_VALUE})
    @ResponseBody
    public @Nonnull String getInfluxDBRootPassword() throws Exception {
        return store_.getRootInfluxPassword();
    }
}
