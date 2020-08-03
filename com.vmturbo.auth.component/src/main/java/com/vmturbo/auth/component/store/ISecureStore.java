package com.vmturbo.auth.component.store;

import java.util.Optional;

import javax.annotation.Nonnull;

import com.vmturbo.auth.api.authorization.AuthorizationException;

/**
 * The ISecureStore implements RDBMS-backed secure storage.
 */
public interface ISecureStore {
    /**
     * Retrieves the secure data associated with the key.
     * The information may be retrieved only by the owner.
     *
     * @param subject The auth token.
     * @param key     The secure storage key.
     * @return The secure data.
     * @throws AuthorizationException In case of an error verifying the JWT token.
     */
    @Nonnull Optional<String> get(final @Nonnull String subject,
                                  final @Nonnull String key) throws AuthorizationException;

    /**
     * Replaces the secure data associated with the key with the data supplied in the dto.
     * In case key/data association does not yet exist, create such an association.
     * Only the owner may modify it.
     *
     * @param subject The subject.
     * @param key     The secure storage key.
     * @param data    The data.
     * @return The data URI.
     * @throws AuthorizationException In case of an error verifying the JWT token.
     */
    String modify(final @Nonnull String subject, final @Nonnull String key,
                  final @Nonnull String data) throws AuthorizationException;

    /**
     * Deletes the key and the secure data associated with the key.
     * Either the owner or administrator may perform this action.
     *
     * @param subject The subject.
     * @param key     The secure storage key.
     * @throws AuthorizationException In case of an error verifying the JWT token.
     */
    void delete(final @Nonnull String subject, final @Nonnull String key)
            throws AuthorizationException;

    /**
     * Retrieves the SQL database root password.
     *
     * @return The database root password.
     */
    @Nonnull String getRootSqlDBPassword();

    /**
     * Retrieves the SQL database root username.
     *
     * @return The database root username.
     */
    @Nonnull String getRootSqlDBUsername();

    /**
     * Retrieves the Postgres database root username.
     *
     * @return The Postgres database root username.
     */
    @Nonnull String getPostgresRootUsername();

    /**
     * Sets the SQL database root password.
     *
     * @param existingPassword The existing root database password.
     * @param newPassword      The new root database password.
     * @return {@code true} iff the password change was successful.
     */
    boolean setRootSqlDBPassword(final @Nonnull String existingPassword,
                                 final @Nonnull String newPassword);

    /**
     * Retrieves the database root password.
     *
     * @return The database root password.
     */
    @Nonnull String getRootArangoDBPassword();

    /**
     * Retrieves the influx database root password.
     *
     * @return The influx database root password.
     */
    @Nonnull String getRootInfluxPassword();
}
