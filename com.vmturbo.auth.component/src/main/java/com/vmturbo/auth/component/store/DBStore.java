package com.vmturbo.auth.component.store;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.util.Collection;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Condition;
import org.jooq.DSLContext;

import com.vmturbo.auth.api.authorization.AuthorizationException;
import com.vmturbo.auth.component.AuthDBConfig;
import com.vmturbo.auth.component.store.db.tables.Storage;
import com.vmturbo.auth.component.store.db.tables.records.StorageRecord;
import com.vmturbo.components.crypto.CryptoFacility;
import com.vmturbo.kvstore.KeyValueStore;

/**
 * The DBStore implements RDBMS-backed secure storage.
 */
public class DBStore implements ISecureStore {
    /**
     * The logger.
     */
    private final Logger logger = LogManager.getLogger(DBStore.class);
    /**
     * The storage record type.
     */
    private static final Storage STORAGE = new Storage();

    /**
     * The JOOQ DSL context.
     */
    private final DSLContext dslContext_;

    /**
     * The Consul key/value store.
     */
    private KeyValueStore keyValueStore_;

    /**
     * The DB URL.
     */
    private String dbUrl_;

    /**
     * The claim key.
     */
    private static final String CLAIM_KEY = "data";

    /**
     * The claims.
     */
    private static final Collection<String> CLAIMS = ImmutableList.of(CLAIM_KEY);

    /**
     * The SQL error code stating that no rows could be found in the table.
     * Please refer to:
     * https://dev.mysql.com/doc/refman/5.5/en/error-messages-server.html
     */
    private static final int ER_PASSWORD_NO_MATCH = 1133;

    /**
     * Constructs the DB-backed secure storage.
     *
     * @param dslContext    The JOOQ DSL context.
     * @param keyValueStore The K/V store.
     * @param dbUrl         The database URL.
     */
    public DBStore(final @Nonnull DSLContext dslContext,
                   final @Nonnull KeyValueStore keyValueStore,
                   final @Nonnull String dbUrl) {
        dslContext_ = dslContext;
        keyValueStore_ = keyValueStore;
        dbUrl_ = dbUrl;
    }

    /**
     * Encrypts data to be stored in the database.
     * Uses subject as part of the salt when encrypting the data.
     *
     * @param subject   The subject.
     * @param clearText The clear text.
     * @return The encrypted raw data.
     */
    private String generateStorageToken(String subject, String clearText) {
        return CryptoFacility.encrypt(subject, clearText);
    }

    /**
     * Obtains the data from the record.
     *
     * @param owner  The owner of the data.
     * @param record The record.
     * @return The decrypted data.
     * @throws AuthorizationException In case of an error verifying the token.
     */
    private String getRecordData(final @Nonnull String owner, final @Nonnull StorageRecord record)
            throws AuthorizationException {
        return CryptoFacility.decrypt(owner, record.getData());
    }

    /**
     * Retrieves the secure data associated with the key.
     * The information may be retrieved only by the owner.
     *
     * @param subject The auth token.
     * @param key     The secure storage key.
     * @return The secure data.
     * @throws AuthorizationException In case of an error verifying the JWT token.
     */
    public @Nonnull Optional<String> get(final @Nonnull String subject, final @Nonnull String key)
            throws AuthorizationException {
        Condition condition = STORAGE.PATH.equal(key).and(STORAGE.OWNER.equal(subject));
        StorageRecord record = dslContext_.fetchOne(STORAGE, condition);
        if (record == null) {
            return Optional.empty();
        }
        return Optional.of(getRecordData(subject, record));
    }

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
    public @Nonnull String modify(final @Nonnull String subject, final @Nonnull String key,
                                  final @Nonnull String data) throws AuthorizationException {
        Condition condition = STORAGE.PATH.equal(key).and(STORAGE.OWNER.equal(subject));
        StorageRecord record = dslContext_.fetchOne(STORAGE, condition);
        String storageToken = generateStorageToken(subject, data);
        // We don't have an association, create it.
        if (record == null) {
            record = new StorageRecord(key, subject, storageToken);
            dslContext_.newRecord(STORAGE, record).store();
        } else {
            // Verify the authenticity using AES/GCM encryption.
            getRecordData(subject, record);
            // Update the existing data.
            record.setData(storageToken);
            dslContext_.executeUpdate(record);
        }
        return new StringBuilder("/securestorage/").append(subject).append('/').append(key)
                                                   .toString();
    }

    /**
     * Retrieves the SQL database root password.
     *
     * @return The database root password.
     */
    public @Nonnull String getRootSqlDBPassword() {
        Optional<String> rootDbPassword = keyValueStore_.get(AuthDBConfig.CONSUL_ROOT_DB_PASS_KEY);
        return CryptoFacility.decrypt(
                rootDbPassword.orElseThrow(() -> new SecurityException("No root SQL DB password")));
    }

    /**
     * Retrieves the SQL database root username.
     *
     * @return The database root username.
     */
    @Override
    public @Nonnull String getRootSqlDBUsername() {
        Optional<String> rootDbUsername = keyValueStore_.get(AuthDBConfig.CONSUL_ROOT_DB_USER_KEY);
        return rootDbUsername.orElseThrow(() -> new SecurityException("No root SQL DB username"));
    }

    @Override
    public @Nonnull String getPostgresRootUsername() {
        Optional<String> rootDbUsername = keyValueStore_.get(AuthDBConfig.POSTGRES_ROOT_USER_KEY);
        return rootDbUsername.orElseThrow(() -> new SecurityException("No Postgres root username"));
    }

    /**
     * Sets the SQL database root password.
     *
     * @param existingPassword The existing root database password.
     * @param newPassword      The new root database password.
     * @return {@code true} iff the password change was successful.
     */
    public boolean setRootSqlDBPassword(final @Nonnull String existingPassword,
                                        final @Nonnull String newPassword) {
        Optional<String> rootDbPassword = keyValueStore_.get(AuthDBConfig.CONSUL_ROOT_DB_PASS_KEY);
        if (!rootDbPassword.isPresent() ||
            !Objects.equals(existingPassword, CryptoFacility.decrypt(rootDbPassword.get()))) {
            logger.error("Error changing SQL DB root password. The existing password doesn't match.");
            return false;
        }
        Connection connection = null;
        try {
            Class.forName("org.mariadb.jdbc.Driver").newInstance();
            connection = DriverManager.getConnection(dbUrl_, "root", existingPassword);
            boolean changed = false;
            for (String host : new String[]{"localhost", "%", "127.0.0.1", "::1"}) {
                try (PreparedStatement stmt = connection
                        .prepareStatement("SET PASSWORD FOR ?@? = PASSWORD(?)")) {
                    stmt.setString(1, "root");
                    stmt.setString(2, host);
                    stmt.setString(3, newPassword);
                    stmt.execute();
                    changed = true;
                } catch (SQLSyntaxErrorException e) {
                    if (e.getErrorCode() == ER_PASSWORD_NO_MATCH) {
                        logger.debug("No user root@" + host + " exists. Skipping...");
                    }
                }
            }
            if (changed) {
                keyValueStore_.put(AuthDBConfig.CONSUL_ROOT_DB_PASS_KEY,
                                   CryptoFacility.encrypt(newPassword));
                logger.info("Successfully changed the SQL DB root password");
            } else {
                logger.info("Unable to locate root user");
            }
            return changed;
        } catch (ClassNotFoundException | IllegalAccessException | InstantiationException e) {
            logger.error("Error establishing JDBC connection:", e);
            return false;
        } catch (SQLException e) {
            logger.error("Error changing SQL DB root password:", e);
            return false;
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    logger.debug("Error closing the JDBC connection.", e);
                }
            }
        }
    }

    /**
     * Retrieves the Arango database root password.
     *
     * @return The database root password.
     */
    @Override
    public @Nonnull String getRootArangoDBPassword() {
        Optional<String> rootDbPassword = keyValueStore_.get(AuthDBConfig.ARANGO_ROOT_PW_KEY);
        return CryptoFacility.decrypt(
            rootDbPassword.orElseThrow(() -> new SecurityException("No root Arango DB password")));
    }

    /**
     * Retrieves the Influx database root password.
     *
     * @return The database root password.
     */
    @Override
    public @Nonnull String getRootInfluxPassword() {
        Optional<String> rootDbPassword = keyValueStore_.get(AuthDBConfig.INFLUX_ROOT_PW_KEY);
        return CryptoFacility.decrypt(
            rootDbPassword.orElseThrow(() -> new SecurityException("No root Influx DB password")));
    }

    /**
     * Deletes the key and the secure data associated with the key.
     * Either the owner or administrator may perform this action.
     *
     * @param subject The subject.
     * @param key     The secure storage key.
     * @throws AuthorizationException In case of an error verifying the JWT token.
     */
    public void delete(final @Nonnull String subject, final @Nonnull String key)
            throws AuthorizationException {
        Condition condition = STORAGE.PATH.equal(key).and(STORAGE.OWNER.equal(subject));
        StorageRecord record = dslContext_.fetchOne(STORAGE, condition);
        record.delete();
    }
}
