package com.vmturbo.auth.api.authentication.credentials;

import java.util.Objects;
import javax.annotation.Nonnull;

import com.vmturbo.auth.api.authentication.ICredentials;

/**
 * The BasicCredentials represents a user/password credentials.
 */
public class BasicCredentials implements ICredentials {
    /**
     * The user name.
     */
    private final String userName_;

    /**
     * The user password.
     */
    private final String password_;

    /**
     * Constructs the basic credentials object.
     *
     * @param userName The user name.
     * @param password The password.
     */
    BasicCredentials(final @Nonnull String userName, final @Nonnull String password) {
        userName_ = userName;
        password_ = password;
    }

    /**
     * Returns the user name.
     *
     * @return The user name.
     */
    public @Nonnull String getUserName() {
        return userName_;
    }

    /**
     * Returns the password.
     *
     * @return The password.
     */
    public @Nonnull String getPassword() {
        return password_;
    }

    /**
     * Checks whether this object equals the one passed in.
     *
     * @param o The object to test against.
     * @return {@code true} iff this equals to {@link #o}
     */
    @Override
    public boolean equals(Object o) {
        if (!(o instanceof BasicCredentials)) {
            return false;
        }

        BasicCredentials creds = (BasicCredentials)o;
        return userName_.equals(creds.userName_) && password_.equals(creds.password_);
    }

    /**
     * Returns the hash code of the object.
     *
     * @return The hash code of the object.
     */
    @Override
    public int hashCode() {
        return Objects.hash(userName_, password_);
    }

}
