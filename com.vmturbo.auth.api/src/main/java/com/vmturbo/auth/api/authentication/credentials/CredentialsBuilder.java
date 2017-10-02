package com.vmturbo.auth.api.authentication.credentials;

import java.util.Objects;
import javax.annotation.Nonnull;

import com.vmturbo.auth.api.authentication.ICredentials;

/**
 * The CredentialsBuilder represents a Fluent API credentials builder.
 */
public class CredentialsBuilder {
    /**
     * The user name.
     */
    private String userName_;

    /**
     * The user password.
     */
    private String password_;

    /**
     * The domain.
     */
    private String domain_;

    /**
     * The builder is never constructed directly.
     */
    private CredentialsBuilder() {
    }

    /**
     * Creates the instance of the credentials builder.
     *
     * @return The instance of the credentials builder.
     */
    public static CredentialsBuilder builder() {
        return new CredentialsBuilder();
    }

    /**
     * Sets user name.
     *
     * @param userName The user name.
     * @return This instance of the credentials builder.
     */
    public CredentialsBuilder withUser(final @Nonnull String userName) {
        userName_ = Objects.requireNonNull(userName);
        return this;
    }

    /**
     * Sets password.
     *
     * @param password The user password.
     * @return This instance of the credentials builder.
     */
    public CredentialsBuilder withPassword(final @Nonnull String password) {
        password_ = Objects.requireNonNull(password);
        return this;
    }

    /**
     * Sets domain.
     *
     * @param domain The AD domain.
     * @return This instance of the credentials builder.
     */
    public CredentialsBuilder withDomain(final @Nonnull String domain) {
        domain_ = Objects.requireNonNull(domain);
        return this;
    }

    /**
     * Builds the credentials object.
     *
     * @return The credentials object.
     */
    public @Nonnull ICredentials build() {
        // Make sure we didn't skip a step.
        Objects.requireNonNull(userName_);
        Objects.requireNonNull(password_);
        if (domain_ == null) {
            return new BasicCredentials(userName_, password_);
        }
        Objects.requireNonNull(domain_);
        return new ADCredentials(userName_, password_, domain_);
    }
}
