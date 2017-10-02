package com.vmturbo.auth.api.authentication.credentials;

import java.util.Objects;
import javax.annotation.Nonnull;

/**
 * The ADCredentials represents Active Directory credentials.
 */
public class ADCredentials extends BasicCredentials {
    /**
     * The domain.
     */
    private String domain_;

    /**
     * Constructs the basic credentials object.
     *
     * @param userName The user name.
     * @param password The password.
     */
    ADCredentials(final @Nonnull String userName, final @Nonnull String password,
                  final @Nonnull String domain) {
        super(userName, password);
        domain_ = domain;
    }

    /**
     * Returns the domain.
     *
     * @return The domain.
     */
    public @Nonnull String getDomain() {
        return domain_;
    }

    /**
     * Checks whether this object equals the one passed in.
     *
     * @param o The object to test against.
     * @return {@code true} iff this equals to {@code o}
     */
    @Override
    public boolean equals(Object o) {
        if (!(o instanceof ADCredentials)) {
            return false;
        }

        ADCredentials creds = (ADCredentials)o;
        return super.equals(o) && domain_.equals(creds.domain_);
    }

    /**
     * Returns the hash code of the object.
     *
     * @return The hash code of the object.
     */
    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), domain_);
    }

}
