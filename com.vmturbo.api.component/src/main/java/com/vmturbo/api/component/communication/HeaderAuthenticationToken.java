package com.vmturbo.api.component.communication;

import java.security.PublicKey;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.springframework.security.authentication.AbstractAuthenticationToken;

import com.vmturbo.api.component.security.HeaderMapper;

/**
 * Custom Spring authentication/authorization token to carry request headers.
 */
public class HeaderAuthenticationToken extends AbstractAuthenticationToken {

    private static final long serialVersionUID = -1949976839306453198L;
    private final String remoteIpAddress;
    private final HeaderMapper headerMapper;
    private final String userName;
    private final String group;
    private final Optional<String> role;
    private final String jwtToken;
    private final Optional<PublicKey> publicKey;
    private final boolean isLatest;

    private HeaderAuthenticationToken(Builder builder) {
        // not passing in any permission
        super(null);
        this.group = builder.group;
        this.userName = builder.user;
        this.remoteIpAddress = builder.remoteIpAddress;
        this.role = builder.role;
        this.jwtToken = builder.jwtToken;
        this.publicKey = builder.publicKey;
        this.headerMapper = builder.headerMapper;
        this.isLatest = builder.isLatest;
    }

    /**
     * Builder for {@link HeaderAuthenticationToken}.
     *
     * @param userName user name.
     * @param group belong to security group.
     * @param remoteIpAddress remote IP address
     * @return the builder for {@link HeaderAuthenticationToken}.
     */
    public static Builder newBuilder(@Nonnull String userName, @Nonnull String group,
            @Nonnull String remoteIpAddress) {
        return new Builder(Objects.requireNonNull(userName), Objects.requireNonNull(group),
                Objects.requireNonNull(remoteIpAddress));
    }

    /**
     * Builder for {@link HeaderAuthenticationToken}.
     *
     * @param publicKey public key for JWT token.
     * @param jwtToken JWT token.
     * @param remoteIpAddress remote IP address.
     * @param headerMapper header mapper.
     * @return the builder for {@link HeaderAuthenticationToken}.
     */
    public static Builder newBuilder(@Nonnull PublicKey publicKey, @Nonnull String jwtToken,
            @Nonnull String remoteIpAddress, @Nonnull HeaderMapper headerMapper) {
        return new Builder(Objects.requireNonNull(publicKey), Objects.requireNonNull(jwtToken),
                Objects.requireNonNull(remoteIpAddress), Objects.requireNonNull(headerMapper));
    }

    @Override
    public Object getCredentials() {
        return userName;
    }

    @Override
    public Object getPrincipal() {
        return userName;
    }

    /**
     * Getter for user name.
     *
     * @return user name.
     */
    public String getUserName() {
        return userName;
    }

    /**
     * Getter for security group.
     *
     * @return security group.
     */
    public String getGroup() {
        return group;
    }

    /**
     * Getter for remote client IP address.
     *
     * @return remote client IP address.
     */
    public String getRemoteIpAddress() {
        return remoteIpAddress;
    }

    /**
     * Getter for user role.
     *
     * @return user role if available in the header.
     */
    public Optional<String> getRole() {
        return role;
    }

    /**
     * Getter for public key.
     *
     * @return public key if available in the header.
     */
    public Optional<PublicKey> getPublicKey() {
        return publicKey;
    }

    /**
     * Getter for authenticated JWT token.
     *
     * @return user JWT token if available in the header.
     */
    public Optional<String> getJwtToken() {
        return Optional.ofNullable(jwtToken);
    }

    /**
     * Getter for {@link HeaderMapper}.
     *
     * @return {@link HeaderMapper}.
     */
    public HeaderMapper getHeaderMapper() {
        return headerMapper;
    }

    /**
     * Getter to indicate is the latest version.
     *
     * @return is latest version.
     */
    public boolean isLatest() {
        return isLatest;
    }

    /**
     * Builder.
     */
    public static class Builder {
        private final Optional<PublicKey> publicKey;
        private HeaderMapper headerMapper;
        private String user;
        private String group;
        private String remoteIpAddress;
        private String jwtToken;
        private Optional<String> role;
        private boolean isLatest = true;

        private Builder(@Nonnull String user, @Nonnull String group,
                @Nonnull String remoteIpAddress) {
            this.user = user;
            this.group = group;
            this.remoteIpAddress = remoteIpAddress;
            this.publicKey = Optional.empty();
        }

        private Builder(@Nonnull PublicKey key, @Nonnull String jwtToken,
                @Nonnull String remoteIpAddress, @Nonnull HeaderMapper headerMapper) {
            this.user = "anonymous";
            this.publicKey = Optional.of(key);
            this.jwtToken = jwtToken;
            this.remoteIpAddress = remoteIpAddress;
            this.headerMapper = headerMapper;
        }

        /**
         * Set user role.
         *
         * @param role user roles
         * @return builder.
         */
        public Builder setRole(@Nonnull Optional<String> role) {
            Objects.requireNonNull(role);
            this.role = role;
            return this;
        }

        /**
         * Build {@link HeaderAuthenticationToken}.
         *
         * @return {@link HeaderAuthenticationToken}.
         */
        public HeaderAuthenticationToken build() {
            return new HeaderAuthenticationToken(this);
        }

        /**
         * Is latest version.
         *
         * @param isLatest latest version?
         * @return builder.
         */
        public Builder setIsLatest(boolean isLatest) {
            this.isLatest = isLatest;
            return this;
        }
    }
}