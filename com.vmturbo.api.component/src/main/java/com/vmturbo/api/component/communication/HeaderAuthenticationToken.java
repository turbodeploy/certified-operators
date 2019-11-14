package com.vmturbo.api.component.communication;

import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.springframework.security.authentication.AbstractAuthenticationToken;

/**
 * Custom Spring authentication/authorization token to carry request headers.
 */
public class HeaderAuthenticationToken extends AbstractAuthenticationToken {

    private static final long serialVersionUID = -1949976839306453198L;
    private final String remoteIpAddress;
    private String userName;
    private String group;
    private Optional<String> role;
    private String jwtToken;

    private HeaderAuthenticationToken(Builder builder) {
        // not passing in any permission
        super(null);
        this.group = builder.group;
        this.userName = builder.user;
        this.remoteIpAddress = builder.remoteIpAddress;
        this.role = builder.role;
        this.jwtToken = builder.jwtToken;
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
     * @param jwtToken JWT token.
     * @param remoteIpAddress remote IP address
     * @return the builder for {@link HeaderAuthenticationToken}.
     */
    public static Builder newBuilder(@Nonnull String jwtToken, @Nonnull String remoteIpAddress) {
        return new Builder(Objects.requireNonNull(jwtToken),
                Objects.requireNonNull(remoteIpAddress));
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
     * Getter for authenticated JWT token.
     *
     * @return user JWT token if available in the header.
     */
    public Optional<String> getJwtToken() {
        return Optional.ofNullable(jwtToken);
    }

    /**
     * Builder.
     */
    public static class Builder {
        private String user;
        private String group;
        private String remoteIpAddress;
        private String jwtToken;
        private Optional<String> role;

        private Builder(@Nonnull String user, @Nonnull String group,
                @Nonnull String remoteIpAddress) {
            this.user = user;
            this.group = group;
            this.remoteIpAddress = remoteIpAddress;
        }

        private Builder(@Nonnull String jwtToken, @Nonnull String remoteIpAddress) {
            this.user = "anonymous";
            this.jwtToken = jwtToken;
            this.remoteIpAddress = remoteIpAddress;
        }

        /**
         * Set user role.
         *
         * @param role user roles
         * @return user role.
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
    }
}