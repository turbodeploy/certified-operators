package com.vmturbo.auth.api.usermgmt;

import java.io.Serializable;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * An AuthUserDTO represents the User object to be exchanged with the AUTH component.
 */
public class AuthUserDTO implements Serializable {
    /**
     * The login provider type.
     */
    public enum PROVIDER {
        LOCAL, LDAP;
    }

    /**
     * The user name.
     */
    private String user;

    /**
     * The cleartext password.
     * A password may not be always set. One example could be an AD user.
     */
    private String password;

    /**
     * The AUTH token.
     * In some cases, we don't yet have the AUTH token, so this field could be {@code null}.
     */
    private String token;

    /**
     * The roles.
     */
    private List<String> roles;

    /**
     * The login provider.
     * We do not always specify it. For example, when we authenticate an user, a return
     * AuthUserDTO might not contain the provider.
     */
    private PROVIDER provider;

    /**
     * The UUID.
     * When we pass an AuthUserDTO to the AUTH component, the UUID is not yet known.
     */
    private String uuid;

    /**
     * Constructs the AuthUserDTO.
     * We keep it non-public, so that only the JSON deserialization could use it.
     */
    protected AuthUserDTO() {
    }

    /**
     * Constructs the AuthUserDTO.
     *
     * @param user     The user name.
     * @param uuid     The user's UUID.
     * @param roles    The list of roles.
     */
    public AuthUserDTO(final @Nonnull String user,
                       final @Nullable String uuid,
                       final @Nonnull List<String> roles) {
        this(null, user, uuid, roles);
    }

    /**
     * Constructs the AuthUserDTO.
     *
     * @param provider The provider.
     * @param user     The user name.
     * @param uuid     The user's UUID.
     * @param roles    The list of roles.
     */
    public AuthUserDTO(final @Nullable PROVIDER provider, final @Nonnull String user,
                       final @Nullable String uuid,
                       final @Nonnull List<String> roles) {
        this(provider, user, null, uuid, null, roles);
    }

    /**
     * Constructs the AuthUserDTO.
     *
     * @param provider The login provider.
     * @param user     The user name.
     * @param password The cleartext password.
     * @param uuid     The user's UUID.
     * @param token    The AUTH token.
     * @param roles    The list of roles.
     */
    public AuthUserDTO(final @Nullable PROVIDER provider, final @Nonnull String user,
                       final @Nullable String password,
                       final @Nullable String uuid, final @Nullable String token,
                       final @Nonnull List<String> roles) {
        this.provider = provider;
        this.user = user;
        this.password = password;
        this.uuid = uuid;
        this.token = token;
        this.roles = roles;
    }

    /**
     * Returns the UUID.
     *
     * @return The UUID.
     */
    public String getUuid() {
        return uuid;
    }

    /**
     * Returns the login provider.
     *
     * @return The login provider.
     */
    public PROVIDER getProvider() {
        return provider;
    }

    /**
     * Returns the AUTH token.
     *
     * @return The AUTH token.
     */
    public String getToken() {
        return token;
    }

    /**
     * Returns the user name.
     *
     * @return The user name.
     */
    public String getUser() {
        return user;
    }

    /**
     * Returns the cleartext password.
     *
     * @return The cleartext password.
     */
    public String getPassword() {
        return password;
    }

    /**
     * Returns the list of roles.
     *
     * @return The list of roles.
     */
    public List<String> getRoles() {
        return roles;
    }
}
