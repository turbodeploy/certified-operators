package com.vmturbo.auth.api.usermgmt;

import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * The AuthUserModifyDTO represents the USER DTO with the new password to be exchanged with the
 * AUTH component.
 */
public class AuthUserModifyDTO extends AuthUserDTO {
    /**
     * The new password.
     */
    private String newPassword;

    /**
     * Constructs the AuthUserModifyDTO.
     * We keep it non-public, so that only the JSON deserialization could use it.
     */
    private AuthUserModifyDTO() {
    }

    /**
     * Constructs the AuthUserDTO.
     *
     * @param user        The user name.
     * @param password    The cleartext password.
     * @param roles       The list of roles.
     * @param newPassword The new password.
     */
    public AuthUserModifyDTO(final @Nonnull String user, final @Nullable String password,
                             final @Nonnull List<String> roles,
                             final @Nonnull String newPassword) {
        super(null, user, password, null, null, null, roles);
        this.newPassword = newPassword;
    }

    /**
     * Constructs the AuthUserDTO.
     *
     * @param provider    The login provider.
     * @param user        The user name.
     * @param password    The cleartext password.
     * @param uuid        The user's UUID.
     * @param token       The AUTH token.
     * @param roles       The list of roles.
     * @param newPassword The new password.
     */
    public AuthUserModifyDTO(final @Nullable PROVIDER provider, final @Nonnull String user,
                             final @Nullable String password,
                             final @Nullable String uuid, final @Nullable String token,
                             final @Nonnull List<String> roles,
                             final @Nonnull String newPassword) {
        super(provider, user, password, null, uuid, token, roles);
        this.newPassword = newPassword;
    }

    /**
     * Returns the new password.
     *
     * @return The new password
     */
    public String getNewPassword() {
        return newPassword;
    }
}
