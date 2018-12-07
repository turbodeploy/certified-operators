package com.vmturbo.auth.api.usermgmt;

import javax.annotation.Nonnull;

/**
 * The AuthUserModifyDTO represents the USER DTO with the new password to be exchanged with the
 * AUTH component.
 */
public class AuthUserModifyDTO {

    /**
     * The user information
     */
    private AuthUserDTO userToModify;

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
     * Constructs the AuthUserModifyDTO w/new password to use
     *
     * @param userToModify the {@link AuthUserDTO} encapsulating the user record to modify
     * @param newPassword the new password to set
     */
    public AuthUserModifyDTO(final @Nonnull AuthUserDTO userToModify,
                             final @Nonnull String newPassword) {
        this.userToModify = userToModify;
        this.newPassword = newPassword;
    }

    /**
     * Get the user object to modify
     *
     * @return
     */
    public AuthUserDTO getUserToModify() { return userToModify; }

    /**
     * Returns the new password.
     *
     * @return The new password
     */
    public String getNewPassword() {
        return newPassword;
    }
}
