package com.vmturbo.auth.api.db;

import java.io.Serializable;

/**
 * The {@link DBPasswordDTO} implements the database password DTO.
 */
public class DBPasswordDTO implements Serializable {
    /**
     * The existing password.
     */
    private String existingPassword;

    /**
     * The new password.
     */
    private String newPassword;

    /**
     * Returns the existing password.
     *
     * @return The existing password.
     */
    public String getExistingPassword() {
        return existingPassword;
    }

    /**
     * Sets the existing password.
     *
     * @param existingPassword The existing password.
     */
    public void setExistingPassword(String existingPassword) {
        this.existingPassword = existingPassword;
    }

    /**
     * Returns the new password.
     *
     * @return The new password.
     */
    public String getNewPassword() {
        return newPassword;
    }

    /**
     * Sets the new password.
     *
     * @param newPassword The new password.
     */
    public void setNewPassword(String newPassword) {
        this.newPassword = newPassword;
    }
}
