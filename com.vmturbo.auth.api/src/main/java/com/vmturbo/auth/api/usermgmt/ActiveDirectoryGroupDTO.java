package com.vmturbo.auth.api.usermgmt;

import javax.annotation.Nonnull;

/**
 * An ActiveDirectoryGroupDTO represents a Active Directory group and associated roles.
 * Any AD user that is authenticated using an AD group will inherit the roles specified in that
 * group.
 */
public class ActiveDirectoryGroupDTO {
    /**
     * The display name.
     */
    private String displayName;

    /**
     * The type.
     */
    private String type;

    /**
     * The role name.
     */
    private String roleName;

    /**
     * Constructs the ActiveDirectoryGroupDTO.
     * We need this constructor for the the JSON deserialization.
     * We do not wish this constructor to be publicly accessible.
     */
    private ActiveDirectoryGroupDTO() {
    }

    /**
     * Constructs the ActiveDirectoryGroupDTO.
     *
     * @param displayName The display name.
     * @param type        The type.
     * @param roleName    The role name.
     */
    public ActiveDirectoryGroupDTO(final @Nonnull String displayName, final @Nonnull String type,
                                   final @Nonnull String roleName) {
        this.displayName = displayName;
        this.type = type;
        this.roleName = roleName;
    }

    /**
     * Returns the display name.
     *
     * @return The display name.
     */
    public @Nonnull String getDisplayName() {
        return displayName;
    }

    /**
     * Returns the type.
     *
     * @return The type.
     */
    public @Nonnull String getType() {
        return type;
    }

    /**
     * Returns the role name.
     *
     * @return The role name.
     */
    public @Nonnull String getRoleName() {
        return roleName;
    }

    /**
     * Sets the role name.
     *
     * @param roleName The role name.
     */
    public void setRoleName(final String roleName) {
        this.roleName = roleName;
    }
}
