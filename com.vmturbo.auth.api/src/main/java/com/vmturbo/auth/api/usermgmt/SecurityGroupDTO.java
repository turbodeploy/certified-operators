package com.vmturbo.auth.api.usermgmt;

import java.util.List;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

/**
 * An SecurityGroupDTO represents a SSO group and associated roles, and potentially entity scope.
 * Any SSO user that is authenticated using an SSO group will inherit the roles specified in that
 * group and entity scope (if applicable).
 *
 * This class is immutable.
 */
@Immutable
public class SecurityGroupDTO {
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
     * The set of scope groups, if any
     */
    private @Nullable List<Long> scopeGroups;

    /**
     * Constructs the ActiveDirectoryGroupDTO.
     * We need this constructor for the the JSON deserialization.
     * We do not wish this constructor to be publicly accessible.
     */
    private SecurityGroupDTO() {
    }

    /**
     * Constructs the ActiveDirectoryGroupDTO.
     *
     * @param displayName The display name.
     * @param type        The type.
     * @param roleName    The role name.
     */
    public SecurityGroupDTO(final @Nonnull String displayName, final @Nonnull String type,
                            final @Nonnull String roleName) {
        this(displayName, type, roleName, null);
    }

    /**
     * Constructs the ActiveDirectoryGroupDTO with scope
     *
     * @param displayName The display name.
     * @param type        The type.
     * @param roleName    The role name.
     * @param scopeGroups The list of scope groups for this group
     */
    public SecurityGroupDTO(final @Nonnull String displayName, final @Nonnull String type,
                            final @Nonnull String roleName, final @Nullable List<Long> scopeGroups) {
        this.displayName = displayName;
        this.type = type;
        this.roleName = roleName;
        this.scopeGroups = scopeGroups;
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
     * Returns the list of scope groups the user may have. (can be null)
     *
     * @return a list of the user's scope groups
     */
    public List<Long> getScopeGroups() { return scopeGroups; }

}
