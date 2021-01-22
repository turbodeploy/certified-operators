package com.vmturbo.api.component.external.api.util;

import javax.annotation.Nonnull;

import org.apache.commons.lang3.StringUtils;

import com.vmturbo.api.dto.user.RoleApiDTO;
import com.vmturbo.api.dto.user.UserApiDTO;
import com.vmturbo.api.serviceinterfaces.IUsersService.LoggedInUserInfo;
import com.vmturbo.auth.api.authorization.jwt.SecurityConstant;

/**
 * Determines and sets the relevant embedded reporting headers for the "current logged in user".
 *
 * <p/>See: https://vmturbo.atlassian.net/wiki/spaces/XD/pages/1575584281/Grafana+User+Management
 */
public class ReportingUserCalculator {

    /**
     * The name of the "editor" user. By default, users share the same "viewer" user,
     * since that user is read-only.
     */
    private final String editorUsername;

    private final boolean enableReporting;

    /**
     * Create a new instance.
     *
     * @param enableReporting Whether or not reporting is enabled in the system.
     * @param editorUsername The username for the reporting editor. THe user with the REPORT_EDITOR role
     *                       uses this username to log into Grafana.
     */
    public ReportingUserCalculator(final boolean enableReporting,
            @Nonnull final String editorUsername) {
        this.editorUsername = editorUsername;
        this.enableReporting = enableReporting;
    }

    /**
     * Get the {@link LoggedInUserInfo} for a {@link UserApiDTO}, with any relevant reporting
     * headers set.
     *
     * @param me The {@link UserApiDTO} of the logged in user.
     * @return The {@link LoggedInUserInfo}.
     */
    @Nonnull
    public LoggedInUserInfo getMe(@Nonnull final UserApiDTO me) {
        if (enableReporting) {
            String reportingUsername = me.getUsername();
            final boolean hasReportEditorRole = me.getRoles().stream()
                .map(RoleApiDTO::getName)
                .filter(name -> !StringUtils.isBlank(name))
                .anyMatch(roleName -> roleName.equalsIgnoreCase(SecurityConstant.REPORT_EDITOR));
            if (hasReportEditorRole) {
                reportingUsername = editorUsername;
            }
            return new LoggedInUserInfo(me, reportingUsername);
        } else {
            return new LoggedInUserInfo(me, null);
        }
    }
}
