package com.vmturbo.api.component.external.api.util;

import javax.annotation.Nonnull;

import org.apache.commons.lang3.StringUtils;

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
     * The name of the "viewer" user. All non-administrator users share the same "viewer" user,
     * since that user is read-only.
     */
    private final String viewerUsername;

    private final boolean enableReporting;

    /**
     * Create a new instance.
     *
     * @param enableReporting Whether or not reporting is enabled in the system.
     * @param viewerUsername The username for the reporting viewer. All non-administrators share
     *                       this "view-only" guest-like username.
     */
    public ReportingUserCalculator(final boolean enableReporting,
            @Nonnull final String viewerUsername) {
        this.viewerUsername = viewerUsername;
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
            String reportingUsername = viewerUsername;
            if (StringUtils.equalsIgnoreCase(me.getRoleName(), SecurityConstant.ADMINISTRATOR)) {
                reportingUsername = me.getUsername();
            }
            return new LoggedInUserInfo(me, reportingUsername);
        } else {
            return new LoggedInUserInfo(me, null);
        }
    }
}
