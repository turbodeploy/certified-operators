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
     * The name of the user that gets mapped to a "real" reporting user. Other users get mapped
     * to the "viewer" user.
     *
     * <p/>TODO (roman, Oct 30 2020): Make this configurable from the UI. When it changes, delete
     *                                the previous one from Grafana.
     */
    private final String reportUsername = "administrator";

    /**
     * The name of the "viewer" user. By default, users share the same "viewer" user,
     * since that user is read-only.
     */
    private final String viewerUsername;

    private final boolean enableReporting;

    private final boolean samlEnabled;

    /**
     * Create a new instance.
     *
     * @param enableReporting Whether or not reporting is enabled in the system.
     * @param samlEnabled If SAML SSO is enabled.
     * @param viewerUsername The username for the reporting viewer. All non-administrators share
     *                       this "view-only" guest-like username.
     */
    public ReportingUserCalculator(final boolean enableReporting,
            final boolean samlEnabled,
            @Nonnull final String viewerUsername) {
        this.viewerUsername = viewerUsername;
        this.samlEnabled = samlEnabled;
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
            if (samlEnabled) {
                // In the SSO case there is no "administrator" user available via the API, so we
                // fall back to role. This means that multiple SSO administrators will result in
                // multiple Grafana administrators, which will exceed the license limits.
                // We will fix that issue when we make the "Reporting" user configurable from the UI.
                if (StringUtils.equalsIgnoreCase(me.getRoleName(), SecurityConstant.ADMINISTRATOR)) {
                    reportingUsername = me.getUsername();
                }
            } else if (StringUtils.equalsIgnoreCase(me.getUsername(), reportUsername)) {
                // In the normal case there is a single "administrator" user, which we can use
                // to schedule reports.
                reportingUsername = me.getUsername();
            }
            return new LoggedInUserInfo(me, reportingUsername);
        } else {
            return new LoggedInUserInfo(me, null);
        }
    }
}
