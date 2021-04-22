package com.vmturbo.api.component.external.api.util;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import javax.annotation.Nonnull;

import it.unimi.dsi.fastutil.booleans.BooleanArrayList;
import it.unimi.dsi.fastutil.booleans.BooleanList;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.dto.user.UserApiDTO;
import com.vmturbo.api.serviceinterfaces.IUsersService.LoggedInUserInfo;
import com.vmturbo.auth.api.authorization.jwt.SecurityConstant;
import com.vmturbo.auth.api.licensing.LicenseCheckClient;
import com.vmturbo.common.protobuf.LicenseProtoUtil;
import com.vmturbo.components.api.FormattedString;

/**
 * Determines and sets the relevant embedded reporting headers for the "current logged in user".
 *
 * <p/>See: https://vmturbo.atlassian.net/wiki/spaces/XD/pages/1575584281/Grafana+User+Management
 */
public class ReportingUserCalculator {

    private final boolean enableReporting;

    private final ReportEditorNamePool reportEditorNamePool;

    private final Map<String, ReportEditorName> editorNameMap = new HashMap<>();

    private static final Logger logger = LogManager.getLogger();

    /**
     * Create a new instance.
     *
     * @param enableReporting Whether or not reporting is enabled in the system.
     * @param editorUsername The username for the reporting editor. THe user with the REPORT_EDITOR role
     *                       uses this username to log into Grafana.
     * @param licenseCheckClient The licenseCheckClient to extract license information. LicenseSummary contains
     *                           the number of allowed report editors.
     */
    public ReportingUserCalculator(final boolean enableReporting,
            @Nonnull final String editorUsername,
            @Nonnull final LicenseCheckClient licenseCheckClient) {
        this.enableReporting = enableReporting;
        this.reportEditorNamePool = new ReportEditorNamePool(editorUsername, LicenseProtoUtil.numberOfSupportedReportEditors(licenseCheckClient.geCurrentLicenseSummary()));

        // On change event of a licenseSummary, update pool of available report editor names.
        // Downsizing (i.e. removing report editors) is not supported and has to be handled manually.
        licenseCheckClient.getUpdateEventStream().subscribe(summary -> {
            int newReportEditorCount = LicenseProtoUtil.numberOfSupportedReportEditors(summary);
            int currentPoolSize = reportEditorNamePool.getSize();

            if (newReportEditorCount < currentPoolSize) {
                logger.warn("Updated license supports {} editors. Previous NamePool size: {}."
                        + "Manual removal of report editors required", newReportEditorCount, currentPoolSize);
            } else {
                reportEditorNamePool.resize(newReportEditorCount);
            }
        });
    }

    /**
     * Called whenever a user is deleted via the API.
     *
     * @param userApiDTO The {@link UserApiDTO} of the modified user.
     */
    public synchronized void onUserDeleted(@Nonnull final UserApiDTO userApiDTO) {
        final String key = getUserKey(userApiDTO);
        final ReportEditorName name = editorNameMap.remove(key);
        if (name != null) {
            logger.info("User {} deleted - releasing associated report editor name {}.",
                key, name.getName());
            reportEditorNamePool.releaseName(name);
        }
    }

    /**
     * Called whenever a user is modified via the API.
     *
     * @param user The modified user.
     */
    public synchronized void onUserModified(@Nonnull final UserApiDTO user) {
        // If the user doesn't have the editor role, it may have lost the editor role. Let's check.
        // If the user DOES have the editor role, we don't need to actually assign a report editor
        // username to it until the user tries to access reports.
        if (!ApiUtils.hasUserRole(user, SecurityConstant.REPORT_EDITOR)) {
            final String key = getUserKey(user);
            final ReportEditorName name = editorNameMap.remove(key);
            if (name != null) {
                logger.info("User {} lost editor role - releasing associated report editor name {}.",
                        key, name.getName());
                reportEditorNamePool.releaseName(name);
            }
        }
    }

    /**
     * Get the {@link LoggedInUserInfo} for a {@link UserApiDTO}, with any relevant reporting
     * headers set.
     *
     * @param me The {@link UserApiDTO} of the logged in user.
     * @return The {@link LoggedInUserInfo}.
     */
    @Nonnull
    public synchronized LoggedInUserInfo getMe(@Nonnull final UserApiDTO me) {
        if (enableReporting) {
            final String key = getUserKey(me);
            String reportingUsername = me.getUsername();
            final boolean hasReportEditorRole = ApiUtils.hasUserRole(me, SecurityConstant.REPORT_EDITOR);

            if (hasReportEditorRole) {
                if (editorNameMap.containsKey(key)) {
                    reportingUsername = editorNameMap.get(key).getName();
                } else {
                    Optional<ReportEditorName> n = reportEditorNamePool.getAvailableName();
                    if (n.isPresent()) {
                        reportingUsername = n.get().getName();
                        editorNameMap.put(key, n.get());
                    } else {
                        logger.error("No more Report Editor roles available. {} will be logged in as viewer", reportingUsername);
                    }
                }
            }
            return new LoggedInUserInfo(me, reportingUsername);
        } else {
            return new LoggedInUserInfo(me, null);
        }
    }

    private String getUserKey(@Nonnull final UserApiDTO userApiDTO) {
        return FormattedString.format("{}:{}", userApiDTO.getLoginProvider(), userApiDTO.getUsername()).toUpperCase();
    }

    /**
     * A utility to represent a fixed-size pool of names (e.g. n-0, n-1, n-2, n-3). Tracks the
     * available names.
     */
    private static class ReportEditorNamePool {
        private final String prefix;
        private final BooleanList availability;

        ReportEditorNamePool(@Nonnull final String prefix, final int size) {
            this.prefix = prefix;
            this.availability = new BooleanArrayList(size);
            for (int i = 0; i < size; ++i) {
                this.availability.add(true);
            }
        }

        public synchronized Optional<ReportEditorName> getAvailableName() {
            for (int i = 0; i < availability.size(); ++i) {
                if (availability.getBoolean(i)) {
                    availability.set(i, false);
                    return Optional.of(new ReportEditorName(prefix, i));
                }
            }
            return Optional.empty();
        }

        public synchronized int getSize() {
            return availability.size();
        }

        public synchronized void releaseName(@Nonnull final ReportEditorName name) {
            availability.set(name.idx, true);
        }

        public synchronized void resize(final int size) {
            int previousSize = availability.size();
            availability.size(size);

            for (int i = previousSize; i < size; i++) {
                availability.set(i, true);
            }
        }
    }

    /**
     * Tuple returned by {@link ReportEditorNamePool} to avoid parsing the string of the
     * name to get back the index.
     */
    private static class ReportEditorName {
        private final String name;
        private final int idx;

        ReportEditorName(@Nonnull final String prefix, final int idx) {
            this.name = LicenseProtoUtil.formatReportEditorUsername(prefix, idx);
            this.idx = idx;
        }

        @Nonnull
        public String getName() {
            return name;
        }
    }
}
