package com.vmturbo.extractor.grafana;

import java.util.Map;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import javax.annotation.Nonnull;

import com.google.common.base.Stopwatch;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.components.common.RequiresDataInitialization;
import com.vmturbo.components.common.utils.BuildProperties;
import com.vmturbo.extractor.grafana.client.GrafanaClient;
import com.vmturbo.extractor.grafana.model.DashboardSpec;
import com.vmturbo.extractor.grafana.model.DashboardSpec.UpsertDashboardRequest;
import com.vmturbo.extractor.grafana.model.DashboardVersion;
import com.vmturbo.extractor.grafana.model.DatasourceInput;
import com.vmturbo.extractor.grafana.model.Folder;
import com.vmturbo.extractor.grafana.model.UserInput;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.sql.utils.DbEndpointConfig;

/**
 * This object drives the Grafana initialization process, and makes all the necessary API calls
 * to ensure the Grafana instance has all the datasources, folders, and dashboards in the
 * latest release.
 *
 * <p/>The {@link Grafanon#initialize()} method kicks off the thread which repeatedly tries to
 * initialize Grafana until it succeeds.
 */
public class Grafanon implements RequiresDataInitialization {

    private static final String COMMIT_MSG_DELIMITER = ":";

    private static final Logger logger = LogManager.getLogger();

    private final GrafanonConfig grafanonConfig;

    private final GrafanaClient grafanaClient;

    private final DashboardsOnDisk dashboardsOnDisk;

    private CompletableFuture<RefreshSummary> initilizationResult = new CompletableFuture<>();

    Grafanon(@Nonnull final GrafanonConfig grafanonConfig,
            @Nonnull final DashboardsOnDisk dashboardsOnDisk,
            @Nonnull final GrafanaClient grafanaClient) {
        this.grafanonConfig = grafanonConfig;
        this.dashboardsOnDisk = dashboardsOnDisk;
        this.grafanaClient = grafanaClient;
    }

    @Nonnull
    CompletableFuture<RefreshSummary> getInitializationFuture() {
        return initilizationResult;
    }

    @Override
    public void initialize() {
        // We initialize asynchronously, with retries.
        new Thread(() -> {
            boolean initialized = false;
            while (!initialized) {
                final RefreshSummary refreshSummary = new RefreshSummary();
                try {
                    Stopwatch stopwatch = Stopwatch.createStarted();
                    refreshGrafana(refreshSummary);
                    logger.info("Grafana initialization completed successfully in {}ms", stopwatch.elapsed(TimeUnit.MILLISECONDS));
                    logger.info(refreshSummary.summarize(logger.isDebugEnabled()));
                    initialized = true;
                    initilizationResult.complete(refreshSummary);
                } catch (RuntimeException e) {
                    logger.error("Failed to initialize some, or all, of the Grafana setup. Error:", e);
                    refreshSummary.recordError(e);
                    logger.info(refreshSummary.summarize(logger.isDebugEnabled()));
                    try {
                        Thread.sleep(grafanonConfig.getErrorSleepIntervalMs());
                    } catch (InterruptedException interruptedException) {
                        logger.error("Grafana initialization thread interrupted." + " Exiting without completing initialization.",
                                interruptedException);
                        initilizationResult.completeExceptionally(interruptedException);
                        break;
                    }
                }
            }
        }, "grafanon-initialization").start();
    }

    /**
     * Refresh the Grafana configuration. This will attempt to bring the Grafana server up to date
     * with the most recent data sources, dashboards, and folders.
     *
     * @param refreshSummary The {@link RefreshSummary} object used to track operations performed
     *                       during this refresh.
     * @throws IllegalArgumentException If there is some configuration error.
     */
    public void refreshGrafana(@Nonnull final RefreshSummary refreshSummary) {
        if (grafanonConfig.getViewerUserInput().isPresent()) {
            grafanaClient.ensureUserExists(grafanonConfig.getViewerUserInput().get(), refreshSummary);
        }

        try {
            // Get the endpoint here (inside the initialization thread) to avoid blocking the
            // main thread.
            final DbEndpointConfig endpointConfig = grafanonConfig.getDbEndpointConfig().get();
            final DatasourceInput input = DatasourceInput.fromDbEndpoint(grafanonConfig.getTimescaleDisplayName(), endpointConfig);
            grafanaClient.upsertDefaultDatasource(input, refreshSummary);
        } catch (UnsupportedDialectException e) {
            throw new IllegalArgumentException("Invalid endpoint configuration.", e);
        }
        final Map<String, Long> existingDashboardsByUid = grafanaClient.dashboardIdsByUid();
        final Map<String, Folder> existingFolders = grafanaClient.foldersByUid();
        dashboardsOnDisk.visit((folderData) -> {
            Optional<Folder> folder = folderData.getFolderSpec().map(folderInput -> {
                Folder upsertedFolder = grafanaClient.upsertFolder(folderInput, existingFolders, refreshSummary);
                folderData.getPermissions().ifPresent(permissions -> {
                    grafanaClient.setFolderPermissions(folderInput.getUid(), permissions, refreshSummary);
                });
                return upsertedFolder;
            });
            folderData.getDashboardsByUid().forEach((uid, dashboardSpec) -> {
                processDashboard(uid, dashboardSpec, folder, existingDashboardsByUid, refreshSummary);
            });
        });
    }

    private void processDashboard(@Nonnull final String uid,
            @Nonnull final DashboardSpec dashboardSpec,
            @Nonnull final Optional<Folder> parentFolder,
            @Nonnull final Map<String, Long> existingDashboardsByUid,
            @Nonnull final RefreshSummary refreshSummary) {
        final Long existingDashboardId = existingDashboardsByUid.get(uid);
        final String dashboardCommitMsg = createDashboardCommitMessage();
        final UpsertDashboardRequest upsertRequest = dashboardSpec.creationRequest()
                .setMessage(dashboardCommitMsg);
        parentFolder.ifPresent(upsertRequest::setParentFolder);
        if (existingDashboardId == null) {
            // Straight-up create.
            grafanaClient.upsertDashboard(upsertRequest, refreshSummary);
        } else {
            DashboardVersion latestVersion = grafanaClient.getLatestVersion(existingDashboardId);
            if (latestVersion == null || dashboardChanged(latestVersion.getMessage(), dashboardCommitMsg)) {
                upsertRequest.setUpdateProperties(existingDashboardId, latestVersion);
                // Check if we need to update based on the last version.
                grafanaClient.upsertDashboard(upsertRequest, refreshSummary);
            } else {
                // No change required.
                logger.info("Skipping update for dashboard {}.", uid);
                refreshSummary.recordDashboardUnchanged(existingDashboardId, dashboardSpec);
            }
        }
    }

    /**
     * Determine whether or not we should re-upload the dashboard, based on the last uploaded
     * dashboard's commit message. See: {@link Grafanon#createDashboardCommitMessage()}.
     *
     * @param oldCommitMsg The last commit message.
     * @param newCommitMsg The potential replacement's commit message.
     * @return True if we should replace.
     */
    private boolean dashboardChanged(@Nonnull final String oldCommitMsg,
                                   @Nonnull final String newCommitMsg) {
        if (oldCommitMsg.equals(newCommitMsg)) {
            String[] oldMsgParts = oldCommitMsg.split(COMMIT_MSG_DELIMITER);
            if (oldCommitMsg.length() == 3) {
                // Check for the "dirty" case.
                // If "dirty" is "true" (i.e. there are local changes) or we are working with
                // a SNAPSHOT build, replace.
                final boolean dirty = Boolean.parseBoolean(oldMsgParts[2]);
                final boolean versionEndsWithSnapshot = oldMsgParts[0].endsWith("SNAPSHOT");
                return dirty || versionEndsWithSnapshot;
            } else {
                return true;
            }
        } else {
            return true;
        }
    }

    /**
     * Create a string message to use for the version of the dashboard associated with the
     * current release.
     *
     * <p/>The commit message will look something like: 7.22.3:ire8rf:false.
     * (maven version):(last git commit hash):(dirty)
     *
     * @return The string message.
     */
    @Nonnull
    private String createDashboardCommitMessage() {
        BuildProperties props = BuildProperties.get();
        StringJoiner stringJoiner = new StringJoiner(COMMIT_MSG_DELIMITER);
        stringJoiner.add(props.getVersion());
        stringJoiner.add(props.getShortCommitId());
        stringJoiner.add(Boolean.toString(props.isDirty()));
        return stringJoiner.toString();
    }

    /**
     * Configuration for {@link Grafanon}. These are the externally-specified properties.
     */
    public static class GrafanonConfig {
        private String timescaleDisplayName = "Turbo Timescale";
        private final Supplier<DbEndpointConfig> postgresDatasourceEndpoint;
        private long onErrorSleepIntervalMs = 30_000;

        private String viewerUsername;
        private String viewerDisplayName;
        private String viewerPassword;

        GrafanonConfig(@Nonnull final Supplier<DbEndpointConfig> dbEndpoint) {
            this.postgresDatasourceEndpoint = dbEndpoint;
        }

        /**
         * Override the datasource display name.
         * @param timescaleDisplayName The new display name of the timescale datasource.
         * @return The config, for method chaining.
         */
        @Nonnull
        public GrafanonConfig setTimescaleDisplayName(String timescaleDisplayName) {
            this.timescaleDisplayName = timescaleDisplayName;
            return this;
        }

        /**
         * Set the report viewer username.
         *
         * @param username Report viewer username.
         * @return The config, for method chaining.
         */
        @Nonnull
        public GrafanonConfig setViewerUsername(String username) {
            this.viewerUsername = username;
            return this;
        }

        /**
         * Set the report viewer display name.
         *
         * @param displayName Report viewer display name.
         * @return The config, for method chaining.
         */
        @Nonnull
        public GrafanonConfig setViewerDisplayName(String displayName) {
            this.viewerDisplayName = displayName;
            return this;
        }

        /**
         * Set the report viewer password.
         *
         * @param password Report viewer password.
         * @return The config, for method chaining.
         */
        @Nonnull
        public GrafanonConfig setViewerPassword(String password) {
            this.viewerPassword = password;
            return this;
        }

        /**
         * Override the interval to sleep when refreshing the configuration fails.
         * We retry indefinitely until success(because Grafana may be down), waiting this long
         * between attempts.
         *
         * @param errorSleepInterval Sleep interval.
         * @param timeUnit Time unit for the interval.
         * @return The config, for method chaining.
         */
        @Nonnull
        public GrafanonConfig setErrorSleepInterval(long errorSleepInterval, TimeUnit timeUnit) {
            this.onErrorSleepIntervalMs = timeUnit.toMillis(errorSleepInterval);
            return this;
        }

        /**
         * Get the display name for the timescale datasource.
         *
         * @return The display name.
         */
        @Nonnull
        public String getTimescaleDisplayName() {
            return timescaleDisplayName;
        }

        /**
         * Get the timescale endpoint configuration.
         *
         * @return The timescale endpoint configuration supplier.
         */
        @Nonnull
        public Supplier<DbEndpointConfig> getDbEndpointConfig() {
            return postgresDatasourceEndpoint;
        }

        /**
         * Get the {@link UserInput} for the common "report viewer" user.
         *
         * @return The {@link UserInput} optional.
         */
        @Nonnull
        public Optional<UserInput> getViewerUserInput() {
            if (StringUtils.isEmpty(viewerDisplayName) || StringUtils.isEmpty(viewerUsername)) {
                return Optional.empty();
            } else {
                String password = viewerPassword;
                if (StringUtils.isEmpty(password)) {
                    // If there is no explicit password provided, create a random alpha-numeric
                    // password. We don't really care about this password, because we never log
                    // in with the "viewer" user directly, only through the reverse proxy.
                    password = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
                }
                return Optional.of(new UserInput(viewerDisplayName, viewerUsername, password));
            }
        }

        /**
         * Get the interval to sleep between refresh attempts, in ms.
         *
         * @return Ms between refresh attempts.
         */
        public long getErrorSleepIntervalMs() {
            return onErrorSleepIntervalMs;
        }
    }
}
