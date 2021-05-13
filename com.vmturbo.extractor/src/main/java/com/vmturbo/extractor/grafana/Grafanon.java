package com.vmturbo.extractor.grafana;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.util.Map;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Record;
import org.jooq.Record1;
import org.jooq.Result;
import org.jooq.ResultQuery;
import org.jooq.impl.DSL;

import com.vmturbo.auth.api.licensing.LicenseCheckClient;
import com.vmturbo.common.protobuf.LicenseProtoUtil;
import com.vmturbo.common.protobuf.licensing.Licensing.LicenseSummary;
import com.vmturbo.components.common.RequiresDataInitialization;
import com.vmturbo.components.common.utils.BuildProperties;
import com.vmturbo.extractor.ExtractorGlobalConfig.ExtractorFeatureFlags;
import com.vmturbo.extractor.grafana.client.GrafanaClient;
import com.vmturbo.extractor.grafana.model.DashboardSpec;
import com.vmturbo.extractor.grafana.model.DashboardSpec.UpsertDashboardRequest;
import com.vmturbo.extractor.grafana.model.DashboardVersion;
import com.vmturbo.extractor.grafana.model.DatasourceInput;
import com.vmturbo.extractor.grafana.model.Folder;
import com.vmturbo.extractor.grafana.model.FolderInput;
import com.vmturbo.extractor.grafana.model.Role;
import com.vmturbo.extractor.grafana.model.UserInput;
import com.vmturbo.extractor.schema.tables.EntityOld;
import com.vmturbo.extractor.schema.tables.records.EntityOldRecord;
import com.vmturbo.sql.utils.DbEndpoint;
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

    @VisibleForTesting
    static final String REPORTS_V1_FOLDER_UUID = "reports_v1";

    private static final Logger logger = LogManager.getLogger();

    private final GrafanonConfig grafanonConfig;

    private final GrafanaClient grafanaClient;

    private final DashboardsOnDisk dashboardsOnDisk;

    private final ExtractorFeatureFlags extractorFeatureFlags;

    protected final DbEndpoint dbEndpoint;

    private final LicenseCheckClient licenseCheckClient;

    private final CompletableFuture<RefreshSummary> initializationResult = new CompletableFuture<>();

    Grafanon(@Nonnull final GrafanonConfig grafanonConfig,
            @Nonnull final DashboardsOnDisk dashboardsOnDisk,
            @Nonnull final GrafanaClient grafanaClient,
            @Nonnull final ExtractorFeatureFlags extractorFeatureFlags,
            @Nonnull final DbEndpoint dbEndpoint,
            @Nonnull final LicenseCheckClient licenseCheckClient) {
        this.grafanonConfig = grafanonConfig;
        this.dashboardsOnDisk = dashboardsOnDisk;
        this.grafanaClient = grafanaClient;
        this.extractorFeatureFlags = extractorFeatureFlags;
        this.dbEndpoint = dbEndpoint;
        this.licenseCheckClient = licenseCheckClient;
        licenseCheckClient.getUpdateEventStream().subscribe(licenseSummary -> {
            // Any exception must be caught to prevent the subscription from terminating
            try {
                RefreshSummary refreshSummary = new RefreshSummary();
                refreshTurboEditors(refreshSummary, licenseSummary);
                logger.info("Turbo editor refresh result: {}", refreshSummary);
            } catch (Exception e) {
                logger.error("Unable to update turbo editors", e);
            }
        });
    }

    @Nonnull
    CompletableFuture<RefreshSummary> getInitializationFuture() {
        return initializationResult;
    }

    @Override
    public void initialize() {
        // do not initialize if reporting is not enabled
        if (!extractorFeatureFlags.isReportingEnabled()) {
            return;
        }
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
                    initializationResult.complete(refreshSummary);
                } catch (RuntimeException e) {
                    logger.error("Failed to initialize some, or all, of the Grafana setup. Error:", e);
                    refreshSummary.recordError(e);
                    logger.info(refreshSummary.summarize(logger.isDebugEnabled()));
                    try {
                        Thread.sleep(grafanonConfig.getErrorSleepIntervalMs());
                    } catch (InterruptedException interruptedException) {
                        logger.error("Grafana initialization thread interrupted." + " Exiting without completing initialization.",
                                interruptedException);
                        initializationResult.completeExceptionally(interruptedException);
                        break;
                    }
                }
            }
        }, "grafanon-initialization").start();
    }

    /**
     * Returns stringified timestamp of migration V1.14 execution time.
     * @return timestamp of migration V1.14
     */
    @Nonnull
    @VisibleForTesting
    String getMigrationV14TimeStamp() {
        ResultQuery<Record1<Timestamp>> query = DSL.select(DSL.field("installed_on", Timestamp.class))
                .from(DSL.table("schema_version"))
                .where(DSL.field("version", String.class).eq("1.14"))
                .limit(1);
        Optional<Result<Record1<Timestamp>>> results = executeRawQuery(query);
        if (results.isPresent()) {
            //We have limited the query to only return 1 result with 1 column
            Timestamp timestamp = (Timestamp)results.get().get(0).get(0);
            return DateFormat.getDateTimeInstance().format(timestamp);
        } else {
            logger.info("No results for extractor migration V1.14 found");
        }
        return "";
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
        refreshTurboEditors(refreshSummary, licenseCheckClient.geCurrentLicenseSummary());

        try {
            // Get the endpoint here (inside the initialization thread) to avoid blocking the
            // main thread.
            final DbEndpointConfig endpointConfig = grafanonConfig.getDbEndpointConfig().get();
            final DatasourceInput input = DatasourceInput.fromDbEndpoint(grafanonConfig.getTimescaleDisplayName(), endpointConfig);
            grafanaClient.upsertDefaultDatasource(input, refreshSummary);
        } catch (UnsupportedDialectException e) {
            throw new IllegalArgumentException("Invalid endpoint configuration.", e);
        }
        final String v14TimestampMigration = getMigrationV14TimeStamp();
        final Map<String, Long> existingDashboardsByUid = grafanaClient.dashboardIdsByUid();
        final Map<String, Folder> existingFolders = grafanaClient.foldersByUid();
        dashboardsOnDisk.visit((folderData) -> {
            Optional<FolderInput> folderInputOptional = folderData.getFolderSpec();
            if (skipFolder(folderInputOptional)) {
                folderInputOptional.ifPresent(folderInput ->
                          logger.info("Skipping folder uuid: {}", folderInput.getUid()));
                return;
            }
            Optional<Folder> folder = folderInputOptional.map(folderInput -> {
                addTimestampToReportsV1FolderTitle(folderInput, v14TimestampMigration);
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

    private void refreshTurboEditors(@Nonnull RefreshSummary refreshSummary, @Nullable LicenseSummary licenseSummary) {
        if (!grafanonConfig.validEditorConfig()) {
            return;
        }

        final int requiredNumberOfReportEditors = LicenseProtoUtil.numberOfSupportedReportEditors(licenseSummary);
        final String reportEditorPrefix = grafanonConfig.getEditorUsernamePrefix();
        final String reportEditorDisplayName = grafanonConfig.getEditorDisplayName();
        for (int i = 0; i < requiredNumberOfReportEditors; i++) {
            String reportEditorUsername = LicenseProtoUtil.formatReportEditorUsername(reportEditorPrefix, i);
            String password = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
            UserInput newReportEditor = new UserInput(reportEditorDisplayName, reportEditorUsername, password);

            grafanaClient.ensureUserExists(newReportEditor, Role.ADMIN, refreshSummary);
        }

        grafanaClient.ensureReportEditorsAreAdmin(reportEditorPrefix, refreshSummary);
    }

    /**
     * Tests folderInput to see if desired behavior is to ignore uploading folder and dashboards.
     * @param folderInputOptional folder of focus
     * @return true if folder generation should be skipped
     */
    @VisibleForTesting
    boolean skipFolder(Optional<FolderInput> folderInputOptional) {
        if (!folderInputOptional.isPresent()) {
            return false;
        }
        return skipFolderV1(folderInputOptional.get());
    }

    /**
     * Returns true if no data in entity_old table.
     *
     * <p>We will not have data in entity_old table on fresh installs and won't
     * require the old reports folder to be generated.</p>
     * @param folderInput FolderInput of focus
     * @return true if folder should be skipped
     */
    private boolean skipFolderV1(FolderInput folderInput) {
        if (!folderInput.getUid().equals(REPORTS_V1_FOLDER_UUID)) {
            return false;
        }
        ResultQuery<EntityOldRecord> query = DSL.selectFrom(EntityOld.ENTITY_OLD).limit(1);
        Optional<Result<EntityOldRecord>> results = executeRawQuery(query);
        return !results.isPresent();
    }

    /**
     * Executes sql query.
     *
     * @param query to execute.
     * @param <R> type of return record
     * @return results if query returned any else, empty optional
     */
    private <R extends Record> Optional<Result<R>> executeRawQuery(ResultQuery<R> query) {
        try {
            Result<R> results = dbEndpoint.dslContext().fetch(query);
            return results.isEmpty() ? Optional.empty() : Optional.of(results);
        } catch (UnsupportedDialectException | SQLException e) {
            logger.error("Error executing query {}", query.getSQL(), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("Error executing query {}", query.getSQL(), e);
        }
        return Optional.empty();
    }

    /**
     * Adds timestamp on title for grafana folder 'reports_v1'.
     *
     * @param folderInput current folder data
     * @param migrationTimestamp timestamp to note in reports title
     */
    @VisibleForTesting
    void addTimestampToReportsV1FolderTitle(@Nonnull final FolderInput folderInput, String migrationTimestamp) {
        if (folderInput.getUid().equals(REPORTS_V1_FOLDER_UUID)) {
            String title = folderInput.getTitle();
            String formattedTitle = String.format(title, migrationTimestamp);
            folderInput.setTitle(formattedTitle);
        }
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

        private String editorUsername;
        private String editorDisplayName;
        private String editorPassword;

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
        public GrafanonConfig setEditorUsername(String username) {
            this.editorUsername = username;
            return this;
        }

        /**
         * Set the report viewer display name.
         *
         * @param displayName Report viewer display name.
         * @return The config, for method chaining.
         */
        @Nonnull
        public GrafanonConfig setEditorDisplayName(String displayName) {
            this.editorDisplayName = displayName;
            return this;
        }

        /**
         * Set the report viewer password.
         *
         * @param password Report viewer password.
         * @return The config, for method chaining.
         */
        @Nonnull
        public GrafanonConfig setEditorPassword(String password) {
            this.editorPassword = password;
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
         * Return whether or not the editor user configuration properties are set correctly.
         *
         * @return True or false.
         */
        public boolean validEditorConfig() {
            return !(StringUtils.isEmpty(editorDisplayName) || StringUtils.isEmpty(editorUsername));
        }

        public String getEditorUsernamePrefix() {
            return editorUsername;
        }

        public String getEditorDisplayName() {
            return editorDisplayName;
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
