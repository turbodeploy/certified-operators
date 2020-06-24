package com.vmturbo.integrations.intersight.licensing;

import java.io.IOException;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;

import com.cisco.intersight.client.ApiException;
import com.cisco.intersight.client.JSON;
import com.cisco.intersight.client.model.LicenseLicenseInfo;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

import com.vmturbo.auth.api.licensing.LicenseCheckClient;
import com.vmturbo.common.protobuf.licensing.Licensing.LicenseSummary;
import com.vmturbo.kvstore.KeyValueStore;

/**
 * Monitors the workload count and publishes updates to the Intersight license servers.
 *
 * <p>We will get the workload count via the license summary updates.
 */
public class IntersightLicenseCountUpdater {
    private static final Logger logger = LogManager.getLogger();

    /**
     * the timestamp of the last workload count that was published. This is the generatedDate of the
     * source LicenseSummary message. We will track this in consul so that in the event of a pod
     * restart, we won't publish stale data.
     */
    public static final String LAST_PUBLISHED_WORKLOAD_TIMESTAMP_KEY = "licenseSync/lastPublishedWorkloadTimestamp";

    /**
     * The actual workload count value that was last pushed to the intersight service.
     */
    public static final String LAST_PUBLISHED_WORKLOAD_COUNT_KEY = "licenseSync/lastPublishedWorkloadCount";

    private boolean enabled;

    private final KeyValueStore kvStore;

    private final LicenseCheckClient licenseCheckClient;

    private final IntersightLicenseClient intersightLicenseClient;

    private final int licenseCountCheckIntervalSecs;

    // the "latest available" workload count info. This will be published to intersight when the
    // sync task runs.
    private WorkloadCountInfo latestWorkloadCountInfo = new WorkloadCountInfo(0, null);

    // tracks the last published workload count info. So we know we need to only publish newer material.
    @GuardedBy("this")
    private WorkloadCountInfo lastPublishedWorkloadCountInfo = new WorkloadCountInfo(0, null);

    /**
     * Construct an instance of IntersightLicenseCountUpdater.
     * @param enabled if false, the updater will be disabled and do nothing even after .start() is called.
     * @param kvStore kv store for tracking last publication information.
     * @param licenseCheckClient {@link LicenseCheckClient} used for monitoring license summary changes.
     * @param intersightLicenseClient {@link IntersightLicenseClient} for making calls to Intersight API
     * @param checkIntervalSecs the delay (in seconds) between workload count upload checks.
     */
    public IntersightLicenseCountUpdater(boolean enabled,
                                         @Nonnull final KeyValueStore kvStore,
                                         @Nonnull final LicenseCheckClient licenseCheckClient,
                                         @Nonnull final IntersightLicenseClient intersightLicenseClient,
                                        final int checkIntervalSecs) {
        this.enabled = enabled;
        this.kvStore = kvStore;
        this.licenseCheckClient = licenseCheckClient;
        this.intersightLicenseClient = intersightLicenseClient;
        this.licenseCountCheckIntervalSecs = checkIntervalSecs;
    }

    /**
     * Initialize the license count updater.
     */
    public void start() {
        if (!enabled) {
            logger.info("License Count Updater is not enabled. Will not start.");
            return;
        }
        if (licenseCheckClient.isReady()) {
            // check the latest available license summary
            onLicenseSummaryUpdated(licenseCheckClient.geCurrentLicenseSummary());
        }
        // start listening for workload count updates.
        licenseCheckClient.getUpdateEventStream().subscribe(this::onLicenseSummaryUpdated);
        initLastPublishedWorkloadCountInfo();

        // start the background publication thread. This is separated from the license summary processing
        // threads because the requests to the intersight API are using their java client jar, which
        // may have unpredictable blocking behavior. Keeping the publication and workload counting
        // threads separate will ensure we can continue to monitor workload count regardless of the
        // status of the publication thread, which will have variable performance and reliability
        // and be subject to configuration errors.
        Runnable publisherTask = new Runnable() {
            @Override
            public void run() {
                try {
                    syncLicenseCounts();
                } catch (InterruptedException ie) {
                    // propagate
                    Thread.currentThread().interrupt();
                }
            }
        };

        Thread licenseCountPublisherThread = new Thread(publisherTask, "license-count-publisher");
        licenseCountPublisherThread.setDaemon(true);
        licenseCountPublisherThread.start();
    }

    /**
     * Retrieve our last published workload count from consul.
     */
    protected synchronized void initLastPublishedWorkloadCountInfo() {
        int lastPublishedCount = kvStore.get(LAST_PUBLISHED_WORKLOAD_COUNT_KEY)
                .map(Integer::valueOf)
                .orElse(0);
        Instant lastPublishedTimeSummaryGenerationTime = kvStore.get(LAST_PUBLISHED_WORKLOAD_TIMESTAMP_KEY)
                .map(Instant::parse)
                .orElse(null);
        logger.info("Initial last published workload count {} generated at {}", lastPublishedCount, lastPublishedTimeSummaryGenerationTime);
        lastPublishedWorkloadCountInfo = new WorkloadCountInfo(lastPublishedCount, lastPublishedTimeSummaryGenerationTime);
    }

    /**
     * Called when a new license summary is available. This method checks to see whether it contains
     * new workload count info that should be published, and queues it for publishing if so.
     * @param licenseSummary the license summary to evaluate.
     */
    protected synchronized void onLicenseSummaryUpdated(LicenseSummary licenseSummary) {
        logger.info("Checking new license summary ({} workloads) generated at {}", licenseSummary.getNumInUseEntities(),
                licenseSummary.getGenerationDate());
        // when a new license summary comes in, make sure it has both a workload count and generation
        // date. There's no sense mining it for workload count if either are not available. A usable
        // summary would have both set, even if there is no workload yet.
        if (licenseSummary.hasNumInUseEntities() && licenseSummary.hasGenerationDate()
                && StringUtils.isNotBlank(licenseSummary.getGenerationDate())) {
            // first check if the workload count has actually changed.
            if (lastPublishedWorkloadCountInfo.getWorkloadCount() == licenseSummary.getNumInUseEntities()) {
                // no change -- skip the rest of processing.
                logger.info("Workload count hasn't changed. Will not update intersight.");
                return;
            }

            // next, make sure the summary is newer than our last published one, and if so, update
            // the latest available workload count info. The background thread will take care of
            // getting this to Intersight.
            Instant generationTime = Instant.parse(licenseSummary.getGenerationDate());
            if (lastPublishedWorkloadCountInfo.wasGeneratedBefore(generationTime)) {
                logger.info("Updating latest workload count info to {} workloads generated at {}", licenseSummary.getNumInUseEntities(),
                        licenseSummary.getGenerationDate());
                latestWorkloadCountInfo = new WorkloadCountInfo(licenseSummary.getNumInUseEntities(), generationTime);
            }
        } else {
            // log for posterity and troubleshooting.
            logger.info("Skipping license summary with: {} {}",
                    (licenseSummary.hasGenerationDate() && StringUtils.isNotBlank(licenseSummary.getGenerationDate()))
                            ? "" : "empty generation date",
                    licenseSummary.hasNumInUseEntities() ? "" : "empty workload count");
        }
    }

    /**
     * This method will run on a background thread, and is responsible for updating the Intersight
     * server with the latest available workload count information we discover in XL.
     */
    protected void syncLicenseCounts() throws InterruptedException {
        logger.info("Starting license count synchronization thread.");
        // we're just going to loop forever
        while (true) {
            // if there is a "pending" WorkloadCountInfo to be uploaded that differs from the
            // "last uploaded" one, then upload it. We'll use a simple object reference check for
            // this, as we only expect the references to change when there is a new summary available.
            if (latestWorkloadCountInfo != null
                    && (latestWorkloadCountInfo != lastPublishedWorkloadCountInfo)) {
                // there's a new workload count to publish
                logger.info("New workload count info generated at {} found -- will upload.", latestWorkloadCountInfo.getGeneratedTime());
                try {
                    uploadWorkloadCount(latestWorkloadCountInfo);
                } catch (Exception e) {
                    logger.error("Error while uploading workload count.", e);
                }
            }

            logger.info("Sleeping for {} secs", licenseCountCheckIntervalSecs);
            Thread.sleep(licenseCountCheckIntervalSecs * 1000);
        }
    }

    /**
     * Upload the latest available workload count to Intersight.
     *
     * <p>This will be best-effort, but we will not go crazy with the retries. If we fail, then we'll
     * end up trying again on the next cycle anyways.
     *
     * @param workloadCountInfo The {@link WorkloadCountInfo} to upload.
     */
    protected synchronized void uploadWorkloadCount(WorkloadCountInfo workloadCountInfo)
            throws IOException, ApiException {
        // get the license list
        List<LicenseLicenseInfo> intersightLicenses = intersightLicenseClient.getIntersightLicenses().getResults();
        // get the first license moid.
        if (intersightLicenses.size() == 0) {
            logger.warn("No intersight licenses found. Will not publish new workload count.");
            return;
        }
        // IMPORTANT: As agreed with Cisco, we are only going to publish license count info to a
        // single license info object!! This is because we have no access to the number of "available
        // license counts" for any given license, and thus are in no position to intelligently
        // subdivide or distribute the workload count across multiple licenses.
        // So, if we have multiple licenses, we will sort them by moid and post license counts to
        // the license with the lowest moid in the list.
        intersightLicenses.sort(Comparator.comparing(LicenseLicenseInfo::getMoid));
        LicenseLicenseInfo licenseToUpdate = intersightLicenses.get(0);
        // get the "parent" account license data moid -- we will need to monitor this object later
        // to know when the workload count update was finished.
        if (licenseToUpdate.getParent() == null) {
            logger.error("LicenseLicenseInfo has no parent account -- will not update license count.");
            return;
        }
        String accountLicenseDataMoid = licenseToUpdate.getParent().getMoid();
        if (StringUtils.isBlank(accountLicenseDataMoid)) {
            logger.error("AccountLicenseData.moid is empty -- cannot update license count.");
            return;
        }

        // publish the new count
        // we'll need to create and edited version of the LicenseInfo object with the new LicenseCount
        // assigned. Since this field is not directly editable, we will make our edit through json.
        // TODO: We *should* be allowed to use licenseInfo.setLicenseCount(x) method directly if/when
        // the IWO back end model is updated to mark this field as read-write instead of read-only.
        JSON json = intersightLicenseClient.getApiClient().getJSON();
        // The flow will be: LicenseLicenseInfo -> json -> json w/license count -> LicenseLicenseInfo w/license count
        String originalJson = json.getMapper().writeValueAsString(licenseToUpdate);
        JSONObject editableJson = new JSONObject(originalJson);
        editableJson.put("LicenseCount", workloadCountInfo.workloadCount);
        String editedJson = editableJson.toString();
        LicenseLicenseInfo editedLicenseInfo = json.getMapper().readValue(editedJson,
                LicenseLicenseInfo.class);

        LicenseLicenseInfo responseInfo = intersightLicenseClient.updateLicenseLicenseInfo(licenseToUpdate.getMoid(), editedLicenseInfo);
        logger.info("Response license: moid {} license count {}}", responseInfo.getMoid(), responseInfo.getLicenseCount());
        // monitor the account license data SyncStatus
        // TBD: will add after the initial license count update is working.

        // update the consul workload tracking values.
        updateLastPublishedWorkloadCountInfo(workloadCountInfo);

    }

    /**
     * Update the last published workload count info in consul. This will be read back when the service
     * starts up and used to avoid publishing either stale or duplicate data after the restart.
     * @param workloadCountInfo the workload count info to save.
     */
    protected synchronized void updateLastPublishedWorkloadCountInfo(WorkloadCountInfo workloadCountInfo) {
        kvStore.put(LAST_PUBLISHED_WORKLOAD_TIMESTAMP_KEY, workloadCountInfo.generatedTime.toString());
        kvStore.put(LAST_PUBLISHED_WORKLOAD_COUNT_KEY, String.valueOf(workloadCountInfo.getWorkloadCount()));
        lastPublishedWorkloadCountInfo = workloadCountInfo;
    }

    /**
     * Simple class that just holds workload count information.
     */
    public static class WorkloadCountInfo {
        private int workloadCount;
        // the time at which the workload count was generated
        private Instant generatedTime;

        /**
         * Constructor.
         * @param count workload count
         * @param timestamp time the workload count was generated.
         */
        public WorkloadCountInfo(int count, Instant timestamp) {
            this.workloadCount = count;
            this.generatedTime = timestamp;
        }

        public int getWorkloadCount() {
            return workloadCount;
        }

        public Instant getGeneratedTime() {
            return generatedTime;
        }

        public boolean wasGeneratedBefore(Instant instant) {
            if (instant == null) {
                return false;
            }
            // if we have no generation time recorded then any instant is considered newer than this.
            if (generatedTime == null) {
                return true;
            }
            // otherwise, compare instants.
            return generatedTime.isBefore(instant);
        }
    }
}
