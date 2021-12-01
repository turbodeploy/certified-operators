package com.vmturbo.integrations.intersight.licensing;

import java.io.IOException;
import java.time.Instant;
import java.util.List;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;

import com.cisco.intersight.client.ApiException;
import com.cisco.intersight.client.model.LicenseLicenseInfo;
import com.fasterxml.jackson.databind.ObjectMapper;

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

    /**
     * The license moid that received the last published workload count.
     */
    public static final String LAST_PUBLISHED_WORKLOAD_COUNT_LICENSE_MOID_KEY = "licenseSync/lastPublishedWorkloadCountLicenseMoid";

    private boolean enabled;

    private final KeyValueStore kvStore;

    private final LicenseCheckClient licenseCheckClient;

    private final IntersightLicenseClient intersightLicenseClient;

    private final int licenseCountCheckIntervalSecs;

    private final IntersightLicenseSyncService licenseSyncService;

    // the "latest available" workload count info. This will be published to intersight when the
    // sync task runs.
    private WorkloadCountInfo latestWorkloadCountInfo;

    // tracks the last published workload count info. So we know we need to only publish newer material.
    @GuardedBy("this")
    private WorkloadCountInfo lastPublishedWorkloadCountInfo = new WorkloadCountInfo(0, null);

    // tracks the last license that we published a workload count to. For detecting license changes
    @GuardedBy("this")
    private String lastPublishedWorkloadLicenseMoid = null;

    /**
     * Construct an instance of IntersightLicenseCountUpdater.
     * @param enabled if false, the updater will be disabled and do nothing even after .start() is called.
     * @param kvStore kv store for tracking last publication information.
     * @param licenseCheckClient {@link LicenseCheckClient} used for monitoring license summary changes.
     * @param intersightLicenseClient {@link IntersightLicenseClient} for making calls to Intersight API
     * @param checkIntervalSecs the delay (in seconds) between workload count upload checks.
     * @param licenseSyncService the {@link IntersightLicenseSyncService} that is synchronizing the licenses
     */
    public IntersightLicenseCountUpdater(boolean enabled,
                                         @Nonnull final KeyValueStore kvStore,
                                         @Nonnull final LicenseCheckClient licenseCheckClient,
                                         @Nonnull final IntersightLicenseClient intersightLicenseClient,
                                        final int checkIntervalSecs,
                                         @Nonnull final IntersightLicenseSyncService licenseSyncService) {
        this.enabled = enabled;
        this.kvStore = kvStore;
        this.licenseCheckClient = licenseCheckClient;
        this.intersightLicenseClient = intersightLicenseClient;
        this.licenseCountCheckIntervalSecs = checkIntervalSecs;
        this.licenseSyncService = licenseSyncService;
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
        // Uncomment following to get update by Reactor, and remove onLicenseSummaryUpdated method
        // from syncLicenseCounts method.
        // start listening for workload count updates.
        //        licenseCheckClient.getUpdateEventStream()
        //                .doOnError(err -> logger.error("Error encountered while subscribing license summary update events: ", err))
        //                .retry()
        //                .subscribe(this::onLicenseSummaryUpdated);
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
                    syncLicenseCounts(true);
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
        lastPublishedWorkloadLicenseMoid = kvStore.get(LAST_PUBLISHED_WORKLOAD_COUNT_LICENSE_MOID_KEY)
                .orElse("");
        logger.info("Initial last published workload count {} generated at {} to license moid {}",
                lastPublishedCount, lastPublishedTimeSummaryGenerationTime, lastPublishedWorkloadLicenseMoid);
        lastPublishedWorkloadCountInfo = new WorkloadCountInfo(lastPublishedCount, lastPublishedTimeSummaryGenerationTime);
    }

    /**
     * Called when a new license summary is available. This method checks to see whether it contains
     * new workload count info that should be published, and queues it for publishing if so.
     * @param licenseSummary the license summary to evaluate.
     */
    protected synchronized void onLicenseSummaryUpdated(LicenseSummary licenseSummary) {
        logger.info("License summary ({} workloads) generated at {}", licenseSummary.getNumInUseEntities(),
                licenseSummary.getGenerationDate());
        // when a new license summary comes in, make sure it has both a workload count and generation
        // date. There's no sense mining it for workload count if either are not available. A usable
        // summary would have both set, even if there is no workload yet.
        if (licenseSummary.hasNumInUseEntities() && licenseSummary.hasGenerationDate()
                && StringUtils.isNotBlank(licenseSummary.getGenerationDate())) {
            // first check if the workload count has actually changed.
            if ((lastPublishedWorkloadCountInfo.getWorkloadCount() == licenseSummary.getNumInUseEntities())
                    && (latestWorkloadCountInfo != null)) {
                // no change -- skip the rest of processing.
                logger.info("Workload count hasn't changed. Will not update last workload count information.");
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
     *
     * <p>IMPORTANT: As agreed with Cisco, we are going to attribute the whole workload count to a single
     * license info object. This is because, unlike regular Turbo or CWOM licenses, we have no access
     * to a number of "available license counts" for any given license, and thus are in no position
     * to intelligently subdivide or distribute the workload count across multiple licenses.
     * As a tiebreaker, if we have multiple license, we will sync to the first item in our proxy
     * license list, which has been pre-sorted by sync priority.
     * @param loopForever flag to control loop for testing.
     * @throws InterruptedException if thread is interrupted.
     */
    protected void syncLicenseCounts(final boolean loopForever) throws InterruptedException {
        logger.info("Starting license count synchronization thread.");
        // we're just going to loop forever
        do {
            try {
                if (licenseCheckClient.isReady()) {
                    // check the latest available license summary
                    onLicenseSummaryUpdated(licenseCheckClient.geCurrentLicenseSummary());
                }
                List<LicenseLicenseInfo> targetLicenses = licenseSyncService.getAvailableIntersightLicenses();
                WorkloadCountInfo workloadCountInfo = latestWorkloadCountInfo;
                if (shouldUpdateWorkloadCounts(workloadCountInfo, targetLicenses)) {
                    logger.info("License count updater -- uploading VM counts.");
                    // we will iterate through the list of target licenses. The first item in the list is
                    // the "primary" license and will have all of the workload set as it's "license count".
                    // all of the others will be zeroed out.
                    // Discussed with Intersight license team, we will only update the "primary"license to avoid count overwritten
                    // the primary license will receive the actual workload count.
                        LicenseLicenseInfo license = targetLicenses.get(0);
                        try {
                            uploadWorkloadCount(license, workloadCountInfo.getWorkloadCount());
                        } catch (ApiException ae) {
                            // log some additional info specific to the Intersight API errors that may help
                            // debugging.
                            logger.error("API error while {} uploading workload count.",
                                    IntersightLicenseUtils.describeApiError(ae), ae);
                        } catch (Exception e) {
                            logger.error("Error while uploading workload count.", e);
                        }
                    // update the consul workload tracking values.
                    updateLastPublishedWorkloadCountInfo(workloadCountInfo, targetLicenses.get(0));
                }
            } catch (RuntimeException rte) {
                logger.error("Error running license count updates.", rte);
            }
            logger.debug("Sleeping for {} secs", licenseCountCheckIntervalSecs);
            if (loopForever) {
                Thread.sleep(licenseCountCheckIntervalSecs * 1000);
            }
        } while (loopForever);
    }

    /**
     * Utility method for determining if we should run a workload count update.
     * We should run a workload count update when we have a target intersight license to sync with,
     * and either of these is true:
     * <ul>
     *     <li>There is a "pending" WorkloadCountInfo to be uploaded that differs from the "last
     *     uploaded" one. We'll use a simple object reference check for this, as we only expect the
     *     references to change when there is a new summary available.</li>
     * </ul>
     * @param latestCountInfo latest count information.
     * @param currentTargetLicenses list of Intersight target license.
     * @return true, if the conditions above are true.
     */
    protected boolean shouldUpdateWorkloadCounts(final WorkloadCountInfo latestCountInfo,
            final List<LicenseLicenseInfo> currentTargetLicenses) {
        boolean hasWorkloadCount = (latestCountInfo != null && latestCountInfo.getGeneratedTime() != null);
        boolean hasTargetLicense = (currentTargetLicenses != null && currentTargetLicenses.size() > 0);
        logger.debug("has Workload Count?: {}", hasWorkloadCount);
        logger.debug("has Target License?: {}", hasTargetLicense);
        return hasTargetLicense && hasWorkloadCount;
    }

    /**
     * Update an Intersight license with a new license count.
     *
     * <p>This will be best-effort, but we will not go crazy with the retries. If we fail, then we'll
     * end up trying again on the next cycle anyways.
     *
     * @param licenseToUpdate The {@link LicenseLicenseInfo} to update.
     * @param newCount the new license count to set.
     */
    protected synchronized void uploadWorkloadCount(LicenseLicenseInfo licenseToUpdate, int newCount)
            throws IOException, ApiException {

        // make sure this is not a fallback license -- this means a true "IWO" license was not
        // found.
        if (IntersightProxyLicenseEdition.fromIntersightLicenseType(licenseToUpdate.getLicenseType())
                .equals(IntersightProxyLicenseEdition.IWO_FALLBACK)) {
            logger.info("LicenseCountUpdater: Fallback license in use, will not update license count");
            return;
        }

        logger.info("Updating license {} licenseCount to {}", licenseToUpdate.getMoid(), newCount);

        // publish the new count
        // we'll need to create and edited version of the LicenseInfo object with the new LicenseCount
        // assigned. Since this field is not directly editable, we will make our edit through json.
        // TODO: We *should* be allowed to use licenseInfo.setLicenseCount(x) method directly if/when
        // the IWO back end model is updated to mark this field as read-write instead of read-only.

        ObjectMapper jsonMapper = intersightLicenseClient.getApiClient().getJSON().getMapper();
        // The flow will be: LicenseLicenseInfo -> json -> json w/license count -> LicenseLicenseInfo w/license count
        JSONObject editableJson = new JSONObject();
        editableJson.put("ClassId", "license.LicenseInfo");
        editableJson.put("LicenseType", licenseToUpdate.getLicenseType().getValue());
        editableJson.put("LicenseState", licenseToUpdate.getLicenseState().getValue());
        editableJson.put("LicenseCount", newCount);
        String editedJson = editableJson.toString();
        LicenseLicenseInfo editedLicenseInfo = jsonMapper.readValue(editedJson, LicenseLicenseInfo.class);

        LicenseLicenseInfo responseInfo = intersightLicenseClient.updateLicenseLicenseInfo(licenseToUpdate.getMoid(), editedLicenseInfo);
    }

    /**
     * Update the last published workload count info in consul. This will be read back when the service
     * starts up and used to avoid publishing either stale or duplicate data after the restart.
     * @param workloadCountInfo the workload count info to save.
     */
    protected synchronized void updateLastPublishedWorkloadCountInfo(WorkloadCountInfo workloadCountInfo,
                                                                     LicenseLicenseInfo targetLicense) {
        kvStore.put(LAST_PUBLISHED_WORKLOAD_TIMESTAMP_KEY, workloadCountInfo.getGeneratedTimeString());
        kvStore.put(LAST_PUBLISHED_WORKLOAD_COUNT_KEY, String.valueOf(workloadCountInfo.getWorkloadCount()));
        kvStore.put(LAST_PUBLISHED_WORKLOAD_COUNT_LICENSE_MOID_KEY, targetLicense.getMoid());
        lastPublishedWorkloadCountInfo = workloadCountInfo;
        lastPublishedWorkloadLicenseMoid = targetLicense.getMoid();
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

        /**
         * Utility method to get the generated time as a string, if a timestamp is present.
         * @return The timestamp string, if present, or null if not.
         */
        public String getGeneratedTimeString() {
            if (generatedTime == null) {
                return null;
            }
            return generatedTime.toString();
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
