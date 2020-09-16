package com.vmturbo.integrations.intersight.licensing;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.cisco.intersight.client.ApiException;
import com.cisco.intersight.client.model.LicenseLicenseInfo;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.dto.license.ILicense;
import com.vmturbo.auth.api.auditing.AuditAction;
import com.vmturbo.auth.api.auditing.AuditLog;
import com.vmturbo.common.protobuf.licensing.LicenseManagerServiceGrpc.LicenseManagerServiceBlockingStub;
import com.vmturbo.common.protobuf.licensing.Licensing.AddLicensesRequest;
import com.vmturbo.common.protobuf.licensing.Licensing.AddLicensesResponse;
import com.vmturbo.common.protobuf.licensing.Licensing.GetLicensesRequest;
import com.vmturbo.common.protobuf.licensing.Licensing.GetLicensesResponse;
import com.vmturbo.common.protobuf.licensing.Licensing.LicenseDTO;
import com.vmturbo.common.protobuf.licensing.Licensing.RemoveLicenseRequest;
import com.vmturbo.common.protobuf.licensing.Licensing.RemoveLicenseResponse;
import com.vmturbo.commons.Pair;
import com.vmturbo.components.api.RetriableOperation;
import com.vmturbo.components.api.RetriableOperation.ConfigurableBackoffStrategy;
import com.vmturbo.components.api.RetriableOperation.RetriableOperationFailedException;
import com.vmturbo.integrations.intersight.licensing.IntersightLicenseUtils.BestAvailableIntersightLicenseComparator;

/**
 * Synchronizes license information with the Intersight upstream services.
 */
public class IntersightLicenseSyncService {
    private static final Logger logger = LogManager.getLogger();

    private static final LicenseDTO FALLBACK_LICENSE = IntersightLicenseUtils.createProxyLicense(
            "FALLBACK", ILicense.PERM_LIC, IntersightProxyLicenseEdition.IWO_FALLBACK
    );

    private boolean enabled;

    private final boolean useFallbackLicense;

    private final IntersightLicenseClient intersightLicenseClient;

    private final LicenseManagerServiceBlockingStub licenseManagerClient;

    private final int licenseSyncInitialCheckSecs;

    private final int licenseSyncIntervalSecs;

    private final int licensePostSyncIntervalSecs;

    private final int licenseSyncRetrySeconds;

    // the list of intersight licenses we are proxying
    private volatile List<LicenseLicenseInfo> availableIntersightLicenses;

    /**
     * Construct an IntersightLicenseSyncService.
     *
     * @param enabled if false, then the service will be disabled.
     * @param intersightLicenseClient intersight license client instance for making intersight api calls
     * @param licenseSyncInitialCheckSecs delay (in seconds) of initial license sync check
     * @param licenseSyncIntervalSecs delay (in secs) between license sync checks when we have not
     *                                detected a succesful sync yet
     * @param licensePostSyncIntervalSecs delay (in secs) between license sync checks AFTER
     *                                    a successful sync check
     * @param licenseSyncRetrySeconds max time allowed (in secs) to complete a license sync, including
     *                                retries.
     * @param licenseManagerClient a grpc client for accessing the LicenseManagerService.
     */
    public IntersightLicenseSyncService(boolean enabled, boolean intersightLicenseSyncUseFallbackLicense,
                                        @Nonnull final IntersightLicenseClient intersightLicenseClient,
                                        int licenseSyncInitialCheckSecs, int licenseSyncIntervalSecs,
                                        int licensePostSyncIntervalSecs, int licenseSyncRetrySeconds,
                                        @Nonnull LicenseManagerServiceBlockingStub licenseManagerClient) {
        this.enabled = enabled;
        this.useFallbackLicense = intersightLicenseSyncUseFallbackLicense;
        this.intersightLicenseClient = intersightLicenseClient;
        this.licenseSyncIntervalSecs = licenseSyncIntervalSecs;
        this.licensePostSyncIntervalSecs = licensePostSyncIntervalSecs;
        this.licenseSyncInitialCheckSecs = licenseSyncInitialCheckSecs;
        this.licenseSyncRetrySeconds = licenseSyncRetrySeconds;
        this.licenseManagerClient = licenseManagerClient;
    }

    /**
     * Start the synchronization routines.
     */
    public void start() {
        if (!enabled) {
            logger.info("Intersight License synchronization is disabled. Will not start.");
            // generate audit log entry
            AuditLog.newEntry(AuditAction.ENABLE_EXTERNAL_LICENSE_SYNC, "Intersight license sync service will be disabled.", false)
                    .targetName("Intersight License Sync Service")
                    .audit();
            return;
        }
        logger.info("Starting the Intersight License Synchronization service.");
        // generate audit log entry
        AuditLog.newEntry(AuditAction.ENABLE_EXTERNAL_LICENSE_SYNC, "Intersight license sync service will be enabled.", true)
                .targetName("Intersight License Sync Service")
                .audit();

        // create a retryable operation for the license sync.
        RetriableOperation<Boolean> licenseSyncOperation = RetriableOperation.newOperation(this::syncIntersightLicenses)
                .retryOnException(e -> e instanceof RetriableIntersightException)
                .backoffStrategy(ConfigurableBackoffStrategy.newBuilder()
                        .withMaxDelay(licenseSyncIntervalSecs * 1000)
                        .build());

        Runnable publisherTask = new Runnable() {
            @Override
            public void run() {
                logger.info("Starting license sync thread w/{} sec delay.", licenseSyncInitialCheckSecs);
                try {
                    Thread.sleep(licenseSyncInitialCheckSecs * 1000);
                    while(true) {
                        Optional<Boolean> syncResult = IntersightLicenseUtils.runRetriableOperation("License Sync",
                                licenseSyncOperation, licenseSyncRetrySeconds);
                        // if sync successfully, use the lazy delay
                        if (syncResult.isPresent() && syncResult.get()) {
                            logger.info("Sync status: an active license was successfully proxied. Will wait {} secs before next check.", licensePostSyncIntervalSecs);
                            Thread.sleep(licensePostSyncIntervalSecs * 1000);
                        } else {
                            logger.info("Sync status: no active license found. Will wait {} secs before next check.", licenseSyncIntervalSecs);
                            Thread.sleep(licenseSyncIntervalSecs * 1000);
                        }
                    }
                } catch (InterruptedException ie) {
                    // propagate
                    Thread.currentThread().interrupt();
                }
            }
        };

        Thread licenseSyncThread = new Thread(publisherTask, "intersight-license-sync-service");
        licenseSyncThread.setDaemon(true);
        licenseSyncThread.start();
    }

    /**
     * Get the last-fetched list of available Intersight licenses. These are sorted by priority, with the first
     * item in the list considered the "best available" license. These licenses may not be
     * active, so status should be checked, depending on the use case.
     * @return the list of proxied intersight licenses.
     */
    public List<LicenseLicenseInfo> getAvailableIntersightLicenses() {
        return availableIntersightLicenses;
    }

    /**
     * Synchronize the Intersight license info. This is in the direction of Intersight -> Local
     * XL Instance. We are polling the Intersight API for now, but hopefully in the future we can
     * get a push from Intersight -> XL instead.
     * @return true if the sync successfully installed a non-fallback proxy license, false otherwise.
     * @throws RetriableOperationFailedException if a retriable exception is caught within the method.
     */
    public Boolean syncIntersightLicenses() throws RetriableOperationFailedException {

        // retrieve the list of licenses from Intersight smart licensing
        logger.debug("Fetching intersight licenses...");
        final List<LicenseLicenseInfo> intersightLicenses;
        try {
            intersightLicenses = intersightLicenseClient.getIntersightLicenses();
        } catch (ApiException | IOException e) {
            if (IntersightLicenseClient.isRetryable(e)) {
                throw new RetriableIntersightException(e);
            } else {
                throw new RetriableOperation.RetriableOperationFailedException(e);
            }
        }
        if (intersightLicenses.size() > 0) {
            // log some info. This would normally be debug output, but we may not get much to work with
            // when debugging issues from the intersight environment, so let's err on the side of
            // TMI until we know things are working reliably.
            StringBuilder sb = new StringBuilder("Found licenses: ");
            for (LicenseLicenseInfo licenseInfo : intersightLicenses) {
                // we will only consider syncing "active" licenses -- either ActiveAdmin or TrialAdmin
                // should be set
                sb.append("moid ").append(licenseInfo.getMoid());
                if (licenseInfo.getActiveAdmin()) {
                    sb.append("(admin-active)");
                }
                if (licenseInfo.getTrialAdmin()) {
                    sb.append("(trial-active)");
                }
                sb.append(" ").append(licenseInfo.getLicenseType().toString());
                sb.append(" ").append(licenseInfo.getLicenseState().name());
                sb.append(" count:").append(licenseInfo.getLicenseCount());
                sb.append(" with ").append(licenseInfo.getDaysLeft()).append(" days left. ");
            }
            logger.debug(sb.toString());
        }
        // build a list of the licenses we want to create proxy licenses for
        final List<LicenseLicenseInfo> targetedIntersightLicenses = new ArrayList<>();
        // sort the intersight licenses by "best available" so we can find the best ones.
        intersightLicenses.sort(new BestAvailableIntersightLicenseComparator());
        if (intersightLicenses.size() > 0) {
            LicenseLicenseInfo bestAvailableLicense = intersightLicenses.get(0);
            // we will target this license, as long as it's active
            if (IntersightLicenseUtils.isActiveIntersightLicense(bestAvailableLicense)) {
                targetedIntersightLicenses.add(bestAvailableLicense);
            }
        }

        // convert any targeted licenses to proxy licenses
        final List<LicenseDTO> proxyLicenses = new ArrayList<>();
        for (LicenseLicenseInfo targetedLicense : targetedIntersightLicenses) {
            proxyLicenses.add(IntersightLicenseUtils.toProxyLicense(targetedLicense));
        }

        // if "fallback license" is enabled and there are no discovered viable intersight licenses,
        // we will install the fallback license.
        if (proxyLicenses.size() == 0 && useFallbackLicense) {
            logger.debug("No active intersight license found. Using fallback license.");
            proxyLicenses.add(FALLBACK_LICENSE);
        }

        // retrieve the list of licenses we have locally installed
        logger.debug("Fetching local licenses...");
        final List<LicenseDTO> localLicenses = getLocalLicenses(licenseManagerClient);

        // get the set of changes we'd need to make to get in sync with the intersight licenses.
        logger.debug("Synchronizing {} local licenses with {} targetted intersight licenses...",
                localLicenses.size(), proxyLicenses.size());
        Pair<List<LicenseDTO>, List<LicenseDTO>> edits = discoverLicenseEdits(localLicenses, proxyLicenses);

        // delete any licenses that need to be deleted.
        List<LicenseDTO> licensesToDelete = edits.second;
        if (licensesToDelete.size() > 0) {
            logger.info("Removing {} intersight proxy licenses.", licensesToDelete.size());
            for (LicenseDTO licenseToRemove : licensesToDelete) {
                logger.info("    Removing license {} with moid {}", licenseToRemove.getUuid(),
                        licenseToRemove.getTurbo().getExternalLicenseKey());
                RemoveLicenseResponse response = licenseManagerClient.removeLicense(RemoveLicenseRequest.newBuilder()
                        .setUuid(licenseToRemove.getUuid())
                        .build());
                if (response.hasWasRemoved()) {
                    logger.info("    license {} was {}removed.", licenseToRemove.getUuid(),
                            response.getWasRemoved() ? "" : "not ");
                }
            }
        }

        // add the new/updated licenses.
        List<LicenseDTO> licensesToAdd = edits.first;
        if (licensesToAdd.size() > 0) {
            logger.info("Adding {} intersight proxy licenses.", licensesToAdd.size());
            AddLicensesResponse response = licenseManagerClient.addLicenses(AddLicensesRequest.newBuilder()
                    .addAllLicenseDTO(licensesToAdd)
                    .build());
            // check for any licenses with errors
            int licensesWithErrorCount = 0;
            StringBuilder sb = new StringBuilder("The following licenses had errors: ");
            for (LicenseDTO license : response.getLicenseDTOList()) {
                if (license.getTurbo().getErrorReasonCount() > 0) {
                    licensesWithErrorCount += 1;
                    sb.append(license.getTurbo().getExternalLicenseKey()).append(":");
                    license.getTurbo().getErrorReasonList().forEach(reason -> sb.append(reason).append(" "));
                }
            }
            if (licensesWithErrorCount > 0) {
                // log an error message. We won't throw an error though, since the presence of the
                // response at all meant the sync task itself ran fine, and the issue is data-related.
                String errorMessage = sb.toString();
                logger.error(errorMessage);
                return false;
            }
        }

        // remember the licenses we targeted - this is provided to the license count updater, which
        // runs as a separate thread.
        this.availableIntersightLicenses = intersightLicenses;
        // return true if we proxied a non-fallback license.
        if (proxyLicenses.size() > 0) {
            if (!StringUtils.equals(proxyLicenses.get(0).getTurbo().getEdition(),
                    IntersightProxyLicenseEdition.IWO_FALLBACK.name())) {
                logger.debug("LicenseSync happy: an active Intersight license is currently installed.");
                return true;
            }
        }
        logger.debug("LicenseSync not happy: could not find an active Intersight license to proxy.");
        return false;
    }

    /**
     * Given a list of local proxy licenses and a list of intersight licenses, discover the set of
     * changes we need to make to the local proxy licenses so that they match the intersight licenses.
     *
     * <p>We will key licenses by the "moid" (managed object id) assigned by Intersight. We will store
     * this value in the "external license key" field of our {@link LicenseDTO} objects.
     *
     * @param localLicenses a list of {@link LicenseDTO} instances that represent the currently-installed
     *                      proxy licenses.
     * @param intersightLicenses a list of {@link LicenseLicenseInfo} instances that each represent a
     *                           license discovered from the Intersight API.
     * @return a {@link Pair} of lists of LicenseDTO objects, where the first member of the pair is a
     * list of licenses that should be added, and the second is a list of licenses that should be removed.
     */
    protected Pair<List<LicenseDTO>, List<LicenseDTO>> discoverLicenseEdits(final List<LicenseDTO> localLicenses,
                                                                            final List<LicenseDTO> intersightLicenses) {
        // we will consider a license to be a "match" if the moid of the Intersight license matches
        // the "external license key" of the proxy license.

        // To determine what updates we need to make, we'll build a map of license key -> Pair<intersight license, local license>
        final Map<String, Pair<LicenseDTO, LicenseDTO>> licenseMap = new HashMap<>(intersightLicenses.size());
        // add the intersight licenses
        // if the fallback license logic is on, then we're only going to consider "active" licenses.
        intersightLicenses.forEach(license -> licenseMap.put(license.getTurbo().getExternalLicenseKey(), new Pair(license, null)));

        // add the local licenses to the map. But only the ones recognized as "Intersight" licenses
        localLicenses.forEach(license -> {
            if (IntersightLicenseUtils.isIntersightLicense(license)) {
                licenseMap.compute(license.getTurbo().getExternalLicenseKey(),
                        (k, existingInfo) -> (existingInfo == null) ? new Pair(null, license) : new Pair(existingInfo.first, license));
            }
        });

        // at this point, the map has both sets of licenses loaded. We'll walk through the map and
        // pick out changes we need to apply.
        List<LicenseDTO> licensesToDelete = new ArrayList<>();
        List<LicenseDTO> licensesToAdd = new ArrayList<>();
        for (Pair<LicenseDTO, LicenseDTO> mapping : licenseMap.values()) {
            if (mapping.first == null) {
                // delete any local proxy licenses that didn't have a match in the intersight API
                // response
                logger.info("Adding local proxy license with key {} to removal list", mapping.second.getTurbo().getExternalLicenseKey());
                licensesToDelete.add(mapping.second);
            } else if (mapping.second == null) {
                // this license was found in the intersight response but not locally --add it.
                logger.info("Adding new proxy license with key {} to additions list", mapping.first.getTurbo().getExternalLicenseKey());
                licensesToAdd.add(mapping.first);
            } else {
                // exists in both places -- check if we need to update it.
                LicenseDTO localLicense = mapping.second;
                LicenseDTO intersightLicense = mapping.first;
                // are they the same?
                if (IntersightLicenseUtils.areProxyLicensesEqual(localLicense, intersightLicense)) {
                    logger.debug("Intersight license {} has not changed -- will not update.", localLicense.getTurbo().getExternalLicenseKey());
                } else {
                    logger.info("intersight license {} has changed, will update.", localLicense.getTurbo().getExternalLicenseKey());
                    // a changed license will be processed as an add of a new license + removal of
                    // old
                    licensesToAdd.add(intersightLicense);
                    licensesToDelete.add(localLicense);
                }
            }
        }

        return new Pair(licensesToAdd, licensesToDelete);
    }

    /**
     * Fetch the locally-installed licenses.
     *
     * @param licenseManagerClient a grpc license manager client.
     * @return a list of licenses currently installed in the xl instance.
     */
    protected List<LicenseDTO> getLocalLicenses(LicenseManagerServiceBlockingStub licenseManagerClient) {

        GetLicensesResponse licensesResponse = licenseManagerClient.getLicenses(GetLicensesRequest.getDefaultInstance());
        return licensesResponse.getLicenseDTOList();
    }

    /**
     * Wraps exception types that we would consider "retryable".
     */
    public static class RetriableIntersightException extends RuntimeException {
        public RetriableIntersightException(Throwable cause) {
            super(cause);
        }
    }
}
