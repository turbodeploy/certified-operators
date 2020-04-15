package com.vmturbo.integrations.intersight.licensing;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.annotation.Nonnull;

import com.cisco.intersight.client.ApiException;
import com.cisco.intersight.client.model.LicenseLicenseInfo;
import com.cisco.intersight.client.model.LicenseLicenseInfoList;
import com.google.protobuf.Empty;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.auth.api.auditing.AuditAction;
import com.vmturbo.auth.api.auditing.AuditLog;
import com.vmturbo.common.protobuf.licensing.LicenseManagerServiceGrpc.LicenseManagerServiceBlockingStub;
import com.vmturbo.common.protobuf.licensing.Licensing.AddLicensesRequest;
import com.vmturbo.common.protobuf.licensing.Licensing.AddLicensesResponse;
import com.vmturbo.common.protobuf.licensing.Licensing.GetLicensesResponse;
import com.vmturbo.common.protobuf.licensing.Licensing.LicenseDTO;
import com.vmturbo.common.protobuf.licensing.Licensing.RemoveLicenseRequest;
import com.vmturbo.common.protobuf.licensing.Licensing.RemoveLicenseResponse;
import com.vmturbo.commons.Pair;
import com.vmturbo.components.api.RetriableOperation;
import com.vmturbo.components.api.RetriableOperation.ConfigurableBackoffStrategy;
import com.vmturbo.components.api.RetriableOperation.RetriableOperationFailedException;

/**
 * Synchronizes license information with the Intersight upstream services.
 */
public class IntersightLicenseSyncService {
    private static final Logger logger = LogManager.getLogger();

    // thread for scheduling the license sync
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    private boolean enabled;

    private final IntersightLicenseClient intersightLicenseClient;

    private final LicenseManagerServiceBlockingStub licenseManagerClient;

    private final int licenseSyncInitialCheckSecs;

    private final int licenseSyncIntervalSecs;

    private final int licenseSyncRetrySeconds;

    /**
     * Construct an IntersightLicenseSyncService.
     *
     * @param enabled if false, then the service will be disabled.
     * @param intersightLicenseClient intersight license client instance for making intersight api calls
     * @param licenseSyncInitialCheckSecs delay (in seconds) of initial license sync check
     * @param licenseSyncIntervalSecs delay (in secs) between license sync checks
     * @param licenseSyncRetrySeconds max time allowed (in secs) to complete a license sync, including
     *                                retries.
     * @param licenseManagerClient a grpc client for accessing the LicenseManagerService.
     */
    public IntersightLicenseSyncService(boolean enabled,
                                        @Nonnull final IntersightLicenseClient intersightLicenseClient,
                                        int licenseSyncInitialCheckSecs, int licenseSyncIntervalSecs,
                                        int licenseSyncRetrySeconds,
                                        @Nonnull LicenseManagerServiceBlockingStub licenseManagerClient) {
        this.enabled = enabled;
        this.intersightLicenseClient = intersightLicenseClient;
        this.licenseSyncIntervalSecs = licenseSyncIntervalSecs;
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
        scheduler.scheduleWithFixedDelay(() -> runRetriableOperation("License Sync", licenseSyncOperation, licenseSyncRetrySeconds), licenseSyncInitialCheckSecs,
                licenseSyncIntervalSecs, TimeUnit.SECONDS );
    }

    private void runRetriableOperation(String name, RetriableOperation operation, long timeout) {
        Instant startTime = Instant.now();
        try {
            logger.info("Running operation {}", name);
            operation.run(timeout, TimeUnit.SECONDS);
        } catch (InterruptedException ie) {
            logger.info("Retriable operation {} interrupted.", name, ie);
        } catch (TimeoutException te) {
            Duration timeoutTime = Duration.between(startTime, Instant.now());
            logger.error("Retriable operation {} timed out after {} ms.", name,
                    timeoutTime.toMillis());
        } catch (RetriableOperationFailedException rofe) {
            logger.error("Operation {} failed with exception.", name, rofe);
        }
    }

    /**
     * Synchronize the Intersight license info. This is in the direction of Intersight -> Local
     * XL Instance. We are polling the Intersight API for now, but hopefully in the future we can
     * get a push from Intersight -> XL instead.
     * @return true if the sync was successful, false otherwise.
     * @throws RetriableOperationFailedException if a retriable exception is caught within the method.
     */
    public Boolean syncIntersightLicenses() throws RetriableOperationFailedException {

        // retrieve the list of licenses from Intersight
        logger.info("Fetching intersight licenses...");
        final LicenseLicenseInfoList licenseInfoList;
        try {
            licenseInfoList = intersightLicenseClient.getIntersightLicenses();
        } catch (ApiException | IOException e) {
            if (IntersightLicenseClient.isRetryable(e)) {
                throw new RetriableIntersightException(e);
            } else {
                throw new RetriableOperation.RetriableOperationFailedException(e);
            }
        }
        final List<LicenseLicenseInfo> intersightLicenses = licenseInfoList.getResults();
        if (intersightLicenses.size() > 0) {
            // log some info. This would normally be debug output, but we may not get much to work with
            // when debugging issues from the intersight environment, so let's err on the side of
            // TMI until we know things are working reliably.
            StringBuilder sb = new StringBuilder("Found licenses: ");
            for (LicenseLicenseInfo licenseInfo : intersightLicenses) {
                sb.append("moid ").append(licenseInfo.getMoid())
                        .append(" with ").append(licenseInfo.getDaysLeft()).append(" days left. ");
            }
            logger.info(sb.toString());
        }

        // retrieve the list of licenses we have locally installed
        logger.info("Fetching local licenses...");
        final List<LicenseDTO> localLicenses = getLocalLicenses(licenseManagerClient);

        // get the set of changes we'd need to make to get in sync with the intersight licenses.
        logger.info("Synchronizing {} local licenses with {} licenses discovered from intersight...",
                localLicenses.size(), intersightLicenses.size());
        Pair<List<LicenseDTO>, List<LicenseDTO>> edits = discoverLicenseEdits(localLicenses, intersightLicenses);

        // delete any licenses that need to be deleted.
        List<LicenseDTO> licensesToDelete = edits.second;
        if (licensesToDelete.size() > 0) {
            logger.info("Removing {} intersight proxy licenses.", licensesToDelete.size());
            for (LicenseDTO licenseToRemove : licensesToDelete) {
                logger.info("    Removing license {} with moid {}", licenseToRemove.getUuid(),
                        licenseToRemove.getExternalLicenseKey());
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
                if (license.getErrorReasonCount() > 0) {
                    licensesWithErrorCount += 1;
                    sb.append(license.getExternalLicenseKey()).append(":");
                    license.getErrorReasonList().forEach(reason -> sb.append(reason).append(" "));
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

        return true;

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
                                                                            final List<LicenseLicenseInfo> intersightLicenses) {
        // we will consider a license to be a "match" if the moid of the Intersight license matches
        // the "external license key" of the proxy license.

        // To determine what updates we need to make, we'll build a map of license key -> Pair<intersight license, local license>
        final Map<String, Pair<LicenseLicenseInfo, LicenseDTO>> licenseMap = new HashMap<>(intersightLicenses.size());
        // add the intersight licenses
        intersightLicenses.forEach(license -> licenseMap.put(license.getMoid(), new Pair(license, null)));
        // add the local licenses to the map. But only the ones recognized as "Intersight" licenses
        localLicenses.forEach(license -> {
            if (IntersightLicenseUtils.isIntersightLicense(license)) {
                licenseMap.compute(license.getExternalLicenseKey(),
                        (k, existingInfo) -> (existingInfo == null) ? new Pair(null, license) : new Pair(existingInfo.first, license));
            }
        });

        // at this point, the map has both sets of licenses loaded. We'll walk through the map and
        // pick out changes we need to apply.
        List<LicenseDTO> licensesToDelete = new ArrayList<>();
        List<LicenseDTO> licensesToAdd = new ArrayList<>();
        for (Pair<LicenseLicenseInfo, LicenseDTO> mapping : licenseMap.values()) {
            if (mapping.first == null) {
                // delete any local proxy licenses that didn't have a match in the intersight API
                // response
                licensesToDelete.add(mapping.second);
            } else if (mapping.second == null) {
                // this license was found in the intersight response but not locally --add it.
                licensesToAdd.add(IntersightLicenseUtils.toProxyLicense(mapping.first));
            } else {
                // exists in both places -- check if we need to update it.
                LicenseDTO localLicense = mapping.second;
                LicenseDTO mappedRemoteLicense = IntersightLicenseUtils.toProxyLicense(mapping.first);
                // are they the same?
                if (IntersightLicenseUtils.areProxyLicensesEqual(localLicense, mappedRemoteLicense)) {
                    logger.info("Intersight license {} has not changed -- will not update.", localLicense.getExternalLicenseKey());
                } else {
                    logger.info("intersight license {} has changed, will update.", localLicense.getExternalLicenseKey());
                    // a changed license will be processed as an add of a new license + removal of
                    // old
                    licensesToAdd.add(mappedRemoteLicense);
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

        GetLicensesResponse licensesResponse = licenseManagerClient.getLicenses(Empty.getDefaultInstance());
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
