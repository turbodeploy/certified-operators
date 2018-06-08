package com.vmturbo.auth.component.licensing;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import com.google.protobuf.Empty;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.grpc.stub.StreamObserver;
import reactor.core.publisher.Flux;

import com.vmturbo.api.dto.license.ILicense;
import com.vmturbo.api.dto.license.ILicense.ErrorReason;
import com.vmturbo.auth.component.licensing.LicenseManagerService.LicenseManagementEvent;
import com.vmturbo.common.protobuf.licensing.LicenseCheckServiceGrpc.LicenseCheckServiceImplBase;
import com.vmturbo.common.protobuf.licensing.Licensing.GetLicenseSummaryResponse;
import com.vmturbo.common.protobuf.licensing.Licensing.LicenseDTO;
import com.vmturbo.common.protobuf.licensing.Licensing.LicenseSummary;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.StringFilter;
import com.vmturbo.common.protobuf.search.Search.SearchFilter;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.search.Search.SearchRequest;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc.SearchServiceBlockingStub;
import com.vmturbo.licensing.License;
import com.vmturbo.licensing.utils.LicenseUtil;
import com.vmturbo.repository.api.Repository;
import com.vmturbo.repository.api.RepositoryListener;

/**
 * The LicenseCheckService is a close confederate of the {@link LicenseManagerService}. It's responsibilities
 * are as follows:
 * <ul>
 *     <li>Monitoring of active software licenses and checks to make sure they are valid and in
 *     compliance.</li>
 *     <li>Publication of a new {@link com.vmturbo.common.protobuf.licensing.Licensing.LicenseSummary} to kafka whenever a new set of checks is
 *     performed. The LicenseSummary includes:
 *   <ul>
 *       <li>workload count information</li>
 *       <li>expiration date information</li>
 *       <li>available feature set</li>
 *       <li>list of any validation errors with the installed licenses</li>
 *   </ul></li>
 *      <li>Whenever a new license check is performed, the LicenseCheckService will publish a
 *      License Summary containing the details of the active licenses and whether or not the
 *      workload allowance is in compliance.</li>
 *      <li>To ensure timeliness of the license checks, a new check will be performed whenever any
 *      of the following occur:
 *      <ul>
 *          <li>A license is added to or removed from storage (via {@link LicenseManagerService})</li>
 *          <li>A new live topology has been published to the Repository (for updating workload counts)</li>
 *          <li>A new day has started. (for catching expirations)</li>
 *      </ul></li>
 * </ul>
 * *
 * *
 * *
 */
public class LicenseCheckService extends LicenseCheckServiceImplBase implements RepositoryListener {
    static private final Logger logger = LogManager.getLogger();

    /**
     * A special constant LicenseSummary that is returned when there are no licenses.
     */
    static private final LicenseSummary NO_LICENSES_SUMMARY = LicenseSummary.getDefaultInstance();

    @Nonnull
    private final LicenseManagerService licenseManagerService;

    @Nonnull
    private final SearchServiceBlockingStub searchServiceClient;

    @Nonnull
    private final Repository repositoryListener;

    // cache the last license summary, defaulting to the "no license" result.
    private LicenseSummary lastSummary = NO_LICENSES_SUMMARY;

    // thread pool for scheduled license check updates
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    public LicenseCheckService(LicenseManagerService licenseManagerService,
                               SearchServiceBlockingStub searchServiceClient,
                               Repository repositoryListener) {
        this.licenseManagerService = licenseManagerService;
        // subscribe to the license manager event stream. This will trigger license check updates
        // whenever licenses are added / removed
        licenseManagerService.getEventStream().subscribe(this::handleLicenseManagerEvent);
        this.searchServiceClient = searchServiceClient;
        this.repositoryListener = repositoryListener;
        // subscribe to new topologies, so we can regenerate the license check when that happens
        repositoryListener.addListener(this);
        // schedule time-based updates. The first update will happen immediately (but on a separate
        // thread), and others will be triggered at the start of a new day.
        scheduleUpdates();
    }

    /**
     * Triggered whenever {@link LicenseManagerService} publishes a new event on it's event stream.
     *
     * @param event an {@link LicenseManagementEvent} to consume.
     */
    private void handleLicenseManagerEvent(LicenseManagementEvent event) {
        // regardless of the event, we will trigger a revalidating of the license data, since the
        // set of licenses will have changed.
        logger.info("Triggering license summary update on license manager event: {}", event.getType().name());
        updateLicenseSummary();
    }

    /**
     * Get the most recent {@link LicenseSummary} that's been generated.
     *
     * @param request an empty request.
     * @param responseObserver will recieve a {@link GetLicenseSummaryResponse} object containing a
     *                          {@link LicenseSummary} object, if one is available. The LicenseSummary field will
     *                         be empty, otherwise.
     */
    @Override
    public void getLicenseSummary(final Empty request, final StreamObserver<GetLicenseSummaryResponse> responseObserver) {
        // return the most recent summary
        GetLicenseSummaryResponse.Builder responseBuilder = GetLicenseSummaryResponse.newBuilder();
        if (lastSummary != null) {
            responseBuilder.setLicenseSummary(lastSummary);
        }
        responseObserver.onNext(responseBuilder.build());
        responseObserver.onCompleted();
    }

    /**
     * Publish a new {@link LicenseSummary}
     *
     * @param newSummary The new license summary to publish.
     */
    private void publishNewLicenseSummary(LicenseSummary newSummary) {
        //This will go to kafka when we add the license check client. For now, it will just update
        // the internal cache.
        logger.info("Publishing new license summary created at {}", newSummary.getGenerationDate());
        lastSummary = newSummary;
    }

    /**
     * Update the license summary info. Synchronized since we may (in unusual cases) have multiple
     * threads at once, such as if a live topology notification is sent from the repository at the
     * same time the daily license summary update is scheduled.
     *
     * When the new summary is ready, it will be published for the rest of the system to consume.
     */
    synchronized private void updateLicenseSummary() {
        logger.info("Updating license summary.");
        Collection<LicenseDTO> licenseDTOs = Collections.emptyList();
        try {
            // get all the licenses from licenseManager
            licenseDTOs = licenseManagerService.getLicenses();
        } catch (IOException ioe) {
            // error getting the licenses
            logger.warn("Error getting licenses from license manager. Will not update the license summary.", ioe);
            return;
        }

        // if no licenses at this point, use the "no license" summary
        if (licenseDTOs.isEmpty()) {
            lastSummary = NO_LICENSES_SUMMARY;
            publishNewLicenseSummary(lastSummary);
            return;
        }

        // we have licenses -- convert them to model licenses, validate them, and merge them together.
        License aggregateLicense = new License();
        for (LicenseDTO licenseDTO : licenseDTOs) {
            License license = LicenseDTOUtils.licenseDTOtoLicense(licenseDTO);
            license.setErrorReasons(LicenseDTOUtils.validateXLLicense(license));

            aggregateLicense.combine(license); // merge subsequent licenses into the first one
        }

        // get the workload count
        boolean isOverLimit = populateWorkloadCount(aggregateLicense);

        // at this point we have the aggregate license and workload count. Combine them to create
        // the license summary.
        LicenseSummary licenseSummary
                = LicenseDTOUtils.licenseToLicenseSummary(aggregateLicense, isOverLimit);

        // publish the news!!
        publishNewLicenseSummary(licenseSummary);
    }

    /**
     * Populate the workload count field on a license, based on it's counted entity type, and return
     * whether the limit was exceeded or not.
     *
     * @param license The license to populate.
     * @return the populated license, which is actually the same license instance that was passed in.
     */
    private boolean populateWorkloadCount(License license) {
        // get the workload count from the repository and check if we are over the limit
        // we will fetch either all PM's or active VM's depending on counted entity type.
        SearchRequest.Builder entityCountRequestBuilder = SearchRequest.newBuilder();
        switch (license.getCountedEntity()) {
            case VM:
                logger.debug("Counting active VMs");
                entityCountRequestBuilder.addSearchParameters(
                        SearchParameters.newBuilder()
                                .setStartingFilter(PropertyFilter.newBuilder()
                                        .setPropertyName("entityType")
                                        .setStringFilter(StringFilter.newBuilder()
                                                .setStringPropertyRegex("VirtualMachine")
                                                .build())
                                        .build())
                                .addSearchFilter(SearchFilter.newBuilder()
                                        .setPropertyFilter(PropertyFilter.newBuilder()
                                                .setPropertyName("state")
                                                .setStringFilter(StringFilter.newBuilder()
                                                        .setStringPropertyRegex("ACTIVE"))))
                                .build());
                break;
            case SOCKET:
                // SOCKET count type is not supported in XL!
                // just log a warning here, since this should have been picked up in the validation
                // process.
                logger.warn("Socket-based licenses are not supported in XL.");
                break;
            default:
                // ditto for any other counted entity type
                logger.warn("Counted Entity type {} not understood -- will not count workloads",
                        license.getCountedEntity());
                break;
        }

        // get the appropriate workload count.
        // we're going to check the workload limit here and set a boolean, since it's not
        // flagged in the license objects directly.
        boolean isOverLimit = false;
        SearchRequest request = entityCountRequestBuilder.build();
        if (request.getSearchParametersCount() == 0) {
            logger.info("Empty entity count request -- will not request workload count.");
        } else {
            // call using waitForReady -- this is on a worker thread, and we are willing to block
            // until the repository service is up.
            int numInUseEntities = searchServiceClient.withWaitForReady().countEntities(request).getEntityCount();
            logger.debug("Search returned {} entities.", numInUseEntities);
            isOverLimit = numInUseEntities > license.getNumLicensedEntities();
            license.setNumInUseEntities(numInUseEntities);

            if (isOverLimit) {
                logger.warn("Active workload count ({}) is over the license limit ({}).",
                        license.getNumInUseEntities(), license.getNumLicensedEntities());
            }
        }
        return isOverLimit;
    }

    /**
     * This function will perform a license check update as part of a scheduled event. In addition
     * to updating the license check information, we may also send out daily reminders, etc. in
     * this function.
     */
    private void updateLicenseSummaryPeriodically() {
        logger.info("Daily license check triggered.");
        updateLicenseSummary();
        //TODO: additional notifications when we have the notifications feature available.
    }

    /**
     * Set up scheduled license check updates:
     * 1) The first one should happen soon so we have a license summary to offer.
     * 2) There should be one daily, at the start of a new day.
     * 3) updates will also be triggered when licenses are added/removed -- this is handled as a
     * license manager event.
     * 4) Updates should be triggered on new live topology broadcasts. This will be handled via a
     */
    private void scheduleUpdates() {
        logger.info("Scheduling license summary updates.");
        // schedule the first one almost immediately.
        scheduler.schedule(this::updateLicenseSummary, 5, TimeUnit.SECONDS);

        // schedule the others as a recurring daily event at midnight.
        LocalDateTime now = LocalDateTime.now();
        LocalDateTime midnight = LocalDate.now().plusDays(1).atStartOfDay();
        long secondsUntilMidnight = now.until(midnight, ChronoUnit.SECONDS);
        logger.info("Daily updates will start in {} seconds.", secondsUntilMidnight);

        // If we want more long-term precision, we should periodically reschedule to account for
        // some drifts in number of seconds per day.
        scheduler.scheduleAtFixedRate(this::updateLicenseSummaryPeriodically,
                secondsUntilMidnight,
                TimeUnit.DAYS.toSeconds(1),
                TimeUnit.SECONDS);
    }

    /**
     * Triggered whenever a "new live topology available" notification from the Repository is picked
     * up from kafka.
     *
     * @param topologyId topology id
     * @param topologyContextId context id of the available topology
     */
    @Override
    public void onSourceTopologyAvailable(final long topologyId, final long topologyContextId) {
        // we are using this as a trigger to update the license summary.
        logger.info("New live topology available in repository -- will update the license summary");
        updateLicenseSummary();
    }
}
