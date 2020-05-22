package com.vmturbo.auth.component.licensing;

import java.io.IOException;
import java.time.Clock;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.Empty;

import io.grpc.stub.StreamObserver;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.util.StringUtils;

import com.vmturbo.api.dto.license.ILicense;
import com.vmturbo.auth.api.auditing.AuditLogUtils;
import com.vmturbo.auth.component.licensing.LicenseManagerService.LicenseManagementEvent;
import com.vmturbo.common.protobuf.action.ActionDTO.Severity;
import com.vmturbo.common.protobuf.licensing.LicenseCheckServiceGrpc.LicenseCheckServiceImplBase;
import com.vmturbo.common.protobuf.licensing.Licensing.GetLicenseSummaryResponse;
import com.vmturbo.common.protobuf.licensing.Licensing.LicenseDTO;
import com.vmturbo.common.protobuf.licensing.Licensing.LicenseSummary;
import com.vmturbo.common.protobuf.search.Search.CountEntitiesRequest;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.StringFilter;
import com.vmturbo.common.protobuf.search.Search.SearchFilter;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.search.Search.SearchQuery;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc.SearchServiceBlockingStub;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.components.api.server.ComponentNotificationSender;
import com.vmturbo.components.api.server.IMessageSender;
import com.vmturbo.components.common.mail.MailConfigException;
import com.vmturbo.components.common.mail.MailEmptyConfigException;
import com.vmturbo.components.common.mail.MailException;
import com.vmturbo.components.common.mail.MailManager;
import com.vmturbo.licensing.License;
import com.vmturbo.notification.api.NotificationSender;
import com.vmturbo.notification.api.dto.SystemNotificationDTO.SystemNotification;
import com.vmturbo.notification.api.dto.SystemNotificationDTO.SystemNotification.Category;
import com.vmturbo.repository.api.RepositoryListener;
import com.vmturbo.repository.api.impl.RepositoryNotificationReceiver;

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
    private static final Logger logger = LogManager.getLogger();

    /**
     * A special constant LicenseSummary that is returned when there are no licenses.
     */
    private static final LicenseSummary NO_LICENSES_SUMMARY = LicenseSummary.getDefaultInstance();
    @VisibleForTesting
    public static final String TURBONOMIC_LICENSE_HAS_EXPIRED_PLEASE_UPDATE_IT
            = "Your license has expired, please update it";

    @VisibleForTesting
    public static final String LICENSE_HAS_EXPIRED = "License has expired";

    @VisibleForTesting
    public static final String LICENSE_WORKLOAD_COUNT_HAS_OVER_LIMIT
            = "It’s great to see you're getting so much value from our product! Your product installation" +
            " on %s is currently managing %d active workloads, while your license only covers %d workloads. Don't worry," +
        " we'll continue to manage all of your workloads without interruption. Unfortunately," +
        " you cannot update your version or add new targets until you upgrade your license to cover" +
        " all your workloads. To add more workloads to your license, contact your" +
        " sales representative or authorized dealer.";

    @VisibleForTesting
    public static final String WORKLOAD_COUNT_IS_OVER_LIMIT = "Workload count is over limit";

    @VisibleForTesting
    public static final String LICENSE_IS_ABOUT_TO_EXPIRE = "License is about to expire";

    @VisibleForTesting
    public static final String TURBONOMIC_LICENSE_WILL_EXPIRE = "The product license on your" +
        " instance %s will expire tomorrow, %s. To keep using the full power of" +
        " this product, be sure to install an updated license. To obtain a license, contact your" +
        " sales representative or authorized dealer.";

    @VisibleForTesting
    public static final String TURBONOMIC_LICENSE_IS_MISSING = "Your instance (%s) has no " +
            "license, or your license has expired. Please install a valid license. To obtain a license, " +
            "contact your sales representative or authorized dealer.";

    @VisibleForTesting
    public static final String LICENSE_IS_MISSING = "License is missing";

    @Nonnull
    private final LicenseManagerService licenseManagerService;

    @Nonnull
    private final SearchServiceBlockingStub searchServiceClient;

    @Nonnull
    private final RepositoryNotificationReceiver repositoryListener;

    @Nonnull
    private final LicenseSummaryPublisher licenseSummaryPublisher;

    @Nonnull
    private final NotificationSender notificationSender;

    @Nonnull
    private final MailManager mailManager;

    private final int numBeforeLicenseExpirationDays;

    @Nonnull
    private final Clock clock;

    // cache the last license summary, defaulting to the "no license" result.
    private LicenseSummary lastSummary = NO_LICENSES_SUMMARY;

    // thread pool for scheduled license check updates
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    public LicenseCheckService(@Nonnull final LicenseManagerService licenseManagerService,
                               @Nonnull final SearchServiceBlockingStub searchServiceClient,
                               @Nonnull final RepositoryNotificationReceiver repositoryListener,
                               @Nonnull final IMessageSender<LicenseSummary> licenseSummarySender,
                               @Nonnull final NotificationSender notificationSender,
                               @Nonnull final MailManager mailManager,
                               @Nonnull final Clock clock,
                               final int numBeforeLicenseExpirationDays,
                               boolean scheduleUpdates) {
        this.licenseManagerService = licenseManagerService;
        // subscribe to the license manager event stream. This will trigger license check updates
        // whenever licenses are added / removed
        licenseManagerService.getEventStream().subscribe(this::handleLicenseManagerEvent);
        this.searchServiceClient = searchServiceClient;
        this.repositoryListener = repositoryListener;

        // create the license summary publisher
        licenseSummaryPublisher = new LicenseSummaryPublisher(licenseSummarySender);

        this.notificationSender = Objects.requireNonNull(notificationSender);

        this.clock = Objects.requireNonNull(clock);

        this.mailManager = mailManager;

        this.numBeforeLicenseExpirationDays = numBeforeLicenseExpirationDays;

        // subscribe to new topologies, so we can regenerate the license check when that happens
        repositoryListener.addListener(this);
        // schedule time-based updates. The first update will happen immediately (but on a separate
        // thread), and others will be triggered at the start of a new day.
        if (scheduleUpdates) {
            scheduleUpdates();
        } else {
            logger.info("Not scheduling daily license check updates");
        }
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
        checkLicensesForNotification();
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
     * Publish a new {@link LicenseSummary}.
     *
     * @param newSummary The new license summary to publish.
     */
    private void publishNewLicenseSummary(LicenseSummary newSummary) {
        //This will go to kafka when we add the license check client. For now, it will just update
        // the internal cache.
        logger.info("Publishing new license summary created at {}", newSummary.getGenerationDate());
        try {
            licenseSummaryPublisher.publish(newSummary);

        } catch (InterruptedException | CommunicationException e) {
            logger.error("Error publishing license summary", e);
        } finally {
            lastSummary = newSummary;
        }
    }

    /**
     * Check license status and will trigger sending out notification if needed.
     */
    @VisibleForTesting
    void checkLicensesForNotification() {
        logger.info("Checking license status.");
        try {
            // get all the licenses from licenseManager
            final Collection<LicenseDTO> licenseDTOs = licenseManagerService.getLicenses();

            // if no licenses at this point, use the "no license" summary
            if (licenseDTOs.isEmpty()) {
                notifyUI(String.format(TURBONOMIC_LICENSE_IS_MISSING, AuditLogUtils.getLocalIpAddress()),
                    LICENSE_IS_MISSING);
            } else {
                // we have licenses -- convert them to model licenses, validate them, and merge them together.
                final License aggregateLicense = LicenseDTOUtils.combineLicenses(licenseDTOs);

                // get the workload count
                boolean isOverLimit = populateWorkloadCount(aggregateLicense);
                publishNotification(isOverLimit, licenseDTOs, aggregateLicense);
            }
        } catch (IOException ioe) {
            // error getting the licenses
            logger.warn("Error getting licenses from license manager. Will not send out notification.", ioe);
        }
    }

    /**
     * Update the license summary info. Synchronized since we may (in unusual cases) have multiple
     * threads at once, such as if a live topology notification is sent from the repository at the
     * same time the daily license summary update is scheduled.
     *
     * When the new summary is ready, it will be published for the rest of the system to consume.
     */
    private synchronized void updateLicenseSummary() {
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
        // we'll use this "combined" license to determine active features and detect invalid license
        // combinations.
        License aggregateLicense = LicenseDTOUtils.combineLicenses(licenseDTOs);

        // get the workload count
        boolean isOverLimit = populateWorkloadCount(aggregateLicense);

        // at this point we have the aggregate license and workload count. Combine them to create
        // the license summary.
        LicenseSummary licenseSummary
                = LicenseDTOUtils.createLicenseSummary(aggregateLicense, isOverLimit);
        // publish the news!!
        publishNewLicenseSummary(licenseSummary);
    }

    /**
     * Is the license going to expire?
     * @param expirationDate the license expiration date
     * @param numBeforeLicenseExpirationDays the days before license expiration
     * @return true if the license is going to expire
     */
    @VisibleForTesting
    boolean isGoingToExpire(@Nonnull final String expirationDate,
                             final int numBeforeLicenseExpirationDays) {
        if (ILicense.PERM_LIC.equals(expirationDate)) {
            return false;
        }
        final LocalDate localExpirationDate = LocalDate.parse(expirationDate);
        final LocalDate now = LocalDate.now(clock);
        final LocalDate adjustedExpirationDate = localExpirationDate.minusDays(numBeforeLicenseExpirationDays);
        return (adjustedExpirationDate.isBefore(now) || adjustedExpirationDate.isEqual(now))
                && localExpirationDate.isAfter(now);
    }


    /**
     * Publish notification to UI and license owner.
     *
     * @param isOverLimit is over workload license limit?
     * @param licenseDTOs collection of license DTOs
     * @param aggregateLicense aggregated license
     */
    @VisibleForTesting
    void publishNotification(final boolean isOverLimit,
                             @Nonnull final Collection<LicenseDTO> licenseDTOs,
                             @Nonnull final License aggregateLicense) {
        licenseDTOs.forEach(licenseDTO -> {
                    final License license = LicenseDTOUtils.licenseDTOtoLicense(licenseDTO);
                    if (license.isExpired()) {
                        notifyLicenseExpiration(TURBONOMIC_LICENSE_HAS_EXPIRED_PLEASE_UPDATE_IT,
                                LICENSE_HAS_EXPIRED, license);
                        return;
                    }
                    if (isGoingToExpire(license.getExpirationDate(), numBeforeLicenseExpirationDays)) {
                        final String description = String.format(TURBONOMIC_LICENSE_WILL_EXPIRE,
                                AuditLogUtils.getLocalIpAddress(), license.getExpirationDate());
                        notifyLicenseExpiration(description, LICENSE_IS_ABOUT_TO_EXPIRE, license);
                    }
                    if (isOverLimit) {
                        final String description = String.format(LICENSE_WORKLOAD_COUNT_HAS_OVER_LIMIT,
                            AuditLogUtils.getLocalIpAddress(),
                            aggregateLicense.getNumInUseEntities(),
                            aggregateLicense.getNumLicensedEntities());
                        notifyLicenseExpiration(description, WORKLOAD_COUNT_IS_OVER_LIMIT, license);
                    }
                }
        );
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
        CountEntitiesRequest.Builder entityCountRequestBuilder = CountEntitiesRequest.newBuilder();
        if (license.getCountedEntity() == null) {
            logger.debug("Counted Entity type is null.");
            return false;
        }
        switch (license.getCountedEntity()) {
            case VM:
                logger.debug("Counting active VMs");
                entityCountRequestBuilder.setSearch(SearchQuery.newBuilder()
                    .addSearchParameters(SearchParameters.newBuilder()
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
                        .build()));
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
        CountEntitiesRequest request = entityCountRequestBuilder.build();
        if (request.getSearch().getSearchParametersCount() == 0) {
            logger.info("Empty entity count request -- will not request workload count.");
        } else {
            // call using waitForReady -- this is on a worker thread, and we are willing to block
            // until the repository service is up.
            int numInUseEntities = searchServiceClient.withWaitForReady().countEntities(request).getEntityCount();
            logger.debug("Search returned {} entities.", numInUseEntities);

            isOverLimit = (numInUseEntities > license.getNumLicensedEntities());
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
    }

    // notify license expiration
    private void notifyLicenseExpiration(@Nonnull final String longDescription,
                                         @Nonnull final String shortDescription,
                                         @Nonnull final License license) {
        try {
            notifyUI(longDescription, shortDescription);
            if (!StringUtils.isEmpty(license.getEmail())) {
                mailManager.sendMail(Collections.singletonList(license.getEmail()),
                        shortDescription, longDescription);
                logger.info("Sent out license expiration email to {}: {}", license.getEmail(), longDescription);
            } else {
                logger.warn("License doesn't have email address, skip sending email.");
            }
        } catch (MailEmptyConfigException e) {
            logger.warn(e.getMessage());
        } catch (MailException e) {
            logger.error("Failed to send notification by email: ", e);
        } catch (MailConfigException e) {
            logger.error("SMTP server is not configured: " + e);
        }
    }

    // distribute notification to UI
    private void notifyUI(@Nonnull final String longDescription,
                          @Nonnull final String shortDescription) {
        try {
            notificationSender.sendNotification(
                    Category.newBuilder().setLicense(SystemNotification.License.newBuilder().build())
                            .build(),
                    longDescription,
                    shortDescription,
                    Severity.CRITICAL);
        } catch (CommunicationException | InterruptedException e) {
            logger.error("Error publishing license expire notification", e);
        }
    }

    /**
     * Set up scheduled license check updates and send notification:
     * 1) The first one should happen soon so we have a license summary to offer.
     * 2) There should be one daily, at the start of a new day.
     * 3) updates will also be triggered when licenses are added/removed -- this is handled as a
     * license manager event.
     * 4) Updates should be triggered on new live topology broadcasts. This will be handled via a
     */
    private void scheduleUpdates() {
        logger.info("Scheduling license summary updates.");
        // schedule the first one almost immediately.
        scheduler.schedule(() -> {
            updateLicenseSummary();
            checkLicensesForNotification();
        }, 5, TimeUnit.SECONDS);

        // schedule the others as a recurring daily event at midnight.
        LocalDateTime now = LocalDateTime.now();
        LocalDateTime midnight = LocalDate.now().plusDays(1).atStartOfDay();
        long secondsUntilMidnight = now.until(midnight, ChronoUnit.SECONDS);
        logger.info("Daily updates will start in {} seconds.", secondsUntilMidnight);

        // If we want more long-term precision, we should periodically reschedule to account for
        // some drifts in number of seconds per day.
        scheduler.scheduleAtFixedRate(() -> {
                updateLicenseSummaryPeriodically();
                checkLicensesForNotification();
            }, secondsUntilMidnight, TimeUnit.DAYS.toSeconds(1), TimeUnit.SECONDS);
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

    /**
     * Sends a LicenseSummary message to kafka.
     */
    public static class LicenseSummaryPublisher extends ComponentNotificationSender<LicenseSummary> {

        public static final String LICENSE_SUMMARY_KEY = "latest";

        private final IMessageSender<LicenseSummary> sender;

        public LicenseSummaryPublisher(@Nonnull IMessageSender<LicenseSummary> sender) {
            this.sender = Objects.requireNonNull(sender);
        }

        public void publish(LicenseSummary newSummary)
                throws CommunicationException, InterruptedException {
            sender.sendMessage(newSummary);
        }

        @Override
        protected String describeMessage(@Nonnull final LicenseSummary licenseSummary) {
            return LicenseSummary.class.getSimpleName()
                    + "[generated " + licenseSummary.getGenerationDate() + "]";
        }

        /**
         * This is a message key generator for sending {@link LicenseSummary} messages. Because we
         * only have a concept of one "latest" license summary, we will always send using the same
         * key. We do not want the default behavior of incremental keys that we would otherwise get.
         *
         * @param summary the message to send
         * @return our static String key to use for the message.
         */
        public static String generateMessageKey(LicenseSummary summary) {
            return LICENSE_SUMMARY_KEY;
        }

    }
}
