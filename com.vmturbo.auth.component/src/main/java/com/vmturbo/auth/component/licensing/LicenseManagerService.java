package com.vmturbo.auth.component.licensing;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import io.grpc.stub.StreamObserver;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

import com.vmturbo.api.dto.license.ILicense;
import com.vmturbo.api.dto.license.ILicense.ErrorReason;
import com.vmturbo.auth.component.licensing.store.ILicenseStore;
import com.vmturbo.common.protobuf.action.ActionDTO.Severity;
import com.vmturbo.common.protobuf.licensing.LicenseManagerServiceGrpc.LicenseManagerServiceImplBase;
import com.vmturbo.common.protobuf.licensing.Licensing.AddLicensesRequest;
import com.vmturbo.common.protobuf.licensing.Licensing.AddLicensesResponse;
import com.vmturbo.common.protobuf.licensing.Licensing.GetLicenseRequest;
import com.vmturbo.common.protobuf.licensing.Licensing.GetLicenseResponse;
import com.vmturbo.common.protobuf.licensing.Licensing.GetLicensesRequest;
import com.vmturbo.common.protobuf.licensing.Licensing.GetLicensesResponse;
import com.vmturbo.common.protobuf.licensing.Licensing.LicenseDTO;
import com.vmturbo.common.protobuf.licensing.Licensing.LicenseDTO.ExternalLicense.Type;
import com.vmturbo.common.protobuf.licensing.Licensing.LicenseDTO.TypeCase;
import com.vmturbo.common.protobuf.licensing.Licensing.LicenseFilter;
import com.vmturbo.common.protobuf.licensing.Licensing.RemoveLicenseRequest;
import com.vmturbo.common.protobuf.licensing.Licensing.RemoveLicenseResponse;
import com.vmturbo.common.protobuf.licensing.Licensing.ValidateLicensesRequest;
import com.vmturbo.common.protobuf.licensing.Licensing.ValidateLicensesResponse;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.licensing.License;
import com.vmturbo.licensing.utils.LicenseUtil;
import com.vmturbo.notification.api.NotificationSender;
import com.vmturbo.notification.api.dto.SystemNotificationDTO.SystemNotification;
import com.vmturbo.notification.api.dto.SystemNotificationDTO.SystemNotification.Category;
import com.vmturbo.proactivesupport.DataMetricCounter;
import com.vmturbo.proactivesupport.DataMetricTimer;

/**
 * The LicenseManagerService manages storage and retrieval of Turbonomic software licenses. More
 * specifically, it has the following responsibilities:
 * <ul>
 *     <li>Track installation and removal of multiple licenses</li>
 *     <li>Allow retrieval of one or all licenses in storage</li>
 *     <li>Validate that new licenses being installed are compatible with any existing installed licenses</li>
 *     <li>Validate that new licenses aren't duplicates or already expired.</li>
 *     <li>User permission checks on license management operations</li>
 *     <li>Support for both Turbonomic Workload licenses and Cisco Workplace Optimization Manager licenses</li>
 *     <li>Publish local events when licenses are added or removed via a {@link reactor.core.publisher.Flux}.</li>
 * </ul>
 *
 */
public class LicenseManagerService extends LicenseManagerServiceImplBase {
    static private final Logger logger = LogManager.getLogger();

    // metrics -- doing these manually for now, but it'd be nice to integrate something at the
    // plumbing level. Just using some cheap counters.
    private static final DataMetricCounter RPC_RECEIVED_COUNT = DataMetricCounter.builder()
            .withName("auth_license_manager_grpc_received_count")
            .withHelp("Number of GRPC requests received by the auth LicenseManager GRPC service.")
            .withLabelNames("method")
            .build()
            .register();

    // Error count will be imperfect since we aren't catching all exceptions. Still, I feel it's
    // better to have some indication of errors than none. This metric will go away when we have
    // lower-level gRPC metrics available anyways.
    private static final DataMetricCounter RPC_ERROR_COUNT = DataMetricCounter.builder()
            .withName("auth_license_manager_grpc_received_error_count")
            .withHelp("Number of GRPC requests received by the auth LicenseManager GRPC service.")
            .withLabelNames("method")
            .build()
            .register();

    private static final DataMetricCounter RPC_PROCESSING_MS = DataMetricCounter.builder()
            .withName("auth_license_manager_grpc_processing_ms")
            .withHelp("Tracks time spent processing GRPC requests on the auth LicenseManager GRPC service.")
            .withLabelNames("method")
            .build()
            .register();
    private final NotificationSender notificationSender;

    // reference to the storage system used to persist the licenses.
    private ILicenseStore licenseStore;

    // The flux and fluxsink are used to broadcast events when licenses are added/removed. License
    // Check Service will be listening for these.
    private Flux<LicenseManagementEvent> eventFlux;
    private FluxSink<LicenseManagementEvent> eventFluxSink;

    /**
     * Constructor.
     *
     * @param licenseStore          license store.
     * @param notificationSender    notification sender.
     */
    public LicenseManagerService(@Nonnull ILicenseStore licenseStore,
            @Nonnull final NotificationSender notificationSender) {
        this.licenseStore = licenseStore;
        // create the event flux
        eventFlux = Flux.create(emitter -> eventFluxSink = emitter);
        // start publishing immediately w/o waiting for a consumer to signal demand.
        // Future subscribers will pick up on future statuses
        eventFlux.publish().connect();
        this.notificationSender = Objects.requireNonNull(notificationSender);
    }

    /**
     * Get access to the {@link LicenseManagementEvent} stream emitted by this component.
     *
     * @return a Flux of {@link LicenseManagementEvent} that can be subscribed to.
     */
    public Flux<LicenseManagementEvent> getEventStream() {
        return eventFlux;
    }

    /**
     * Publish a new event on the stream
     *
     * @param eventType The type of {@link LicenseManagementEventType} to publish.
     */
    private void publishEvent(LicenseManagementEventType eventType) {
        eventFluxSink.next(new LicenseManagementEvent(eventType));
    }

    @Override
    public void getLicense(final GetLicenseRequest request, final StreamObserver<GetLicenseResponse> responseObserver) {
        RPC_RECEIVED_COUNT.labels("getLicense").increment();
        try (DataMetricTimer timer = RPC_PROCESSING_MS.labels("getLicense").startTimer()) {
            try {
                GetLicenseResponse.Builder responseBuilder = GetLicenseResponse.newBuilder();
                LicenseDTO license = licenseStore.getLicense(request.getUuid());
                if (license != null) {
                    responseBuilder.setLicenseDTO(license);
                }
                responseObserver.onNext(responseBuilder.build());
            } catch (Exception e) {
                responseObserver.onError(e);
                RPC_ERROR_COUNT.labels("getLicense").increment();
                return;
            }
        }
    }

    private boolean licenseMatchesFilter(LicenseDTO license, LicenseFilter filter) {
        if (ILicense.isExpired(LicenseUtil.getExpirationDate(license)) && !filter.getIncludeExpired()) {
            return false;
        }

        boolean typeMatches = true;
        switch (filter.getType()) {
            case TURBO:
                typeMatches = license.getTypeCase() == TypeCase.TURBO;
                break;
            case EXTERNAL:
                typeMatches = license.getTypeCase() == TypeCase.EXTERNAL;
                break;
        }
        if (!typeMatches) {
            return false;
        }

        boolean externalTypeMatches = true;
        if (filter.hasExternalLicenseType() && license.getTypeCase() == TypeCase.EXTERNAL) {
            switch (filter.getExternalLicenseType()) {
                case GRAFANA:
                    externalTypeMatches = license.getExternal().getType() == Type.GRAFANA;
                    break;
            }
        }

        if (!externalTypeMatches) {
            return false;
        }

        return true;
    }

    @Override
    public void getLicenses(final GetLicensesRequest request, final StreamObserver<GetLicensesResponse> responseObserver) {
        RPC_RECEIVED_COUNT.labels("getLicenses").increment();
        DataMetricTimer timer = RPC_PROCESSING_MS.labels("getLicenses").startTimer();
        try {
            GetLicensesResponse.Builder respBldr = GetLicensesResponse.newBuilder();
            licenseStore.getLicenses().stream()
                .filter(license -> licenseMatchesFilter(license, request.getFilter()))
                .forEach(respBldr::addLicenseDTO);

            responseObserver.onNext(respBldr.build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
            RPC_ERROR_COUNT.labels("getLicenses").increment();
        }
        timer.observe();
    }

    /**
     * This parameter-free version of getLicenses just grabs the licenses from storage and passes
     * them back. It's intended for use by other objects in the same VM, vs. the GRPC version of
     * the function with the same name.
     *
     * @return a collection of all {@link LicenseDTO} objects currently stored.
     * @throws IOException if there is an error getting the licenses from storage.
     */
    public Collection<LicenseDTO> getLicenses() throws IOException {
        return licenseStore.getLicenses();
    }

    /**
     * Validate the license(s) in the request, and return the licenses w/validation information
     * populated in them.
     *
     * @param request a {@link ValidateLicensesRequest} containing the license(s) to validate.
     * @param responseObserver a {@link ValidateLicensesResponse} containing the same licenses with
     *                         validation errors populated.
     */
    //    @PreAuthorize("hasRole('ADMINISTRATOR')") // pending GRPC auth support in OM-35910
    @Override
    public void validateLicenses(final ValidateLicensesRequest request, final StreamObserver<ValidateLicensesResponse> responseObserver) {
        RPC_RECEIVED_COUNT.labels("validateLicenses").increment();
        logger.info("Validating licenses.");
        try(DataMetricTimer timer = RPC_PROCESSING_MS.labels("validateLicenses").startTimer()) {
            // create LicenseApiDTO objects and use them to validate all licenses
            // We don't validate external licenses - we assume they are valid.
            final InputLicenses requestLicenses = new InputLicenses(request.getLicenseDTOList());

            ValidateLicensesResponse.Builder responseBuilder = ValidateLicensesResponse.newBuilder();
            try {
                // validate the licenses and add them back to the response.
                validateMultipleLicenses(requestLicenses.getTurboLicenses());
                requestLicenses.forEach(responseBuilder::addLicenseDTO);
            } catch (IOException ioe) {
                // IO exception may occur when trying to load licenses to detect duplicates
                // Error out of the call if we get this exception.
                logger.error("IOException while validating {} licenses",
                        requestLicenses.getTurboLicenses().size());
                responseObserver.onError(ioe);
                RPC_ERROR_COUNT.labels("validateLicenses").increment();
                return;
            }

            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
        }
    }

    /**
     * An object to capture licenses in requests, with the turbonomic licenses split from the
     * external licenses.
     */
    private static class InputLicenses {
        private final List<ILicense> turboModelLicenses;
        private final List<LicenseDTO> externalLicenses;

        InputLicenses(Collection<LicenseDTO> inputLicenses) {
            turboModelLicenses = new ArrayList<>();
            externalLicenses = new ArrayList<>();
            inputLicenses.forEach(licenseDTO -> {
                Optional<License> turboLicense = LicenseDTOUtils.licenseDTOtoLicense(licenseDTO);
                if (turboLicense.isPresent()) {
                    turboModelLicenses.add(turboLicense.get());
                } else {
                    externalLicenses.add(licenseDTO);
                }
            });
        }

        List<ILicense> getTurboLicenses() {
            return turboModelLicenses;
        }

        List<LicenseDTO> getExternalLicenses() {
            return externalLicenses;
        }

        int size() {
            return turboModelLicenses.size() + externalLicenses.size();
        }

        void forEach(Consumer<LicenseDTO> licenseDTOConsumer) {
            turboModelLicenses.stream()
                .map(LicenseDTOUtils::iLicenseToLicenseDTO)
                .forEach(licenseDTOConsumer);
            externalLicenses.forEach(licenseDTOConsumer);
        }
    }
    /**
     * Add the licenses specified in the request to storage. Before storing them, this method will
     * first validate the licenses using the same validation code as validateLicenses, and if all
     * pass validation, they will be stored and the original licenses returned. If any fail
     * validation, the licenses will be returned, with validation information populated in them.
     *
     * @param request an {@link AddLicensesRequest} containing the set of licenses to be stored.
     * @param responseObserver the set of licenses, with validation information added if any failed
     *                         validation checks.
     */
    //    @PreAuthorize("hasRole('ADMINISTRATOR')") // pending GRPC auth support in OM-35910
    @Override
    public void addLicenses(final AddLicensesRequest request, final StreamObserver<AddLicensesResponse> responseObserver) {
        RPC_RECEIVED_COUNT.labels("addLicenses").increment();
        logger.info("Adding {} license(s).", request.getLicenseDTOCount());
        try(DataMetricTimer timer = RPC_PROCESSING_MS.labels("addLicenses").startTimer()) {

            // create LicenseApiDTO objects and use them to validate all licenses
            InputLicenses inputLicenses = new InputLicenses(request.getLicenseDTOList());

            try {
                // validate the licenses
                validateMultipleLicenses(inputLicenses.getTurboLicenses());
            } catch (IOException ioe) {
                // IO exception may occur when trying to load licenses to detect duplicates
                // Error out of the call if we get this exception.
                logger.error("IOException while validating {} turbo licenses",
                    inputLicenses.getTurboLicenses().size());
                responseObserver.onError(ioe);
                RPC_ERROR_COUNT.labels("addLicenses").increment();
                return;
            }

            // check if all are valid.
            boolean allAreValid = inputLicenses.getTurboLicenses().stream().allMatch(ILicense::isValid);

            AddLicensesResponse.Builder responseBuilder = AddLicensesResponse.newBuilder();
            // if not all valid, return the validated licenses.
            if ((!allAreValid) || inputLicenses.size() == 0) {
                logger.info("Invalid license or no licenses found, skipping save.");
                inputLicenses.forEach(responseBuilder::addLicenseDTO);
                responseObserver.onNext(responseBuilder.build());
                responseObserver.onCompleted();
                return;
            }
            // otherwise all are valid -- save them
            logger.info("License(s) valid. Saving...");
            for (LicenseDTO licenseDTO : request.getLicenseDTOList()) {
                try {
                    // store this license and add it to the response. (the license may have had a
                    // uuid assigned to it if it didn't have one already)
                    responseBuilder.addLicenseDTO(storeLicense(licenseDTO));
                } catch (IOException ioe) {
                    logger.error("IOException in addLicenses()", ioe);
                    responseObserver.onError(ioe);
                    RPC_ERROR_COUNT.labels("addLicenses").increment();
                    // we don't know if any of the underlying saves succeeded or not -- fire
                    // the added event anyways. No harm in checking.
                    publishEvent(LicenseManagementEventType.LICENSE_ADDED);
                    return;
                }
            }
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();

            int numStored = request.getLicenseDTOCount();
            logger.info("Added {} licenses.", numStored);
            // if any were store, fire a "license added" event.
            if (numStored > 0) {
                publishEvent(LicenseManagementEventType.LICENSE_ADDED);
                systemLicenseNotification("Added new license.", "Successfully added new license");
            }
        }
    }

    /**
     * Validate a collection of license objects. This may modify the incoming licenses by populating
     * their internal error structures with any validation errors found on each.
     *
     * @param licenses the collection of {@link ILicense} objects to validate. This method may modify
     *                 these input licenses, populating them with relevant errors.
     * @return The same licenses, for convenience.
     * @throws IOException if there is a problem loading existing licenses during validation.
     */
    protected Collection<ILicense> validateMultipleLicenses(Collection<ILicense> licenses) throws IOException {
        // we're filtering out expired licenses for the purposes of validating new licenses.
        Collection<ILicense> allNonExpiredTurboLicenses = licenseStore.getLicenses().stream()
                .filter(license -> !LicenseUtil.isExpired(license))
                .map(LicenseDTOUtils::licenseDTOtoLicense)
                // Ignore external licenses.
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList());

        // get a combined feature set across all currently-installed, non-expired licenses. We'll
        // use this to validate that all feature sets are the same (and thus, compatible -- XL does
        // not support licenses with mismatched feature sets).
        Set<String> targetLicenseFeatures = allNonExpiredTurboLicenses.stream()
                .map(ILicense::getFeatures)
                .flatMap(Set::stream)
                .collect(Collectors.toSet());

        // we'll also validate the incoming licenses against each other, so add them to the list too
        allNonExpiredTurboLicenses.addAll(licenses);
        // if the existing feature set is empty, then let's build it again, this time using the
        // incoming licenses. This will let us validate that the incoming licenses are at least
        // compatible with each other.
        if (targetLicenseFeatures.isEmpty()) {
            targetLicenseFeatures = licenses.stream()
                    .map(ILicense::getFeatures)
                    .flatMap(Set::stream)
                    .collect(Collectors.toSet());
        }

        for (ILicense license : licenses) {
            // run the standard validations and set the error reasons on the licenseApiDTO object we
            // are going to be returning. This how the caller gets per-license validation errors.
            LicenseDTOUtils.validateXLLicense(license).forEach(license::addErrorReason);

            // if we have multiple potential licenses, run some other checks to make sure the incoming
            // license is compatible with the other licenses either already in the system or being
            // added.
            if (allNonExpiredTurboLicenses.size() > 0) {
                // check for duplicate license keys. There should be exactly one of this type in the
                // collection.
                long numMatchingKeys = allNonExpiredTurboLicenses.stream()
                        .filter(otherLicense -> otherLicense.getLicenseKey().equals(license.getLicenseKey()))
                        .count();
                if (numMatchingKeys > 1) {
                    // duplicate license error
                    license.addErrorReason(ErrorReason.DUPLICATE_LICENSE);
                }

                // verify that the CWOM/Workload license type matches that of the other licenses.
                boolean isExternalLicense = StringUtils.isNotBlank(license.getExternalLicenseKey());
                boolean incompatibleLicenseType = allNonExpiredTurboLicenses.stream()
                        .anyMatch(otherLicense -> StringUtils.isNotBlank(otherLicense.getExternalLicenseKey())
                                != isExternalLicense);
                if (incompatibleLicenseType) {
                    license.addErrorReason(ErrorReason.INVALID_LICENSE_TYPE_CWOM_ONLY);
                }

                // verify that the feature set for this license is compatible with the other licenses
                if (!LicenseUtil.equalFeatures(license.getFeatures(), targetLicenseFeatures)) {
                    license.addErrorReason(ErrorReason.INVALID_FEATURE_SET);
                }
            }
        }
        return licenses;
    }

    /**
     * Validate a license object, populating it's errors collection with any validation errors found.
     *
     * @param licenseApiDTO the license to validate
     * @return the license w/validation errors populated.
     */
    protected ILicense validateLicense(ILicense licenseApiDTO) throws IOException {
        // we're going to re-use the validateMultipleLicenses method for simplicity.
        validateMultipleLicenses(Collections.singletonList(licenseApiDTO));
        return licenseApiDTO;
    }

    /**
     * Save a turbonomic license in encrypted protobuf format.
     *
     * @param license the license to store. We will derive the key from this license object.
     * @return The actual license DTO that was stored. It may have a uuid assigned to it, if
     * the original license did not have one already.
     */
    private LicenseDTO storeLicense(LicenseDTO license) throws IOException {
        RPC_RECEIVED_COUNT.labels("storeLicense").increment();
        try(DataMetricTimer timer = RPC_PROCESSING_MS.labels("storeLicense").startTimer()) {

            // if no uuid on the license, generate one.
            if (! license.hasUuid()) {
                String uuid = String.valueOf(IdentityGenerator.next());
                logger.info("Assigning uuid {} to license.", uuid);
                // rebuild the license DTO w/uuid added
                license = LicenseDTO.newBuilder(license)
                        .setUuid(uuid)
                        .build();
            }
            logger.info("Storing license {}", license.getUuid());

            licenseStore.storeLicense(license);
            return license;
        }
    }

    /**
     * Removes a license from storage.
     *
     * @param request a {@link RemoveLicenseRequest} containing the uuid of the license to remove.
     * @param responseObserver will recieve a {@link RemoveLicenseResponse} containing a boolean
     *                        indicating whether the license was removed or not.
     */
    //    @PreAuthorize("hasRole('ADMINISTRATOR')") // pending GRPC auth support in OM-35910
    @Override
    public void removeLicense(final RemoveLicenseRequest request, final StreamObserver<RemoveLicenseResponse> responseObserver) {
        RPC_RECEIVED_COUNT.labels("removeLicense").increment();
        logger.info("Removing license {}", request.getUuid());
        try(DataMetricTimer timer = RPC_PROCESSING_MS.labels("removeLicense").startTimer()) {
            if (! request.hasUuid()) {
                throw new IllegalArgumentException("License uuid for removal not specified.");
            }
            String uuid = request.getUuid();
            try {
                boolean wasRemoved = licenseStore.removeLicense(uuid);
                responseObserver.onNext(RemoveLicenseResponse.newBuilder()
                        .setWasRemoved(wasRemoved)
                        .build());
                if (wasRemoved) {
                    publishEvent(LicenseManagementEventType.LICENSE_REMOVED);
                }
                responseObserver.onCompleted();
            } catch (Exception ioe) {
                responseObserver.onError(ioe);
                RPC_ERROR_COUNT.labels("removeLicense").increment();
            }
            logger.info("Successfully removed license {}", uuid);
            systemLicenseNotification("Removed license.", "Successfully removed license");
        }
    }

    /**
     * An enum of {@link LicenseManagementEvent} types that can be sent from the
     * @{link LicenseManagerService}'s event stream.
     */
    public enum LicenseManagementEventType {
        LICENSE_ADDED,
        LICENSE_REMOVED
    }

    /**
     * LicenseManagementEvent is very simple and just wraps a {@link LicenseManagementEventType}.
     * However, we can add event metadata here too, if it ever becomes necessary.
     */
    public static class LicenseManagementEvent {

        final private LicenseManagementEventType eventType;

        public LicenseManagementEvent(LicenseManagementEventType type) {
            eventType = type;
        }

        public LicenseManagementEventType getType() {
            return eventType;
        }
    }

    // Send notification
    private void systemLicenseNotification(@Nonnull final String longDescription,
            @Nonnull final String shortDescription) {
        try {
            notificationSender.sendNotification(
                    Category.newBuilder().setLicense(SystemNotification.License.newBuilder().build())
                            .build(),
                    longDescription,
                    shortDescription,
                    Severity.CRITICAL);
        } catch (CommunicationException | InterruptedException e) {
            logger.error("Error publishing license notification", e);
        }
    }

}
