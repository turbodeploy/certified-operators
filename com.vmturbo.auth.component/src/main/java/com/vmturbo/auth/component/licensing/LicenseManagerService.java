package com.vmturbo.auth.component.licensing;

import static com.vmturbo.auth.component.licensing.LicenseDTOUtils.licenseDTOtoLicense;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.protobuf.Empty;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.grpc.stub.StreamObserver;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

import com.vmturbo.api.dto.license.ILicense;
import com.vmturbo.api.dto.license.ILicense.ErrorReason;
import com.vmturbo.auth.component.licensing.store.ILicenseStore;
import com.vmturbo.common.protobuf.licensing.LicenseManagerServiceGrpc.LicenseManagerServiceImplBase;
import com.vmturbo.common.protobuf.licensing.Licensing.AddLicensesRequest;
import com.vmturbo.common.protobuf.licensing.Licensing.AddLicensesResponse;
import com.vmturbo.common.protobuf.licensing.Licensing.GetLicenseRequest;
import com.vmturbo.common.protobuf.licensing.Licensing.GetLicenseResponse;
import com.vmturbo.common.protobuf.licensing.Licensing.GetLicensesResponse;
import com.vmturbo.common.protobuf.licensing.Licensing.LicenseDTO;
import com.vmturbo.common.protobuf.licensing.Licensing.RemoveLicenseRequest;
import com.vmturbo.common.protobuf.licensing.Licensing.RemoveLicenseResponse;
import com.vmturbo.common.protobuf.licensing.Licensing.ValidateLicensesRequest;
import com.vmturbo.common.protobuf.licensing.Licensing.ValidateLicensesResponse;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.licensing.utils.LicenseUtil;
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

    // reference to the storage system used to persist the licenses.
    private ILicenseStore licenseStore;

    // The flux and fluxsink are used to broadcast events when licenses are added/removed. License
    // Check Service will be listening for these.
    private Flux<LicenseManagementEvent> eventFlux;
    private FluxSink<LicenseManagementEvent> eventFluxSink;

    public LicenseManagerService(@Nonnull ILicenseStore licenseStore) {
        this.licenseStore = licenseStore;
        // create the event flux
        eventFlux = Flux.create(emitter -> eventFluxSink = emitter);
        // start publishing immediately w/o waiting for a consumer to signal demand.
        // Future subscribers will pick up on future statuses
        eventFlux.publish().connect();

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
    @Override
    public void getLicenses(final Empty request, final StreamObserver<GetLicensesResponse> responseObserver) {
        RPC_RECEIVED_COUNT.labels("getLicenses").increment();
        DataMetricTimer timer = RPC_PROCESSING_MS.labels("getLicenses").startTimer();
        try {
            // get stored licenses in xml form for transfer
            responseObserver.onNext(GetLicensesResponse.newBuilder()
                    .addAllLicenseDTO(licenseStore.getLicenses())
                    .build());
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
            ValidateLicensesResponse.Builder responseBuilder = ValidateLicensesResponse.newBuilder();
            for (LicenseDTO licenseDTO : request.getLicenseDTOList()) {
                try {
                    // convert to a License object and validate.
                    ILicense license = validateLicense(licenseDTOtoLicense(licenseDTO));
                    // add the resulting license to the response.
                    responseBuilder.addLicenseDTO(LicenseDTOUtils.iLicenseToLicenseDTO(license));

                } catch (IOException ioe) {
                    // IO exception may occur when trying to load licenses to detect duplicates
                    // Error out of the call if we get this exception.
                    logger.error("IOException while validating license {}", licenseDTO.getFilename());
                    responseObserver.onError(ioe);
                    RPC_ERROR_COUNT.labels("validateLicenses").increment();
                    return;
                }
            }
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
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
            // first validate the licenses and put the validated licenses into the response.
            boolean allAreValid = true;
            // collect the validated licenses in case we have to return them with errors.
            List<ILicense> validatedLicenses = new ArrayList();
            for (LicenseDTO licenseDTO : request.getLicenseDTOList()) {
                try {
                    // convert to a LicenseApi object, validate, and add to the responses
                    ILicense license = validateLicense(licenseDTOtoLicense(licenseDTO));
                    allAreValid = allAreValid && license.isValid();
                    validatedLicenses.add(license);
                } catch (IOException ioe) {
                    // IO exception may occur when trying to load licenses to detect duplicates
                    // Error out of the call if we get this exception.
                    logger.error("IOException while validating license {}", licenseDTO.getFilename());
                    responseObserver.onError(ioe);
                    RPC_ERROR_COUNT.labels("addLicenses").increment();
                    return;
                }
            }

            AddLicensesResponse.Builder responseBuilder = AddLicensesResponse.newBuilder();
            // if not all valid, return the validated licenses.
            if ((! allAreValid) || validatedLicenses.size() == 0) {
                logger.info("Invalid license or no licenses found, skipping save.");
                responseBuilder.addAllLicenseDTO(validatedLicenses.stream()
                            .map(LicenseDTOUtils::iLicenseToLicenseDTO)
                            .collect(Collectors.toList()));
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
            }
        }
    }

    /**
     * Validate a license object, populating it's errors collection with any validation errors found.
     *
     * @param licenseApiDTO the license to validate
     * @return the license w/validation errors populated.
     */
    private ILicense validateLicense(ILicense licenseApiDTO) throws IOException {
        // run the standard validations and set the error reasons on the licenseApiDTO object we
        // are going to be returning. This how the caller gets per-license validation errors.
        LicenseDTOUtils.validateXLLicense(licenseApiDTO)
                .forEach(licenseApiDTO::addErrorReason); // add all error reasons to the license

        // check for duplicate license keys and compatibility
        String newLicenseKey = licenseApiDTO.getLicenseKey();
        Collection<LicenseDTO> existingLicenses = licenseStore.getLicenses();
        boolean alreadyExists = existingLicenses.stream()
                .anyMatch(license -> license.getLicenseKey().equals(newLicenseKey));
        if (alreadyExists) {
            // duplicate license error
            licenseApiDTO.addErrorReason(ErrorReason.DUPLICATE_LICENSE);
        }

        // if we have existing licenses, run some other checks to make sure the incoming license is
        // compatible with the other licenses in the system.
        if (existingLicenses.size() > 0) {
            // verify that CWOM/Workload license type matches our existing licenses.
            boolean isExternalLicense = StringUtils.isNotBlank(licenseApiDTO.getExternalLicenseKey());
            boolean sameLicenseType = existingLicenses.stream()
                    .anyMatch(license -> (license.hasExternalLicenseKey() == isExternalLicense));
            if (!sameLicenseType) {
                licenseApiDTO.addErrorReason(ErrorReason.INVALID_LICENSE_TYPE_CWOM_ONLY);
            }

            // validate that the feature set in the new license matches the features in the
            // existing licenses.
            // we are merging all existing license features here, but technically this shouldn't be
            // necessary, since we are marking licenses with non-matching feature sets as invalid.
            // So all added licenses should have the same feature sets.
            List<String> existingFeatures = existingLicenses.stream()
                    .map(LicenseDTO::getFeaturesList)
                    .flatMap(List::stream)
                    .distinct()
                    .collect(Collectors.toList());
            if (!LicenseUtil.equalFeatures(licenseApiDTO.getFeatures(), existingFeatures)) {
                licenseApiDTO.addErrorReason(ErrorReason.INVALID_FEATURE_SET);
            }
        }

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
}
