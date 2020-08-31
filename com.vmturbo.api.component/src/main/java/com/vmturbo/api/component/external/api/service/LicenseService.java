package com.vmturbo.api.component.external.api.service;

import java.time.LocalDate;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.Empty;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import com.vmturbo.api.component.communication.RestAuthenticationProvider;
import com.vmturbo.api.component.external.api.mapper.LicenseMapper;
import com.vmturbo.api.dto.license.LicenseApiDTO;
import com.vmturbo.api.exceptions.UnauthorizedObjectException;
import com.vmturbo.api.serviceinterfaces.ILicenseService;
import com.vmturbo.auth.api.auditing.AuditAction;
import com.vmturbo.auth.api.auditing.AuditLog;
import com.vmturbo.auth.api.usermgmt.AuthUserDTO;
import com.vmturbo.common.protobuf.licensing.LicenseCheckServiceGrpc.LicenseCheckServiceBlockingStub;
import com.vmturbo.common.protobuf.licensing.LicenseManagerServiceGrpc.LicenseManagerServiceBlockingStub;
import com.vmturbo.common.protobuf.licensing.Licensing.AddLicensesRequest;
import com.vmturbo.common.protobuf.licensing.Licensing.GetLicenseRequest;
import com.vmturbo.common.protobuf.licensing.Licensing.GetLicenseResponse;
import com.vmturbo.common.protobuf.licensing.Licensing.GetLicenseSummaryResponse;
import com.vmturbo.common.protobuf.licensing.Licensing.GetLicensesResponse;
import com.vmturbo.common.protobuf.licensing.Licensing.LicenseDTO;
import com.vmturbo.common.protobuf.licensing.Licensing.RemoveLicenseRequest;
import com.vmturbo.common.protobuf.licensing.Licensing.ValidateLicensesRequest;
import com.vmturbo.licensing.utils.LicenseDeserializer;

public class LicenseService implements ILicenseService {
    public static final String PATH = "/license";
    /**
     * The HTTP accept header.
     */
    private static final List<MediaType> HTTP_ACCEPT = ImmutableList.of(MediaType.APPLICATION_JSON);
    private static final String FAILED_TO_PARSE_POTENTIAL_MALICIOUS_LICENSE_FILE =
            "Failed to parse potential malicious license file";
    /**
     * The logger.
     */
    private final Logger logger_ = LogManager.getLogger();
    /**
     * The synchronous client-side HTTP access.
     */
    private final RestTemplate restTemplate_;
    /**
     * The auth service host.
     */
    private final String authHost_;
    /**
     * The auth service port.
     */
    private final int authPort_;

    /**
     * The License Manager Service stub.
     */
    private final LicenseManagerServiceBlockingStub licenseManagerService;

    private final LicenseCheckServiceBlockingStub licenseCheckService;

    /**
     * The error message returned if the Auth component is not running.
     */
    private static final String AUTH_SERVICE_NOT_AVAILABLE_MSG =
            "The Authorization Service is not responding";

    final LocalDate EXPIRATION_DATE = LocalDate.of(2019, 1, 1);

    /**
     * Constructs the users service.
     *
     * @param authHost     The authentication host.
     * @param authPort     The authentication port.
     * @param restTemplate The synchronous client-side HTTP access.
     */
    public LicenseService(final @Nonnull String authHost,
                          final int authPort,
                          final @Nonnull RestTemplate restTemplate,
                          final @Nonnull LicenseManagerServiceBlockingStub licenseManagerService,
                          final @Nonnull LicenseCheckServiceBlockingStub licenseCheckService) {
        authHost_ = Objects.requireNonNull(authHost);
        authPort_ = authPort;
        if (authPort_ < 0 || authPort_ > 65535) {
            throw new IllegalArgumentException("Invalid AUTH port.");
        }
        restTemplate_ = Objects.requireNonNull(restTemplate);
        this.licenseManagerService = licenseManagerService;
        this.licenseCheckService = licenseCheckService;
    }


    /**
     * Composes the HTTP headers for REST calls.
     *
     * @return The HTTP headers.
     */
    private HttpHeaders composeHttpHeaders() {
        HttpHeaders headers = new HttpHeaders();
        headers.setAccept(HTTP_ACCEPT);
        headers.set(RestAuthenticationProvider.AUTH_HEADER_NAME, getLoggedInPrincipal().getToken());
        return headers;
    }

    /**
     * Returns the logged in principal.
     *
     * @return The logged in user.
     */
    private @Nonnull AuthUserDTO getLoggedInPrincipal() {
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        if (auth != null && auth.getPrincipal() instanceof AuthUserDTO) {
            return (AuthUserDTO) auth.getPrincipal();
        } else {
            throw new IllegalStateException("No user logged in!");
        }
    }

    /**
     * Builds base AUTH REST request.
     *
     * @return The base AUTH REST request.
     */
    private @Nonnull
    UriComponentsBuilder baseRequest() {
        return UriComponentsBuilder.newInstance().scheme("http").host(authHost_).port(authPort_);
    }

    // For end to end test only, will be replaced with real implementation.
    private static String parse(Document document, String tag) {
        NodeList list = document.getElementsByTagName(tag);
        Element element = (Element) list.item(0);
        return element.getFirstChild().getNodeValue();
    }

    @Override
    public Optional<LicenseApiDTO> readLicense(final String s) {
        GetLicenseResponse response = licenseManagerService.getLicense(
                GetLicenseRequest.newBuilder()
                        .setUuid(s)
                        .build());
        if (response.hasLicenseDTO()) {
            LicenseApiDTO license = LicenseMapper.licenseDTOtoLicenseApiDTO(response.getLicenseDTO());
            return Optional.of(license);
        }
        return Optional.empty();
    }

    @Override
    public List<LicenseApiDTO> addLicenses(final List<LicenseInput> list, final boolean dryRun) {
        // filter out duplicate licenses in the request. I don't know how/when a duplicate license
        // gets into the request, but this check exists in ops manager so I'm carrying it into XL.
        List<LicenseDTO> licenseDTOS = list.stream()
            .map(licenseInput -> LicenseDeserializer.deserialize(licenseInput.getLicense(), licenseInput.getFilename()))
            .distinct()
            .collect(Collectors.toList());

        List<LicenseDTO> processedLicenseDTOS;
        // if it's a "dry run", we will only validate the licenses and skip the storage part. The
        // UI passes this in to show the user validation results first, then will call this method a
        // second time with dryRun = false to perform the actual storage.
        if (dryRun) {
            // ask for validation only
            processedLicenseDTOS = licenseManagerService.validateLicenses(
                    ValidateLicensesRequest.newBuilder()
                            .addAllLicenseDTO(licenseDTOS)
                            .build())
                        .getLicenseDTOList();
        } else {
            // validate and save
            processedLicenseDTOS = licenseManagerService.addLicenses(
                    AddLicensesRequest.newBuilder()
                            .addAllLicenseDTO(licenseDTOS)
                            .build())
                    .getLicenseDTOList();
            final String licenseFileName = licenseDTOS.stream()
                .map(LicenseDTO::getFilename)
                .findFirst()
                .orElse("");
            AuditLog.newEntry(AuditAction.ADD_LICENSE,
                "Turbonomic license added", true)
                .targetName(licenseFileName)
                .audit();
        }

        // return the set of licenses that were added
        List<LicenseApiDTO> addedLicenses = processedLicenseDTOS.stream()
                                        .map(LicenseMapper::licenseDTOtoLicenseApiDTO)
                                        .collect(Collectors.toList());
        return addedLicenses;
    }

    @Override
    public List<LicenseApiDTO> findAllLicenses() {
        GetLicensesResponse response = licenseManagerService.getLicenses(Empty.getDefaultInstance());
        List<LicenseApiDTO> allLicenses = response.getLicenseDTOList().stream()
                                            .map(LicenseMapper::licenseDTOtoLicenseApiDTO)
                                            .collect(Collectors.toList());
        return allLicenses;
    }

    @Override
    public boolean deleteLicense(final String s) {
        logger_.info("Deleting license {}", s);
        final boolean isSuccessful =  licenseManagerService.removeLicense(RemoveLicenseRequest.newBuilder()
                .setUuid(s)
                .build()).getWasRemoved();
        AuditLog.newEntry(AuditAction.DELETE_LICENSE,
            "Turbonomic license deletion", isSuccessful)
            .targetName(s)
            .audit();
        return isSuccessful;
    }

    @Override
    public LicenseApiDTO getLicensesSummary() throws UnauthorizedObjectException {
        GetLicenseSummaryResponse response = licenseCheckService.getLicenseSummary(Empty.getDefaultInstance());
        if (response.hasLicenseSummary()) {
            // create a licenseApiDTO from the LicenseSummary.
            return LicenseMapper.licenseSummaryToLicenseApiDTO(response.getLicenseSummary());
        }
        // no summary yet -- return a blank object.
        return new LicenseApiDTO();
    }
}
