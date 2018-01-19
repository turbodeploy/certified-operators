package com.vmturbo.api.component.external.api.service;

import java.io.ByteArrayInputStream;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nonnull;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import com.google.common.collect.ImmutableList;

import com.vmturbo.api.component.communication.RestAuthenticationProvider;
import com.vmturbo.api.dto.license.LicenseApiDTO;
import com.vmturbo.api.dto.license.LicenseApiInputDTO;
import com.vmturbo.api.exceptions.ServiceUnavailableException;
import com.vmturbo.api.serviceinterfaces.ILicenseService;
import com.vmturbo.auth.api.usermgmt.AuthUserDTO;

public class LicenseService implements ILicenseService {
    public static final String PATH = "/license";
    /**
     * The HTTP accept header.
     */
    private static final List<MediaType> HTTP_ACCEPT = ImmutableList.of(MediaType.APPLICATION_JSON);
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
                          final @Nonnull RestTemplate restTemplate) {
        authHost_ = Objects.requireNonNull(authHost);
        authPort_ = authPort;
        if (authPort_ < 0 || authPort_ > 65535) {
            throw new IllegalArgumentException("Invalid AUTH port.");
        }
        restTemplate_ = Objects.requireNonNull(restTemplate);
    }


    /**
     * Get license from Auth component, currently it only supports socket based license.
     * @return LicenseApiDTO
     */
    @Override
    public LicenseApiDTO getLicense() {
        LicenseApiDTO license = new LicenseApiDTO();
        UriComponentsBuilder builder = baseRequest().path(PATH);
        String request = builder.build().toUriString();
        HttpHeaders headers = new HttpHeaders();
        headers.setAccept(HTTP_ACCEPT);

        ResponseEntity<String> result;
        try {
            result = restTemplate_.exchange(request, HttpMethod.GET, new HttpEntity<>(headers),
                    String.class);
            String rawLicense = result.getBody();
            if (rawLicense == null || rawLicense.isEmpty()) {
                // when license component is ready in XL, we should return empty LicenseApiDTO.
                return getDefaultXlLicense(); // return the default XL license for now
            }
            return convertSocketLicenseToDTO(rawLicense);
        } catch (RestClientException e) {
            throw new ServiceUnavailableException(AUTH_SERVICE_NOT_AVAILABLE_MSG);
        }
    }

    @Override
    public LicenseApiDTO populateLicense(final LicenseApiInputDTO licenseApiInputDTO) {
        logger_.debug("Populating license.");
        UriComponentsBuilder builder = baseRequest().path(PATH);
        String request = builder.build().toUriString();
        HttpHeaders headers = composeHttpHeaders();
        HttpEntity<LicenseApiInputDTO> entity;
        entity = new HttpEntity<>(licenseApiInputDTO, headers);
        restTemplate_.exchange(request, HttpMethod.POST, entity, LicenseApiInputDTO.class);
        logger_.debug("Done populating license.");
        return getLicense();
    }

    @Override
    public LicenseApiDTO validateLicense() {
        return getLicense();
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
    private static LicenseApiDTO convertSocketLicenseToDTO(String rawLicense) {
        LicenseApiDTO license = new LicenseApiDTO();
        try {
            DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder documentBuilder = documentBuilderFactory.newDocumentBuilder();
            Document document = documentBuilder.parse(new InputSource(new ByteArrayInputStream(rawLicense.getBytes("utf-8"))));

            NodeList featureNodelist = document.getElementsByTagName("feature");
            List<String> featureList = new ArrayList<>();
            for (int i = 0; i < featureNodelist.getLength(); i++) {
                Element sessionElement = (Element) featureNodelist.item(i);
                String featureName = sessionElement.getAttribute("FeatureName");
                featureList.add(featureName);
            }

            license.setLicenseOwner(parse(document, "first-name") + " " + parse(document, "last-name"));
            license.setEmail(parse(document, "email"));
            license.setExpirationDate(parse(document, "expiration-date"));
            license.setNumSocketsInUse(0);

            license.setFeatures(featureList);

            license.setNumSocketsLicensed(Integer.parseInt(parse(document, "num-sockets")));
            license.setIsValid(true);
            return license;
        } catch (Exception e) {
            return license;
        }
    }

    // For end to end test only, will be replaced with real implementation.
    private static String parse(Document document, String tag) {
        NodeList list = document.getElementsByTagName(tag);
        Element element = (Element) list.item(0);
        return element.getFirstChild().getNodeValue();
    }

    private LicenseApiDTO getDefaultXlLicense() {
        LicenseApiDTO license = new LicenseApiDTO();

        final Date expirationDate = Date.from(EXPIRATION_DATE
                .atStartOfDay(ZoneId.systemDefault())
                .toInstant());

        license.setLicenseOwner("Turbonomic XL");
        license.setEmail("");
        license.setExpirationDate(new SimpleDateFormat("MMM dd yyyy").format(expirationDate));

        license.setFeatures(Arrays.asList(
                "historical_data",
                "multiple_vc",
                "scoped_user_view",
                "customized_views",
                "group_editor",
                "vmturbo_api",
                "automated_actions",
                "active_directory",
                "custom_reports",
                "planner",
                "optimizer",
                "full_policy",
                "deploy",
                "aggregation",
                "action_script",
                "fabric",
                "cloud_targets",
                "cluster_flattening",
                "cluster_flattening",
                "network_control",
                "cluster_flattening",
                "storage",
                "vdi_control",
                "public_cloud",
                "container_control",
                "applications",
                "loadbalancer",
                "app_control"
        ));

        license.setNumSocketsLicensed(Integer.MAX_VALUE);
        license.setNumSocketsInUse(0);
        license.setIsValid(true);
        license.setExpired(LocalDate.now().isAfter(EXPIRATION_DATE));

        return license;
    }
}
