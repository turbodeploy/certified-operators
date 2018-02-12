package com.vmturbo.api.component.external.api.service;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

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
import com.google.common.collect.Lists;

import com.vmturbo.api.component.communication.RestAuthenticationProvider;
import com.vmturbo.api.component.external.api.util.ApiUtils;
import com.vmturbo.api.dto.license.LicenseApiDTO;
import com.vmturbo.api.exceptions.ServiceUnavailableException;
import com.vmturbo.api.exceptions.UnauthorizedObjectException;
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

    private LicenseApiDTO getDefaultXlLicense() {
        LicenseApiDTO license = new LicenseApiDTO();

        final Date expirationDate = Date.from(EXPIRATION_DATE
                .atStartOfDay(ZoneId.systemDefault())
                .toInstant());

        license.setLicenseOwner("Turbonomic XL");
        license.setEmail("");
        license.setExpirationDate(new SimpleDateFormat("MMM dd yyyy").format(expirationDate));
        return license;
    }

    @Override
    public Optional<LicenseApiDTO> readLicense(final String s) {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public List<LicenseApiDTO> addLicenses(final List<LicenseApiDTO> list, final boolean b) {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public List<LicenseApiDTO> findAllLicenses() {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public boolean deleteLicense(final String s) {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public LicenseApiDTO deserializeLicenseToLicenseDTO(final InputStream inputStream, final String s) {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public LicenseApiDTO deserializeLicenseToLicenseDTO(final String s) {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public LicenseApiDTO getLicensesSummary() throws UnauthorizedObjectException {
        return getDefaultXlLicense();
    }
}
