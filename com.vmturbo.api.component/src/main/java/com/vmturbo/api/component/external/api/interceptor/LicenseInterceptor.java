package com.vmturbo.api.component.external.api.interceptor;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

import javax.annotation.Nonnull;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;

import org.apache.http.entity.ContentType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.http.HttpStatus;
import org.springframework.web.servlet.HandlerInterceptor;
import org.springframework.web.servlet.ModelAndView;

import com.vmturbo.api.dto.ErrorApiDTO;
import com.vmturbo.auth.api.licensing.LicenseCheckClient;
import com.vmturbo.platform.sdk.common.util.ProbeLicense;

/**
 * This interceptor is used to validate license for every request before further execution. It
 * contains three methods:
 * "preHandle": Check license, which is called before methods in every controller
 * "postHandle" and "afterCompletion": not implemented since license is already checked in preHandle
 */
public class LicenseInterceptor implements HandlerInterceptor {

    private static final Logger logger = LogManager.getLogger();

    @VisibleForTesting
    static final String API_COMPONENT_IS_NOT_READY = "API component is not ready";

    private final LicenseCheckClient licenseCheckClient;

    // may contain a set of license features that would all be required to be available in the license
    // in order to get access to the resource.
    private final Collection<ProbeLicense> requiredFeatures;

    /**
     * Construct a license interceptor that checks for the presence of a valid license before granting
     * access to the protected resources.
     * @param licenseCheckClient the license check client to use.
     */
    public LicenseInterceptor(@Nonnull LicenseCheckClient licenseCheckClient) {
        this.licenseCheckClient = licenseCheckClient;
        this.requiredFeatures = Collections.emptySet();
    }

    /**
     * Construct a license interceptor that, in addition to checking license validity, also checks
     * that a set of features is available before granting access to the protected resources.
     * @param licenseCheckClient the license check client to make checks with.
     * @param requiredFeatures the set of {@link ProbeLicense} that are required to access the
     *                         resources protected by this interceptor.
     */
    public LicenseInterceptor(@Nonnull LicenseCheckClient licenseCheckClient, @Nonnull Collection<ProbeLicense> requiredFeatures) {
        this.licenseCheckClient = licenseCheckClient;
        this.requiredFeatures = requiredFeatures;
    }

    /**
     * Check if current license is valid before continuing every request. If license is invalid,
     * it will stop execution chain and return it.
     *
     * @return true if license is valid and continue the handler execution chain; false if license
     * is invalid and it will send error response.
     */
    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response,
                             Object handler) throws Exception {
        if (licenseCheckClient.isReady()) {
            if (licenseCheckClient.hasValidLicense()) {
                if (!requiredFeatures.isEmpty()) {
                    // if the interceptor also includes a set of required features, check them here.
                    if (!licenseCheckClient.areFeaturesAvailable(requiredFeatures)) {
                        sendMissingLicenseFeaturesResponse(response, requiredFeatures);
                        return false;
                    }
                }
                return true;
            }
            logger.warn("Invalid license: " + request.getMethod() + " " + request.getPathInfo());
            sendInvalidLicenseErrorResponse(response);
        } else {
            sendLicenseSummaryNotAvailableErrorResponse(response);
            logger.debug("License summary is not available.");
        }
        return false;
    }

    @Override
    public void postHandle(HttpServletRequest request, HttpServletResponse response,
                           Object handler, ModelAndView modelAndView) throws Exception {
        // nothing to do
    }

    @Override
    public void afterCompletion(HttpServletRequest request, HttpServletResponse response,
                                Object handler, Exception ex) throws Exception {
        // nothing to do
    }

    /**
     * Send the invalid license error DTO in the response.
     */
    private void sendMissingLicenseFeaturesResponse(final HttpServletResponse response, Collection<ProbeLicense> features) throws IOException {
        StringBuilder sbMessage = new StringBuilder("Requires an active license with the following features: ");
        features.forEach(feature -> sbMessage.append(feature.getKey()).append(" "));
        sendErrorResponse(response, sbMessage.toString());
    }


    /**
     * Send the invalid license error DTO in the response.
     */
    private void sendInvalidLicenseErrorResponse(final HttpServletResponse response) throws IOException {
        sendErrorResponse(response, "Invalid license");
    }

    /**
     * Set API component is not ready DTO in the user response. Internally, it's license summary is not
     * available.
     */
    private void sendLicenseSummaryNotAvailableErrorResponse(final HttpServletResponse response) throws IOException {
        // create error dto
        sendErrorResponse(response, API_COMPONENT_IS_NOT_READY);
    }

    private void sendErrorResponse(@Nonnull final HttpServletResponse response,
                                   @Nonnull final String errorMessage) throws IOException {
        ErrorApiDTO error = new ErrorApiDTO();
        error.setMessage(errorMessage);
        error.setType(HttpStatus.FORBIDDEN.value());
        response.setStatus(HttpStatus.FORBIDDEN.value());
        response.setContentType(ContentType.APPLICATION_JSON.toString());
        new ObjectMapper().writeValue(response.getWriter(), error);
    }
}
