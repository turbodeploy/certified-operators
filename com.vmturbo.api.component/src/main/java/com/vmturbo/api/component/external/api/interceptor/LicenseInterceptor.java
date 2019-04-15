package com.vmturbo.api.component.external.api.interceptor;

import java.io.IOException;

import javax.annotation.Nonnull;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.http.entity.ContentType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.http.HttpStatus;
import org.springframework.web.servlet.HandlerInterceptor;
import org.springframework.web.servlet.ModelAndView;

import com.fasterxml.jackson.databind.ObjectMapper;

import com.vmturbo.api.dto.ErrorApiDTO;
import com.vmturbo.auth.api.licensing.LicenseCheckClient;

/**
 * This interceptor is used to validate license for every request before further execution. It
 * contains three methods:
 * "preHandle": Check license, which is called before methods in every controller
 * "postHandle" and "afterCompletion": not implemented since license is already checked in preHandle
 */
public class LicenseInterceptor implements HandlerInterceptor {

    private static final Logger logger = LogManager.getLogger();

    private final LicenseCheckClient licenseCheckClient;

    public LicenseInterceptor(@Nonnull LicenseCheckClient licenseCheckClient) {
        this.licenseCheckClient = licenseCheckClient;
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
        if(licenseCheckClient.hasValidLicense()) {
            return true;
        }

        logger.warn("Invalid license: " + request.getMethod() + " " + request.getPathInfo());
        sendInvalidLicenseErrorResponse(response);
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
    private void sendInvalidLicenseErrorResponse(final HttpServletResponse response) throws IOException {
        // create error dto
        ErrorApiDTO error = new ErrorApiDTO();
        error.setMessage("Invalid license");
        error.setType(HttpStatus.FORBIDDEN.value());
        response.setStatus(HttpStatus.FORBIDDEN.value());
        response.setContentType(ContentType.APPLICATION_JSON.toString());
        new ObjectMapper().writeValue(response.getWriter(), error);
    }
}
