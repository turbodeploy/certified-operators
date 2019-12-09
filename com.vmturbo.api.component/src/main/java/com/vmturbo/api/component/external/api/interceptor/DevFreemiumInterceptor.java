package com.vmturbo.api.component.external.api.interceptor;

import javax.annotation.Nonnull;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.web.servlet.HandlerInterceptor;
import org.springframework.web.servlet.ModelAndView;

import com.vmturbo.auth.api.licensing.LicenseCheckClient;

/**
 * This interceptor is used to validate if a request is allowed if the
 * current license is a developer freemium license.
 */
public class DevFreemiumInterceptor implements HandlerInterceptor {

    private static final Logger logger = LogManager.getLogger();

    private final LicenseCheckClient licenseCheckClient;

    public DevFreemiumInterceptor(@Nonnull LicenseCheckClient licenseCheckClient) {
        this.licenseCheckClient = licenseCheckClient;
    }

    /**
     * Check if the current request is allowed.
     *
     * @return true if the current request is allowed; false if the current request
     * is not allowed for developer freemium edition.
     */
    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response,
                             Object handler) throws Exception {
        if (licenseCheckClient.isDevFreemium()) {
            logger.debug("Dev freemium license. Skip " + request.getMethod() + " " + request.getPathInfo());
            return false;
        }
        return true;
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
}
