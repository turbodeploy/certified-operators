package com.vmturbo.components.common;

import javax.servlet.http.HttpServletRequest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.web.filter.AbstractRequestLoggingFilter;

/**
 * A bean that logs REST requests to the API component.
 */
public class LoggingFilter extends AbstractRequestLoggingFilter {

    private Logger logger = LogManager.getLogger();

    public LoggingFilter() {
        super();
        setIncludeQueryString(true);
        setIncludePayload(true);
    }

    @Override
    protected void beforeRequest(final HttpServletRequest request, final String message) {
         if (logger.isDebugEnabled()) {
            logger.debug("Starting request processing to URI: {}", request.getRequestURI());
        }
    }

    @Override
    protected void afterRequest(final HttpServletRequest request, final String message) {
        if (logger.isDebugEnabled()) {
            logger.debug("Done request processing to URI: {}", request.getRequestURI());
        }
    }
}
