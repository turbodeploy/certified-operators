package com.vmturbo.auth.component.handler;


import javax.annotation.Nonnull;
import javax.servlet.http.HttpServletRequest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;

import com.google.common.base.Throwables;

import com.vmturbo.api.dto.ErrorApiDTO;
import com.vmturbo.auth.api.authentication.AuthenticationException;

/**
 * Any exception thrown by the Controller, or implementing service will be repackaged as
 * an ErrorApiDTO depending on the type of exception thrown. It hooks into to controller
 * through Spring's {@link ControllerAdvice} annotation.
 */
@ControllerAdvice
public class GlobalExceptionHandler {

    private static final Logger logger = LogManager.getLogger();

    @ExceptionHandler(AuthenticationException.class)
    @ResponseBody
    public ResponseEntity<ErrorApiDTO> handleException(HttpServletRequest req, AuthenticationException ex) {
        final ResponseStatus responseStatus = ex.getClass().getAnnotation(ResponseStatus.class);
        // API component is expecting HttpServerErrorException for authenticationExcetion, so
        // setting HttpStatus as INTERNAL_SERVER_ERROR
        return createErrorDTO(req, ex, responseStatus != null ?
            responseStatus.value() : HttpStatus.INTERNAL_SERVER_ERROR);
    }

    private ResponseEntity<ErrorApiDTO> createErrorDTO(@Nonnull final HttpServletRequest req,
                                                         @Nonnull final Exception ex,
                                                         @Nonnull final HttpStatus statusCode) {
        logger.error(ex.getMessage());
        ErrorApiDTO err = new ErrorApiDTO(logger.isDebugEnabled(), statusCode.value(),
            ex.toString(), Throwables.getRootCause(ex).getMessage(), ex.getStackTrace());
        return new ResponseEntity<>(err, statusCode);
    }
}