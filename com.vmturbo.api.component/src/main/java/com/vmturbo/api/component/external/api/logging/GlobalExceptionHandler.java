package com.vmturbo.api.component.external.api.logging;

import java.io.FileNotFoundException;
import java.util.regex.PatternSyntaxException;

import javax.annotation.Nonnull;
import javax.servlet.http.HttpServletRequest;
import javax.validation.ConstraintViolation;
import javax.validation.ConstraintViolationException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Throwables;

import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.validation.BindingResult;
import org.springframework.validation.ObjectError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.MissingServletRequestParameterException;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.client.HttpClientErrorException;

import com.vmturbo.api.dto.ErrorApiDTO;
import com.vmturbo.api.exceptions.InvalidOperationException;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.api.exceptions.ServiceUnavailableException;
import com.vmturbo.api.exceptions.UnauthorizedObjectException;
import com.vmturbo.api.validators.settings.OsMigrationManagerInputException;

/**
 * Any exception thrown by the Controller, or implementing service will be repackaged as
 * an ErrorApiDTO depending on the type of exception thrown. It hooks into to controller
 * through Spring's {@link ControllerAdvice} annotation.
 * <p>
 * XL note: this class was copied from classic (6.4) and the only changes are using log4j2 logger
 * instead of log4j. Because XL uses log4j2 logger. Upgrading the logger to log4j2, so this logger's
 * logging level can be controlled by XL log configuration service.
 *</p>
 */
@ControllerAdvice
public class GlobalExceptionHandler {

    private static final Logger logger = LogManager.getLogger("com.vmturbo.api.handler");

    public static void handleValidation(BindingResult result) {
        if (result.hasErrors()) {
            String errMsg = "";
            for (ObjectError error : result.getAllErrors()) {
                errMsg += ". " + error.getDefaultMessage();
            }
            throw new IllegalArgumentException(errMsg.substring(2));
        }
    }

    @ExceptionHandler(Exception.class)
    @ResponseBody
    public ResponseEntity<ErrorApiDTO> handleException(HttpServletRequest req, Exception ex) {
        final ResponseStatus responseStatus = ex.getClass()
                .getAnnotation(ResponseStatus.class);
        return createErrorDTO(req, ex,
                responseStatus != null ? responseStatus.value() : HttpStatus.INTERNAL_SERVER_ERROR);
    }

    @ExceptionHandler(HttpMessageNotReadableException.class)
    @ResponseBody
    public ResponseEntity<ErrorApiDTO> handleHttpMessageNotReadableException(HttpServletRequest req, HttpMessageNotReadableException ex) {
        return createErrorDTO(req, ex, HttpStatus.BAD_REQUEST);
    }

    @ExceptionHandler(PatternSyntaxException.class)
    @ResponseBody
    public ResponseEntity<ErrorApiDTO> handlePatternSyntaxException(HttpServletRequest req, PatternSyntaxException ex) {
        return createErrorDTO(req, ex, HttpStatus.BAD_REQUEST);
    }

    @ExceptionHandler(ClassCastException.class)
    @ResponseBody
    public ResponseEntity<ErrorApiDTO> handleClassCastException(HttpServletRequest req, ClassCastException ex) {
        return createErrorDTO(req, ex, HttpStatus.BAD_REQUEST);
    }

    @ExceptionHandler(MissingServletRequestParameterException.class)
    @ResponseBody
    public ResponseEntity<ErrorApiDTO> handleMissingServletRequestParameterException(HttpServletRequest req, MissingServletRequestParameterException ex) {
        return createErrorDTO(req, ex, HttpStatus.BAD_REQUEST);
    }

    @ExceptionHandler(NumberFormatException.class)
    @ResponseBody
    public ResponseEntity<ErrorApiDTO> handleNumberFormatException(HttpServletRequest req, NumberFormatException ex) {
        return createErrorDTO(req, ex, HttpStatus.BAD_REQUEST);
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    @ResponseBody
    public ResponseEntity<ErrorApiDTO> handleMethodArgumentNotValidException(HttpServletRequest req, MethodArgumentNotValidException ex) {
        return createErrorDTO(req, ex, HttpStatus.BAD_REQUEST);
    }

    @ExceptionHandler(IllegalArgumentException.class)
    @ResponseBody
    public ResponseEntity<ErrorApiDTO> handleIllegalArgumentException(HttpServletRequest req, IllegalArgumentException ex) {
        return createErrorDTO(req, ex, HttpStatus.BAD_REQUEST);
    }

    @ExceptionHandler(FileNotFoundException.class)
    @ResponseBody
    public ResponseEntity<ErrorApiDTO> handleFileNotFoundException(HttpServletRequest req, FileNotFoundException ex) {
        return createErrorDTO(req, ex, HttpStatus.BAD_REQUEST);
    }

    /**
     * Handle the {@link InvalidOperationException} by creating an error dto to return to client.
     *
     * @param req http request
     * @param ex the exception to handle
     * @return spring {@link ResponseEntity} wrapping around the error dto
     */
    @ExceptionHandler(InvalidOperationException.class)
    @ResponseBody
    public ResponseEntity<ErrorApiDTO> handleInvalidOperationException(HttpServletRequest req, InvalidOperationException ex) {
        return createErrorDTO(req, ex, HttpStatus.BAD_REQUEST);
    }

    /**
     * Extracts the message from ConstraintViolation from the ConstraintViolationException
     * and returns it into the ResponseEntity.
     *
     * @param req The request that caused the error.
     * @param ex The exception containing the {@link ConstraintViolation} to extract caused
     *           by the request.
     * @return A response packaging the constraint violation for the request.
     */
    @ExceptionHandler(ConstraintViolationException.class)
    @ResponseBody
    public ResponseEntity<ErrorApiDTO> handleConstraintViolationException(HttpServletRequest req, ConstraintViolationException ex) {
        ErrorApiDTO err = new ErrorApiDTO(
                        logger.isDebugEnabled(),
                        HttpStatus.BAD_REQUEST.value(),
                        ex.toString(),
                        // the error message is hidden in the ConstraintViolation in ConstraintViolationException
                        ex.getConstraintViolations().stream()
                            .map(ConstraintViolation::getMessage).findAny().orElse(""),
                        ex.getStackTrace());
        return new ResponseEntity<>(err, HttpStatus.BAD_REQUEST);
    }

    @ExceptionHandler(OsMigrationManagerInputException.class)
    @ResponseBody
    public ResponseEntity<ErrorApiDTO> handleOsMigrationInputException(HttpServletRequest req, OsMigrationManagerInputException ex) {
        return createErrorDTO(req, ex, HttpStatus.BAD_REQUEST);
    }

    @ExceptionHandler(JsonProcessingException.class)
    @ResponseBody
    public ResponseEntity<String> handleJsonProcessingException(HttpServletRequest req, JsonProcessingException ex) {
        return new ResponseEntity<>(ex.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
    }

    @ExceptionHandler(UnauthorizedObjectException.class)
    @ResponseBody
    public ResponseEntity<ErrorApiDTO> handleUnauthorizedObjectException(HttpServletRequest req, UnauthorizedObjectException ex) {
        return createErrorDTO(req, ex, HttpStatus.UNAUTHORIZED);
    }

    @ExceptionHandler(AccessDeniedException.class)
    @ResponseBody
    public ResponseEntity<ErrorApiDTO> handleAccessDeniedException(HttpServletRequest req, AccessDeniedException ex) {
        return createErrorDTO(req, ex, HttpStatus.FORBIDDEN);
    }

    @ExceptionHandler(SecurityException.class)
    @ResponseBody
    public ResponseEntity<ErrorApiDTO> handleSecurityException(HttpServletRequest req, SecurityException ex) {
        return createErrorDTO(req, ex, HttpStatus.UNAUTHORIZED);
    }

    @ExceptionHandler(NoSuchMethodException.class)
    @ResponseBody
    public ResponseEntity<ErrorApiDTO> handleNoSuchMethodException(HttpServletRequest req, NoSuchMethodException ex) {
        return createErrorDTO(req, ex, HttpStatus.NOT_IMPLEMENTED);
    }

    @ExceptionHandler(UnsupportedOperationException.class)
    @ResponseBody
    public ResponseEntity<ErrorApiDTO> handleUnsupporedOperationException(HttpServletRequest req, UnsupportedOperationException ex) {
        return createErrorDTO(req, ex, HttpStatus.NOT_IMPLEMENTED);
    }

    @ExceptionHandler(ServiceUnavailableException.class)
    @ResponseBody
    public ResponseEntity<ErrorApiDTO> handleServiceUnavailableException(HttpServletRequest req, ServiceUnavailableException ex) {
        return createErrorDTO(req, ex, HttpStatus.SERVICE_UNAVAILABLE);
    }

    @ExceptionHandler(HttpClientErrorException.class)
    @ResponseBody
    public ResponseEntity<ErrorApiDTO> handleHttpClientErrorException(HttpServletRequest req, HttpClientErrorException ex) {
        ErrorApiDTO err = new ErrorApiDTO(logger.isDebugEnabled(), ex.getStatusCode().value(),
            ex.toString(),
            // the error message is in the response body
            ex.getResponseBodyAsString(),
            ex.getStackTrace());
        return new ResponseEntity<>(err, ex.getStatusCode());
    }

    @ExceptionHandler(OperationFailedException.class)
    @ResponseBody
    public ResponseEntity<ErrorApiDTO> handleOperationFailedException(HttpServletRequest req, OperationFailedException ex) {
        return createErrorDTO(req, ex, HttpStatus.BAD_REQUEST);
    }

    /**
     * Repackaged {@link StatusRuntimeException} as an ErrorApiDTO to remove stack trace.
     *
     * @param req request {@link HttpServletRequest}.
     * @param ex exception {@link StatusRuntimeException}.
     * @return {@link ErrorApiDTO}.
     */
    @ExceptionHandler(StatusRuntimeException.class)
    @ResponseBody
    public ResponseEntity<ErrorApiDTO> handleStatusRuntimeException(HttpServletRequest req,
            StatusRuntimeException ex) {
        if (ex.getStatus() != null && ex.getStatus().getCode() == Code.INVALID_ARGUMENT) {
            return createErrorDTO(req, ex, HttpStatus.BAD_REQUEST);
        }
        return createErrorDTO(req, ex, HttpStatus.INTERNAL_SERVER_ERROR);
    }

    protected ResponseEntity<ErrorApiDTO> createErrorDTO(@Nonnull HttpServletRequest req, @Nonnull Exception ex,
                                                       HttpStatus statusCode) {
        String logMsg = "API Status Code: " + statusCode.value();
        if (req != null && req.getRequestURI() != null) {
            String url = req.getRequestURL().toString();
            logMsg += " URL: " + url;
        }

        // Print the stacktrace only when it's a server exception
        if (statusCode.equals(HttpStatus.INTERNAL_SERVER_ERROR)) {
            logger.error(logMsg, ex);
        } else {
            if (logger.isDebugEnabled()) {
                logger.error(logMsg, ex);
            } else {
                logger.error(ex.getMessage());
                logger.error(logMsg);
            }
        }
        ErrorApiDTO err = new ErrorApiDTO(logger.isDebugEnabled(), statusCode.value(), ex.toString(), Throwables.getRootCause(ex).getMessage(), ex.getStackTrace());
        return new ResponseEntity<>(err, statusCode);
    }
}
