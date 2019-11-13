package com.vmturbo.api.component.external.api.logging;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

import javax.servlet.http.HttpServletRequest;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import org.junit.Before;
import org.junit.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import com.vmturbo.api.dto.ErrorApiDTO;

/**
 * Tests for {@link GlobalExceptionHandler}.
 */
public class GlobalExceptionHandlerTest {

    private GlobalExceptionHandler handler;
    private HttpServletRequest request;

    /**
     * Setup.
     */
    @Before
    public void setup() {
        handler = new GlobalExceptionHandler();
        request = mock(HttpServletRequest.class);
    }

    /**
     * Tests {@link StatusRuntimeException} with {@link Status} INVALID_ARGUMENT. The return status
     * code should be HttpStatus.BAD_REQUEST, so stack trace will not be printed out.
     */
    @Test
    public void handleStatusRuntimeExceptionWithInvalidArgumentStatus() {
        StatusRuntimeException ex =
                Status.INVALID_ARGUMENT.withDescription("msg").asRuntimeException();
        ResponseEntity<ErrorApiDTO> entity = handler.handleStatusRuntimeException(request, ex);
        assertEquals(entity.getStatusCode(), HttpStatus.BAD_REQUEST);
    }

    /**
     * Tests {@link StatusRuntimeException} with {@link Status} NOT_FOUND. The return status
     * code should be HttpStatus.INTERNAL_SERVER_ERROR, so stack trace will be printed out.
     */
    @Test
    public void handleStatusRuntimeExceptionWithOtherStatus() {
        HttpServletRequest request = mock(HttpServletRequest.class);
        StatusRuntimeException ex = Status.NOT_FOUND.withDescription("msg").asRuntimeException();
        ResponseEntity<ErrorApiDTO> entity = handler.handleStatusRuntimeException(request, ex);
        assertEquals(entity.getStatusCode(), HttpStatus.INTERNAL_SERVER_ERROR);
    }
}