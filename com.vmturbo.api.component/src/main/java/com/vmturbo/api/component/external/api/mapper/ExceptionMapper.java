package com.vmturbo.api.component.external.api.mapper;

import javax.annotation.Nonnull;

import org.springframework.security.access.AccessDeniedException;

import io.grpc.StatusRuntimeException;

import com.vmturbo.api.component.external.api.service.ProbesService;
import com.vmturbo.api.exceptions.InvalidOperationException;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.api.exceptions.UnauthorizedObjectException;
import com.vmturbo.api.exceptions.UnknownObjectException;

/**
 * Converts status exceptions received by a gRPC calls to exception that
 * {@link com.vmturbo.api.handler.GlobalExceptionHandler} can understand.
 */
public class ExceptionMapper {
    /**
     * This method translates exceptions coming from a gRPC calls to exceptions
     * that API services are expected to throw.
     *
     * @param statusException a gRPC exception.
     * @return {@link UnknownObjectException} if gRPC status was {@code NOT_FOUND}.
     *         {@link UnauthorizedObjectException} if gRPC status was {@code UNAUTHENTICATED}.
     *         {@link InterruptedException} if gRPC status was {@code CANCELLED}.
     *         {@link AccessDeniedException} if gRPC status was {@code PERMISSION_DENIED}.
     *         {@link OperationFailedException} if gRPC status was anything else.
     */
    // TODO (yannis 22/2/2019): returning a generic Exception is not ideal
    // The goal of OM-42959 is to fix this design (for example by creating an checked ApiException
    // as a superclass of all exceptions that the API is expected to throw)
    public static Exception translateStatusException(@Nonnull StatusRuntimeException statusException) {
        switch (statusException.getStatus().getCode()) {
            case NOT_FOUND:
                return new UnknownObjectException(statusException.getCause());
            case UNAUTHENTICATED:
                return new UnauthorizedObjectException(statusException.getMessage());
            case PERMISSION_DENIED:
                return new AccessDeniedException(statusException.getMessage(), statusException.getCause());
            case CANCELLED:
                return new InterruptedException(statusException.getMessage());
            case INVALID_ARGUMENT:
                return new InvalidOperationException(statusException.getMessage());
            default:
                return new OperationFailedException(statusException.getMessage(), statusException);
        }
    }
}
