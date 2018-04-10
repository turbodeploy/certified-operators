package com.vmturbo.api.component.external.api;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.http.HttpStatus;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.web.AuthenticationEntryPoint;

/**
 * This entry point class is called when {@link org.springframework.security.web.access.ExceptionTranslationFilter}
 * throw exception which is {@link AuthenticationException} or {@link AccessDeniedException} without
 * authenticated. And in this case, it should send out unauthorized response.
 */
public class RestAuthenticationEntryPoint implements AuthenticationEntryPoint {

    @Override
    public void commence(HttpServletRequest request, HttpServletResponse response,
                         AuthenticationException authException) throws IOException, ServletException {
        response.sendError(HttpStatus.UNAUTHORIZED.value(), authException.getMessage());
    }
}
