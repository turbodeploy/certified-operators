package com.vmturbo.api.component.external.api.interceptor;

import static com.vmturbo.api.component.external.api.interceptor.LicenseInterceptor.API_COMPONENT_IS_NOT_READY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.PrintWriter;
import java.io.StringWriter;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.http.entity.ContentType;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.http.HttpStatus;

import com.fasterxml.jackson.databind.ObjectMapper;

import com.vmturbo.api.dto.ErrorApiDTO;
import com.vmturbo.auth.api.licensing.LicenseCheckClient;

@RunWith(MockitoJUnitRunner.class)
public class LicenseInterceptorTest {

    @Mock
    private HttpServletRequest request;

    @Mock
    private HttpServletResponse response;

    @Mock
    private LicenseCheckClient licenseCheckClient;

    private LicenseInterceptor licenseInterceptor;

    private StringWriter responseWriter;

    private ObjectMapper objectMapper;

    @Before
    public void setUp() throws Exception {
        licenseInterceptor = new LicenseInterceptor(licenseCheckClient);
        objectMapper = new ObjectMapper();
        when(request.getMethod()).thenReturn("POST");
        when(request.getPathInfo()).thenReturn("/targets");
        responseWriter = new StringWriter();
        when(response.getWriter()).thenReturn(new PrintWriter(responseWriter));
    }

    @Test
    public void testValidLicense() throws Exception {
        // test API call when license is valid
        when(licenseCheckClient.hasValidLicense()).thenReturn(true);
        when(licenseCheckClient.isReady()).thenReturn(true);
        boolean result = licenseInterceptor.preHandle(request, response, null);
        assertTrue(result);
    }

    @Test
    public void testInvalidLicense() throws Exception {
        // test API call when license is invalid
        when(licenseCheckClient.hasValidLicense()).thenReturn(false);
        when(licenseCheckClient.isReady()).thenReturn(true);
        boolean result = licenseInterceptor.preHandle(request, response, null);
        assertFalse(result);

        verify(response).setStatus(HttpStatus.FORBIDDEN.value());
        verify(response).setContentType(ContentType.APPLICATION_JSON.toString());

        ErrorApiDTO errorApiDTO = objectMapper.readValue(responseWriter.toString(), ErrorApiDTO.class);
        assertEquals(HttpStatus.FORBIDDEN.value(), errorApiDTO.getType());
        assertEquals("Invalid license", errorApiDTO.getMessage());
    }

    @Test
    public void testAPICompnentNotReady() throws Exception {
        // test API call when license summary is not available
        when(licenseCheckClient.hasValidLicense()).thenReturn(false);
        when(licenseCheckClient.isReady()).thenReturn(false);
        boolean result = licenseInterceptor.preHandle(request, response, null);
        assertFalse(result);

        verify(response).setStatus(HttpStatus.FORBIDDEN.value());
        verify(response).setContentType(ContentType.APPLICATION_JSON.toString());

        ErrorApiDTO errorApiDTO = objectMapper.readValue(responseWriter.toString(), ErrorApiDTO.class);
        assertEquals(HttpStatus.FORBIDDEN.value(), errorApiDTO.getType());
        assertEquals(API_COMPONENT_IS_NOT_READY, errorApiDTO.getMessage());
    }
}
