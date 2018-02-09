package com.vmturbo.api.component.external.api.service;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import com.vmturbo.api.exceptions.ServiceUnavailableException;
import com.vmturbo.auth.api.authorization.jwt.JWTAuthorizationVerifier;

/**
 * Unit tests for when {@link AuthenticationService} dependencies, in particular REST
 * calls to Auth Component, fail.
 */
public class AuthenticationServiceDependencyTest {


    private static final int AUTH_PORT = 4321;
    public static final String AUTH_HOST = "AUTH_HOST";
    AuthenticationService testAuthenticationService;
    JWTAuthorizationVerifier mockVerifier;
    RestTemplate mockRestTemplate;

    @Before
    public void setup() {
        mockVerifier = Mockito.mock(JWTAuthorizationVerifier.class);
        mockRestTemplate = Mockito.mock(RestTemplate.class);
        testAuthenticationService = new AuthenticationService(
                AUTH_HOST,
                AUTH_PORT,
                mockVerifier,
                mockRestTemplate
        );
    }

    /**
     * Test exception thrown when /users/checkAdminInit request to Auth component
     * throws RestClientException.
     */
   // @Test(expected = ServiceUnavailableException.class)
    public void testCheckInitAuthServiceDown() {
        // Arrange
        when(mockRestTemplate.getForEntity(anyString(), any()))
                .thenThrow(new RestClientException("test"));
        // Act
        testAuthenticationService.checkInit();
        // Assert
        Assert.fail("should have thrown an exception");
    }

    /**
     * Test exception thrown when /users/initAdmin request to Auth component
     * throws RestClientException.
     */
    //@Test(expected = ServiceUnavailableException.class)
    public void testInitAdminAuthServiceDown() {
        // Arrange
        when(mockRestTemplate.postForObject(anyString(), any(), any(Class.class)))
                .thenThrow(new RestClientException("test"));
        // Act
        testAuthenticationService.initAdmin("username", "password");
        // Assert
        Assert.fail("should have thrown an exception");
    }

    /**
     * Test exception thrown when /users/checkAdminInit request to Auth component
     * throws RestClientException.
     */
    //@Test(expected = ServiceUnavailableException.class)
    public void testLoginAuthServiceDown() {
        // Arrange
        when(mockRestTemplate.getForEntity(anyString(), any(Class.class)))
                .thenThrow(new RestClientException("test"));
        // Act
        testAuthenticationService.login("username", "password", true);
        // Assert
        Assert.fail("should have thrown an exception");
    }
}
