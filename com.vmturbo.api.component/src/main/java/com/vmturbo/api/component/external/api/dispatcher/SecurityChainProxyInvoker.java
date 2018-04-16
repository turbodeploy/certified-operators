package com.vmturbo.api.component.external.api.dispatcher;

import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.web.context.AbstractSecurityWebApplicationInitializer;

/**
 * Configuration dedicated to invoke Spring security filters chain in order to guard REST API
 * interface with authorization.
 */
@Configuration
@EnableWebSecurity
public class SecurityChainProxyInvoker extends AbstractSecurityWebApplicationInitializer {
}
