package com.vmturbo.api.component.external.api;

import static com.vmturbo.api.component.external.api.service.AuthenticationService.LOGIN_MANAGER;

import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.authentication.builders.AuthenticationManagerBuilder;
import org.springframework.security.config.annotation.method.configuration.EnableGlobalMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.web.authentication.www.BasicAuthenticationFilter;

import com.vmturbo.api.component.communication.HeaderAuthenticationProvider;
import com.vmturbo.api.component.security.HeaderAuthenticationCondition;
import com.vmturbo.api.component.security.HeaderAuthenticationFilter;
import com.vmturbo.api.component.security.HeaderMapper;
import com.vmturbo.auth.api.auditing.AuditAction;
import com.vmturbo.auth.api.auditing.AuditLog;

/**
 * <p>Spring security configuration for head base authentication and authorization.</p>
 */
@Configuration
@Conditional(HeaderAuthenticationCondition.class)
@EnableWebSecurity
@EnableGlobalMethodSecurity(prePostEnabled = true)
public class HeaderApiSecurityConfig extends ApiSecurityConfig {

    private static final Logger logger = LogManager.getLogger();

    @Value("${vendor_supported_admin_role:Server Administrator,Account Administrator,Device Administrator}")
    private String externalAdminRoles;

    @Value("${vendor_supported_observer_role:Read-Only}")
    private String externalObserverRoles;

    @Value("${vendor_requested_username:x-barracuda-account}")
    private String externalUser;

    @Value("${vendor_requested_role:x-barracuda-roles}")
    private String externalRole;

    @Autowired(required = false)
    private HeaderAuthenticationProvider headerAuthenticationProvider;

    @Override
    protected void configure(HttpSecurity http) throws Exception {
        super.configure(http);
        http.addFilterBefore(headerAuthenticationFilter(), BasicAuthenticationFilter.class);
    }

    /**
     * Sets a custom authentication provider.
     *
     * @param auth SecurityBuilder used to create an AuthenticationManager.
     */
    @Override
    protected void configure(AuthenticationManagerBuilder auth) {
        super.configure(auth);
        auth.authenticationProvider(headerAuthenticationProvider);
    }

    /**
     * Create a filer based for header based authentication.
     *
     * @return the new {@link HeaderAuthenticationFilter}
     */
    @Bean
    @Conditional(HeaderAuthenticationCondition.class)
    public HeaderAuthenticationFilter headerAuthenticationFilter() {
        final HeaderMapper headerMapper =
                HeaderMapperFactory.getHeaderMapper(Optional.empty(), externalAdminRoles,
                        externalObserverRoles, externalUser, externalRole);

        logger.info("System is in header authentication and authorization.");
        AuditLog.newEntry(AuditAction.SET_HEADER_AUTH,
                "Enabled header authentication and authorization.", true)
                .targetName(LOGIN_MANAGER)
                .audit();
        return new HeaderAuthenticationFilter(headerMapper);
    }
}

