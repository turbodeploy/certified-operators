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

    private static final String PUBLIC_KEY = "PublicKey";

    private static final Logger logger = LogManager.getLogger();

    /**
     * If environment variable "vendor_url_context" is set, use it's value as URL context.
     */
    static final String VENDOR_URL_CONTEXT_TAG = "vendor_url_context";

    @Value("${vendor_supported_admin_role:Server Administrator,Account Administrator,Device Administrator,System Administrator}")
    private String externalAdminRoles;

    @Value("${vendor_supported_observer_role:Read-Only}")
    private String externalObserverRoles;

    @Value("${vendor_request_username_header:x-barracuda-account}")
    private String externalUserTag;

    @Value("${vendor_request_role_header:x-barracuda-roles}")
    private String externalRoleTag;

    @Value("${vendor_public_key_header:X-Starship-Auth-Token-PublicKey}")
    private String jwtTokenPublicKeyTag;

    // the public key should use property name "vendor_public_key"
    @Value("${vendor_public_key:PublicKey}")
    private String jwtTokenPublicKey;

    @Value("${vendor_request_jwt_token:X-Starship-Auth-Token}")
    private String jwtTokenTag;

    @Value("${vendor_latest_version:true}")
    private String isLatestVersion;

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
                        externalObserverRoles, externalUserTag, externalRoleTag, jwtTokenPublicKeyTag, jwtTokenTag);

        logger.info("System is in header authentication and authorization.");
        AuditLog.newEntry(AuditAction.SET_HEADER_AUTH,
                "Enabled header authentication and authorization.", true)
                .targetName(LOGIN_MANAGER)
                .audit();
        return new HeaderAuthenticationFilter(headerMapper, getPublicKey(), isLatestVersion());
    }

    // If the public key is not default value, use it, otherwise just Optional.empty().
    private Optional<String> getPublicKey() {
        return jwtTokenPublicKey.equalsIgnoreCase(PUBLIC_KEY) ? Optional.empty() :
                Optional.of(jwtTokenPublicKey);
    }

    // Assume always use the latest mapping, unless "isLatestVersion" is set to false.
    private boolean isLatestVersion() {
        return isLatestVersion.equalsIgnoreCase("true") ? true : false;
    }
}

