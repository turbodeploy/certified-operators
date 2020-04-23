package com.vmturbo.api.component.external.api;

import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;

import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.method.configuration.EnableGlobalMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.builders.WebSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.security.core.session.SessionRegistry;
import org.springframework.security.core.session.SessionRegistryImpl;
import org.springframework.security.web.AuthenticationEntryPoint;
import org.springframework.security.web.firewall.DefaultHttpFirewall;
import org.springframework.security.web.session.HttpSessionEventPublisher;

import com.vmturbo.api.component.security.FailedAuthRequestAuditor;
import com.vmturbo.api.component.security.UserPolicyCondition;

/**
 * Configure security for the REST API Dispatcher here.
 * The BASE_URI are defined in {@link ExternalApiConfig#BASE_URL_MAPPINGS}.
 *
 * <p>The legacy API - BASE_URI/api/** - requires no permissions (OM-22367).</p>
 * Permissions for the "new API":
 * <ul>
 * <li>BASE_URI/api/** - legacy API - no authentication.
 * <li>BASE_URI/login/** - only "anonymous" requests are allowed.
 * I.e. the "/login" request won't be honored if the user is already logged in
 * <li>BASE_URI/** - other requests require ".authenticated()".
 * </ul>
 * Following requests are used in UI login page before authentication.
 * To fix the access deny error messages, they are allowed to be accessed by unauthenticated requests.
 * We should revisit to make sure they have valid business uses
 * and are NOT exposing sensitive data (OM-22369).
 * <ul>
 * <li>BASE_URI/checkInit - checks whether the admin has been initialized
 * <li>BASE_URI/cluster/isXLEnabled - check whether XL is enable
 * <li>BASE_URI/cluster/proactive/initialized - check whether proactive is initialized
 * <li>BASE_URI/cluster/proactive/enabled - check whether proactive is enabled
 * <li>BASE_URI/users/me - get the current User
 * <li>BASE_URI/admin/versions - get current product version info and available updates
 * <li>BASE_URI/license - get license
 * <li>BASE_URI/initAdmin - set initial administrator password (will fail if call more than one time)
 * </ul>
 */
@Configuration
@Conditional(UserPolicyCondition.class)
@EnableWebSecurity
@EnableGlobalMethodSecurity(prePostEnabled = true)
public class ApiSecurityConfig extends WebSecurityConfigurerAdapter {



    // mapping to remove the trailing character, so /vmturbo/rest/* -> /vmturbo/rest/
    private static ImmutableList<String> baseURIs = ImmutableList.copyOf(
            ExternalApiConfig.BASE_URL_MAPPINGS.stream()
                    .map(StringUtils::chop)
                    .collect(Collectors.toList()));

    @Autowired
    ApplicationContext applicationContext;

    /**
     * Add a custom authentication exception entry point to replace the default handler. The reason
     * is, for example, if there are some request without login authenticated, the default handler
     * "Http403ForbiddenEntryPoint" will send 403 Forbidden response, but the right response should
     * be 401 Unauthorized.
     *
     * @return the new default authentication exception handler
     */
    @Bean
    public AuthenticationEntryPoint restAuthenticationEntryPoint() {
        return new RestAuthenticationEntryPoint();
    }

    @Override
    protected void configure(HttpSecurity http) throws Exception {
        http.csrf().disable();
        http.headers().frameOptions().disable();

        // Add a custom authentication exception entry point to replace the default handler.
        // The reason is, for example, if there are some request without login authenticated, the default
        // handler "Http403ForbiddenEntryPoint" will send 403 Forbidden response, but the right
        // response should be 401 Unauthorized.
        http.exceptionHandling().authenticationEntryPoint(restAuthenticationEntryPoint());
        // Preventing session fixation is implemented in AuthenticationService::login().
        // Not here with Java configuration, because:
        // 1. login/logout is not configured in the standard way (E.g. by setting the formLogin, loginPage),
        // so changeSessionId does't work.
        // 2. Minimize the impact on the session fixation fix. Since not all REST APIs are secured,
        // by enabling sessionManagement policy here, the secure cookie could be overwritten by insecure cookie
        // and cause HTTP 403 error.
        // http.sessionManagement().sessionCreationPolicy(SessionCreationPolicy.IF_REQUIRED);
        // http.sessionManagement().sessionFixation().changeSessionId();
        for (String base_uri : baseURIs) {
            http.authorizeRequests()
                    .antMatchers(base_uri + "api/**").permitAll()
                    .antMatchers(base_uri + "login").permitAll()
                    .antMatchers(base_uri + "logout").permitAll()
                    .antMatchers(base_uri + "checkInit").permitAll()
                    .antMatchers(base_uri + "cluster/isXLEnabled").permitAll()
                    .antMatchers(base_uri + "cluster/proactive/initialized").permitAll()
                    .antMatchers(base_uri + "cluster/proactive/enabled").permitAll()
                    .antMatchers(base_uri + "users/me").permitAll()
                    .antMatchers(base_uri + "users/saml").permitAll()
                    .antMatchers(base_uri + "admin/versions").permitAll()
                    .antMatchers(base_uri + "admin/productcapabilities").permitAll()
                    .antMatchers(base_uri + "license").permitAll()
                    .antMatchers(base_uri + "initAdmin").permitAll()
                    .antMatchers(base_uri + "**").authenticated();
        }

        // enable session management to keep active session in SessionRegistry,
        // also allow multiple sessions for same user (-1).
        http.sessionManagement().maximumSessions(-1).sessionRegistry(sessionRegistry());
    }

    /**
     * Publish http session event to remove destroyed sessions from {@link
     * org.springframework.security.core.session.SessionRegistry}.
     *
     * @return publisher for HTTP Session events
     */
    @Bean
    public HttpSessionEventPublisher httpSessionEventPublisher() {
        return new HttpSessionEventPublisher();
    }

    /**
     * Keep user sessions, so they can be used to expire deleted user's active sessions.
     *
     * @return an instance of the Spring Session Registry default implementation
     */
    @Bean
    public SessionRegistry sessionRegistry() {
        return new SessionRegistryImpl();
    }

    /**
     * Add a handler for auding failed Auth requests.
     *
     * @return a handler for Failed Auth Requests that logs messages to the audit log
     */
    @Bean
    public FailedAuthRequestAuditor failedAuthRequestAuditor() {
        return new FailedAuthRequestAuditor(applicationContext);
    }

    @Override
    public void configure(WebSecurity web) throws Exception {
        super.configure(web);
        // avoid using default StrictHttpFirewall from Spring
        web.httpFirewall(new DefaultHttpFirewall());
    }


}