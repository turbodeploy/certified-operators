package com.vmturbo.api.component.external.api;

import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.method.configuration.EnableGlobalMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;


/**
 * <p>Configure security for the REST API Dispatcher here.
 * The BASE_URI are defined in {@link com.vmturbo.api.component.external.api.ExternalApiConfig#BASE_URL_MAPPINGS}.</p>
 * <p>The legacy API - BASE_URI/api/** - requires no permissions (OM-22367).</p>
 * Permissions for the "new API":
 * <ul>
 *   <li>BASE_URI/api/** - legacy API - no authentication.
 *   <li>BASE_URI/login/** - only "anonymous" requests are allowed.
 *   I.e. the "/login" request won't be honored if the user is already logged in
 *   <li>BASE_URI/** - other requests require ".authenticated()".
 * </ul>
 * Following requests are used in UI login page before authentication.
 * To fix the access deny error messages, they are allowed to be accessed by unauthenticated requests.
 * We should revisit to make sure they have valid business uses
 * and are NOT exposing sensitive data (OM-22369).
 * <ul>
 *  <li>BASE_URI/checkInit - checks whether the admin has been initialized
 *  <li>BASE_URI/cluster/isXLEnabled - check whether XL is enable
 *  <li>BASE_URI/cluster/proactive/initialized - check whether proactive is initialized
 *  <li>BASE_URI/cluster/proactive/enabled - check whether proactive is enabled
 *  <li>BASE_URI/users/me - get the current User
 *  <li>BASE_URI/admin/versions - get current product version info and available updates
 *  <li>BASE_URI/license - get license
 *  <li>BASE_URI/initAdmin - set initial administrator password (will fail if call more than one time)
 * </ul>
 */
@Configuration
@EnableWebSecurity
@EnableGlobalMethodSecurity(prePostEnabled = true)
public class ApiSecurityConfig extends WebSecurityConfigurerAdapter {
    // map "/vmturbo/rest/*" to "/vmturbo/rest/",
    // and "/vmturbo/api/v2/*" to "/vmturbo/api/v2/"
    private static Function<String, String> mappingsToBaseURI = input -> input.substring(0, input.length() - 1);
    private static ImmutableList<String> baseURIs =
            ImmutableList.copyOf(Iterables.transform(ExternalApiConfig.BASE_URL_MAPPINGS,
                            mappingsToBaseURI));

    @Override
    protected void configure(HttpSecurity http) throws Exception {
        http.csrf().disable();
        // Preventing session fixation is implemented in AuthenticationService::login().
        // Not here with Java configuration, because:
        // 1. login/logout is not configured in the standard way (E.g. by setting the formLogin, loginPage),
        // so changeSessionId does't work.
        // 2. Minimize the impact on the session fixation fix. Since not all REST APIs are secured,
        // by enabling sessionManagement policy here, the secure cookie could be overwritten by insecure cookie
        // and cause HTTP 403 error.
        // http.sessionManagement().sessionCreationPolicy(SessionCreationPolicy.IF_REQUIRED);
        // http.sessionManagement().sessionFixation().changeSessionId();
        for (String base_uri: baseURIs)  {
            http.authorizeRequests()
            .antMatchers(base_uri + "api/**").permitAll()
            .antMatchers(base_uri + "login").permitAll()
            .antMatchers(base_uri + "logout").permitAll()
            .antMatchers(base_uri + "checkInit").permitAll()
            .antMatchers(base_uri + "cluster/isXLEnabled").permitAll()
            .antMatchers(base_uri + "cluster/proactive/initialized").permitAll()
            .antMatchers(base_uri + "cluster/proactive/enabled").permitAll()
            .antMatchers(base_uri + "users/me").permitAll()
            .antMatchers(base_uri + "admin/versions").permitAll()
            .antMatchers(base_uri + "license").permitAll()
            .antMatchers(base_uri + "licenses/summary").permitAll()
            .antMatchers(base_uri + "initAdmin").permitAll()
            .antMatchers(base_uri + "**").authenticated();
        }
    }
}

