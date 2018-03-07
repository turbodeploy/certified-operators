package com.vmturbo.history.api;

import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;


/**
 * Configure security for the REST API Dispatcher here.
 *
 * The legacy API - /vmturbo/api/** - requires no permissions.
 *
 * Permissions for the "new API" - /vmturbo/rest
 * This specifies permissions required:
 * <ul>
 *   <li>/api/** - legacy API - no authentication (???)
 *   <li>/rest/login/** - only "anonymous" requests are allowed. I.e. the "/login" request won't be honored if the user
 *   is already logged in
 *   <li>/rest/** - other requests are permitted now, but should require ".authenticated()" in the future.
 * </ul>
 */
@Configuration
@EnableWebSecurity
public class ApiSecurityConfig extends WebSecurityConfigurerAdapter {
    @Override
    protected void configure(HttpSecurity http) throws Exception {
        http.csrf().disable();
        http.authorizeRequests()
            .antMatchers("/**").permitAll();
    }
}

