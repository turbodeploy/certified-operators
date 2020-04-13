package com.vmturbo.repository.controller;

import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;

import com.vmturbo.repository.RepositoryComponent;

/**
 * Because of @EnableAutoConfiguration in {@link RepositoryComponent}, Spring security
 * gets enabled in the repository if the spring security JARs are in the classpath.
 * We are permitting all communication on the http interfaces to work without logging in.
 */
@Configuration
@EnableWebSecurity
public class RepositorySecurityConfig extends WebSecurityConfigurerAdapter {
    @Override
    protected void configure(HttpSecurity http) throws Exception {
        http.csrf().disable();
        http.authorizeRequests()
            .antMatchers("/**").permitAll();
    }
}
