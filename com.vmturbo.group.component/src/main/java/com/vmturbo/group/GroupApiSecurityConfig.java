package com.vmturbo.group;

import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;

/**
 * To switch off the Boot default configuration completely in a web application
 * you can add a bean with @EnableWebSecurity.
 * See https://docs.spring.io/spring-boot/docs/current/reference/html/boot-features-security.html
 */
@Configuration
@EnableWebSecurity
public class GroupApiSecurityConfig extends WebSecurityConfigurerAdapter {
    @Override
    protected void configure(HttpSecurity http) throws Exception {
        http.csrf().disable();
        // allow all requests.
        http.authorizeRequests()
                .antMatchers("/**").permitAll();
    }
}
