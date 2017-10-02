package com.vmturbo.topology.processor.api.server;

import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;

import com.vmturbo.topology.processor.TopologyProcessorComponent;

/**
 * Because of @EnableAutoConfiguration in {@link TopologyProcessorComponent}, Spring security
 * gets enabled in the topology processor if the spring security JARs are in the classpath.
 * We permit all communication on the /api/v2/** route to allow inter-component REST and Websocket
 * communication (as well as swagger-ui) to work without logging in.
 */
@Configuration
@EnableWebSecurity
public class TopologyProcessorApiSecurityConfig extends WebSecurityConfigurerAdapter {
    @Override
    protected void configure(HttpSecurity http) throws Exception {
        http.csrf().disable();
        http.authorizeRequests()
                .antMatchers("/api/v2/**").permitAll();
    }
}
