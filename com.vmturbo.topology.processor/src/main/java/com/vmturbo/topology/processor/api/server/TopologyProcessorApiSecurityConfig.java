package com.vmturbo.topology.processor.api.server;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.builders.WebSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.security.web.authentication.UsernamePasswordAuthenticationFilter;
import org.springframework.security.web.firewall.DefaultHttpFirewall;

import com.vmturbo.auth.api.SpringSecurityConfig;
import com.vmturbo.components.common.featureflags.FeatureFlags;
import com.vmturbo.topology.processor.TopologyProcessorComponent;

/**
 * Because of @EnableAutoConfiguration in {@link TopologyProcessorComponent}, Spring security
 * gets enabled in the topology processor if the spring security JARs are in the classpath.
 * We are permitting all communication on the http interfaces to work without logging in.
 */
@Configuration
@EnableWebSecurity
@Import({SpringSecurityConfig.class})
public class TopologyProcessorApiSecurityConfig extends WebSecurityConfigurerAdapter {

    /**
     * Security config.
     */
    @Autowired
    public SpringSecurityConfig securityConfig;

    @Override
    protected void configure(HttpSecurity http) throws Exception {
        http.csrf().disable();
        if (FeatureFlags.ENABLE_TP_PROBE_SECURITY.isEnabled()) {
            http.authorizeRequests()
                .antMatchers("/**").permitAll()
                .and().addFilterBefore(new SpringTpFilter(securityConfig.verifier()),
                UsernamePasswordAuthenticationFilter.class);
        } else {
            http.authorizeRequests()
                .antMatchers("/**").permitAll();
        }
    }

    @Override
    public void configure(WebSecurity web) throws Exception {
        super.configure(web);
        // avoid using default StrictHttpFirewall from Spring
        web.httpFirewall(new DefaultHttpFirewall());
    }
}