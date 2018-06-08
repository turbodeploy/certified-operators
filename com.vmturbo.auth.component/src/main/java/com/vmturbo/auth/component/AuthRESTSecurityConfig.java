package com.vmturbo.auth.component;

import com.vmturbo.auth.api.SpringSecurityConfig;
import com.vmturbo.auth.component.services.AuthUsersController;
import com.vmturbo.auth.component.spring.SpringAuthFilter;
import com.vmturbo.auth.component.store.AuthProvider;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpMethod;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configuration
        .WebSecurityConfigurerAdapter;
import org.springframework.security.web.authentication.UsernamePasswordAuthenticationFilter;

/**
 * Configure security for the REST API Dispatcher here.
 * <p>
 * The users API - /users/** - requires no permissions.
 */
@Configuration
@EnableWebSecurity
@Import({SpringSecurityConfig.class, AuthKVConfig.class})
public class AuthRESTSecurityConfig extends WebSecurityConfigurerAdapter {

    /**
     * We allow autowiring between different configuration objects, but not for a bean.
     */
    @Autowired
    private SpringSecurityConfig securityConfig;

    @Autowired
    private AuthKVConfig authKVConfig;

    @Override
    protected void configure(HttpSecurity http) throws Exception {
        http.csrf().disable();
        http.authorizeRequests()
            .antMatchers("/health")
            .permitAll()
            .and()
            .authorizeRequests()
            .antMatchers("/state")
            .permitAll()
            .and()
            .authorizeRequests()
            .antMatchers("/summary")
            .permitAll()
            .and()
            .authorizeRequests()
            .antMatchers("/diagnostics")
            .permitAll()
            .and()
            .authorizeRequests()
            .antMatchers("/users/authenticate/**")
            .permitAll()
            .and()
            .authorizeRequests()
            .antMatchers("/users/checkAdminInit/**")
            .permitAll()
            .and()
            .authorizeRequests()
            .antMatchers("/users/initAdmin/**")
            .permitAll()
            .and()
            .authorizeRequests()
            .antMatchers("/users/setpassword/**")
            .permitAll()
            .and()
            .authorizeRequests()
            .antMatchers("/securestorage/getSqlDBRootPassword/**")
            .permitAll()
            .and()
            .authorizeRequests()
            .antMatchers("/securestorage/setSqlDBRootPassword/**")
            .permitAll()
            .and()
            .authorizeRequests()
            .antMatchers("/securestorage/getArangoDBRootPassword/**")
            .permitAll()
            .and()
            .authorizeRequests()
            .antMatchers("/swagger/**")
            .permitAll()
            .and()
            .authorizeRequests()
            .antMatchers(HttpMethod.GET,"/license/**")
            .permitAll()
            .anyRequest().authenticated().and()
            .addFilterBefore(new SpringAuthFilter(securityConfig.verifier()),
                             UsernamePasswordAuthenticationFilter.class);
    }

    @Bean
    public AuthProvider targetStore() {
        return new AuthProvider(authKVConfig.authKeyValueStore());
    }

    @Bean
    public AuthUsersController authUsersController() {
        return new AuthUsersController(targetStore());
    }

}

