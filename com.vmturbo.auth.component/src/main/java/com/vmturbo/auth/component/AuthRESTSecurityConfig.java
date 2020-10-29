package com.vmturbo.auth.component;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpMethod;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.builders.WebSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.security.web.authentication.UsernamePasswordAuthenticationFilter;
import org.springframework.security.web.firewall.DefaultHttpFirewall;

import com.vmturbo.auth.api.SpringSecurityConfig;
import com.vmturbo.auth.api.auditing.AuditLog;
import com.vmturbo.auth.api.db.DBPasswordUtil;
import com.vmturbo.auth.component.handler.GlobalExceptionHandler;
import com.vmturbo.auth.component.policy.UserPolicy;
import com.vmturbo.auth.component.policy.UserPolicy.LoginPolicy;
import com.vmturbo.auth.component.services.AuthUsersController;
import com.vmturbo.auth.component.spring.SpringAuthFilter;
import com.vmturbo.auth.component.store.AuthProvider;
import com.vmturbo.auth.component.store.sso.SsoUtil;
import com.vmturbo.auth.component.widgetset.WidgetsetConfig;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.group.api.GroupClientConfig;

/**
 * Configure security for the REST API Dispatcher here.
 *
 * <p>The users API - /users/** - requires no permissions.
 */
@Configuration
@EnableWebSecurity
@Import({SpringSecurityConfig.class, AuthKVConfig.class, GroupClientConfig.class, WidgetsetConfig.class})
public class AuthRESTSecurityConfig extends WebSecurityConfigurerAdapter {

    @Value("${com.vmturbo.kvdir:/home/turbonomic/data/kv}")
    private String keyDir;

    /**
     * See {@link UserPolicy.LoginPolicy} for details. Default is to allow all.
     */
    @Value("${loginPolicy:ALL}")
    private String loginPolicy;

    @Value("${enableMultiADGroupSupport:true}")
    private boolean enableMultiADGroupSupport;

    /**
     * We allow autowiring between different configuration objects, but not for a bean.
     */
    @Autowired
    private SpringSecurityConfig securityConfig;

    @Autowired
    private AuthKVConfig authKVConfig;

    @Autowired
    private GroupClientConfig groupClientConfig;

    @Autowired
    private WidgetsetConfig widgetsetConfig;

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
            .antMatchers("/metrics")
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
            .antMatchers(DBPasswordUtil.SECURESTORAGE_PATH + DBPasswordUtil.SQL_DB_ROOT_PASSWORD_PATH + "/**")
            .permitAll()
            .and()
            .authorizeRequests()
            .antMatchers(DBPasswordUtil.SECURESTORAGE_PATH + DBPasswordUtil.SQL_DB_ROOT_USERNAME_PATH + "/**")
            .permitAll()
            .and()
            .authorizeRequests()
            .antMatchers(DBPasswordUtil.SECURESTORAGE_PATH + DBPasswordUtil.POSTGRES_DB_ROOT_USERNAME_PATH + "/**")
            .permitAll()
            .and()
            .authorizeRequests()
            .antMatchers("/securestorage/setSqlDBRootPassword/**")
            .permitAll()
            .and()
            .authorizeRequests()
            .antMatchers(DBPasswordUtil.SECURESTORAGE_PATH + DBPasswordUtil.ARANGO_DB_ROOT_PASSWORD_PATH + "/**")
            .permitAll()
            .and()
            .authorizeRequests()
            .antMatchers(DBPasswordUtil.SECURESTORAGE_PATH + DBPasswordUtil.INFLUX_DB_ROOT_PASSWORD_PATH + "/**")
            .permitAll()
            .and()
            .authorizeRequests()
            .antMatchers("/swagger/**")
            .permitAll()
            .and()
            .authorizeRequests()
            .antMatchers(HttpMethod.GET,"/license/**")
            .permitAll()
            .and()
            .authorizeRequests()
            .antMatchers(HttpMethod.POST,"/LogConfigurationService/getLogLevels")
            .permitAll()
            .and()
            .authorizeRequests()
            .antMatchers(HttpMethod.POST,"/LogConfigurationService/setLogLevels")
            .permitAll()
            .and()
            .authorizeRequests()
            .antMatchers(HttpMethod.POST, "/LicenseManagerService/getLicenses")
            .permitAll()
            .anyRequest().authenticated().and()
            .addFilterBefore(new SpringAuthFilter(securityConfig.verifier()),
                             UsernamePasswordAuthenticationFilter.class);
    }

    @Bean
    public GroupServiceBlockingStub groupRpcService() {
        return GroupServiceGrpc.newBlockingStub(groupClientConfig.groupChannel());
    }

    @Bean
    public AuthProvider targetStore() {
        return new AuthProvider(authKVConfig.authKeyValueStore(), groupRpcService(), () -> keyDir,
                widgetsetConfig.widgetsetDbStore(), userPolicy(), new SsoUtil(),
                enableMultiADGroupSupport);
    }

    /**
     * System wide user policy.
     *
     * @return user policy.
     */
    @Bean
    public UserPolicy userPolicy() {
        final LoginPolicy policy = LoginPolicy.valueOf(loginPolicy);
        final UserPolicy userPolicy = new UserPolicy(policy);
        AuditLog.newEntry(userPolicy.getAuditAction(),
                "User login policy is set to " + loginPolicy, true)
                .targetName("Login Policy")
                .audit();
        return userPolicy;
    }

    @Bean
    public AuthUsersController authUsersController() {
        return new AuthUsersController(targetStore());
    }

    @Bean
    public GlobalExceptionHandler globalExceptionHandler() {
        return new GlobalExceptionHandler();
    }

    @Override
    public void configure(WebSecurity web) throws Exception {
        super.configure(web);
        // avoid using default StrictHttpFirewall from Spring
        web.httpFirewall(new DefaultHttpFirewall());
    }
}

