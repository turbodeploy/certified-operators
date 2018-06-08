package com.vmturbo.auth.component;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;

import java.util.ArrayList;
import java.util.List;

import org.junit.runner.RunWith;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.access.AccessDecisionManager;
import org.springframework.security.access.AccessDecisionVoter;
import org.springframework.security.access.expression.method.ExpressionBasedPreInvocationAdvice;
import org.springframework.security.access.expression.method.MethodSecurityExpressionHandler;
import org.springframework.security.access.prepost.PreInvocationAuthorizationAdviceVoter;
import org.springframework.security.access.vote.AuthenticatedVoter;
import org.springframework.security.access.vote.RoleVoter;
import org.springframework.security.access.vote.UnanimousBased;
import org.springframework.security.config.annotation.method.configuration.EnableGlobalMethodSecurity;
import org.springframework.security.config.annotation.method.configuration.GlobalMethodSecurityConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;

import com.vmturbo.auth.api.authorization.spring.SpringMethodSecurityExpressionHandler;
import com.vmturbo.auth.component.RestTest.TestExceptionHandler;
import com.vmturbo.auth.component.services.LicenseController;
import com.vmturbo.auth.component.store.ILicenseStore;
import com.vmturbo.auth.component.store.LicenseKVStore;
import com.vmturbo.auth.component.store.LicenseLocalStore;
import com.vmturbo.kvstore.MapKeyValueStore;

/**
 *  License REST tests when using {@link LicenseKVStore}.
 *
 * {@link LicenseController}
 * <p>
 * Note: if you want to debug Spring response, set "org.springframework.web" logger to debug in log4j2.xml file
 * </p>
 */
@RunWith(SpringJUnit4ClassRunner.class)
@WebAppConfiguration
public class LicenseKVStoreRestTest extends LicenseRestBase {

    @Configuration
    @EnableGlobalMethodSecurity(prePostEnabled = true)
    static class SecurityConfig extends GlobalMethodSecurityConfiguration {
        /**
         * {@inheritDoc}
         */
        @Override
        protected MethodSecurityExpressionHandler createExpressionHandler() {
            return new SpringMethodSecurityExpressionHandler();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        protected AccessDecisionManager accessDecisionManager() {
            List<AccessDecisionVoter<? extends Object>>
                    decisionVoters = new ArrayList<>();
            ExpressionBasedPreInvocationAdvice expressionAdvice =
                    new ExpressionBasedPreInvocationAdvice();
            expressionAdvice.setExpressionHandler(getExpressionHandler());
            decisionVoters
                    .add(new PreInvocationAuthorizationAdviceVoter(expressionAdvice));
            decisionVoters.add(new RoleVoter());
            decisionVoters.add(new AuthenticatedVoter());
            return new UnanimousBased(decisionVoters);
        }
    }

    /**
     * Nested configuration for Spring context.
     */
    @Configuration
    @EnableWebMvc
    static class ContextConfiguration extends WebMvcConfigurerAdapter {
        @Bean
        public LicenseController licenseController() {
            return new LicenseController(licenseStore());
        }

        // Using LicenseKVStore
        @Bean
        public ILicenseStore licenseStore() {
            return new LicenseKVStore(new MapKeyValueStore());
        }

        @Bean
        public GlobalMethodSecurityConfiguration securityConfiguration() {
            return new SecurityConfig();
        }

        @Bean
        public TestExceptionHandler exceptionHandler() {
            return new TestExceptionHandler();
        }

    }
}
