package com.vmturbo.auth.component.licensing;

import java.util.ArrayList;
import java.util.List;

import org.junit.Ignore;
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
import com.vmturbo.auth.component.licensing.store.ILicenseStore;
import com.vmturbo.auth.component.licensing.store.LicenseLocalStore;
import com.vmturbo.auth.component.services.LicenseController;

/**
 * License REST test when using {@link LicenseLocalStore}.
 * <p>
 * {@link LicenseController}
 * <p>
 * Note: if you want to debug Spring response, set "org.springframework.web" logger to debug in log4j2.xml file
 * </p>
 */
@Ignore // will revisit while fixing licensing authorization in OM-35910
@RunWith(SpringJUnit4ClassRunner.class)
@WebAppConfiguration
public class LicenseLocalStoreRestTest extends LicenseRestBase {

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

        @Bean
        public ILicenseStore licenseStore() {
            return new LicenseLocalStore();
        }

        @Bean
        public GlobalMethodSecurityConfiguration securityConfiguration() {
            return new SecurityConfig();
        }
/*
        @Bean
        public TestExceptionHandler exceptionHandler() {
            return new TestExceptionHandler();
        }
*/
    }
}
