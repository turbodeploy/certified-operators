package com.vmturbo.auth.api;

import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
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

import com.vmturbo.auth.api.authorization.jwt.JWTAuthorizationVerifier;
import com.vmturbo.auth.api.authorization.kvstore.AuthApiKVConfig;
import com.vmturbo.auth.api.authorization.kvstore.AuthStore;
import com.vmturbo.auth.api.authorization.kvstore.IAuthStore;
import com.vmturbo.auth.api.authorization.spring.SpringMethodSecurityExpressionHandler;
import com.vmturbo.kvstore.PublicKeyStoreConfig;

/**
 * The SpringSecurityConfig creates the Spring security configuration.
 */
@Configuration
@EnableGlobalMethodSecurity(prePostEnabled = true)
@Import({AuthApiKVConfig.class, PublicKeyStoreConfig.class})
public class SpringSecurityConfig extends GlobalMethodSecurityConfiguration {
    @Autowired
    private AuthApiKVConfig authApiKvConfig;

    @Autowired
    private PublicKeyStoreConfig publicKeyStoreConfig;

    @Bean
    public IAuthStore apiAuthKVStore() {
        return new AuthStore(authApiKvConfig.authKeyValueStore(),
                publicKeyStoreConfig.publicKeyStore());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected MethodSecurityExpressionHandler createExpressionHandler() {
        return new SpringMethodSecurityExpressionHandler();
    }

    @Bean
    public JWTAuthorizationVerifier verifier() {
        return new JWTAuthorizationVerifier(apiAuthKVStore());
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
