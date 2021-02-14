package com.vmturbo.auth.api;

import java.util.ArrayList;
import java.util.List;

import com.vmturbo.auth.api.authorization.jwt.JWTAuthorizationVerifier;
import com.vmturbo.auth.api.authorization.keyprovider.EncryptionKeyProvider;
import com.vmturbo.auth.api.authorization.keyprovider.MasterKeyReader;
import com.vmturbo.auth.api.authorization.kvstore.AuthStore;
import com.vmturbo.auth.api.authorization.kvstore.AuthApiKVConfig;
import com.vmturbo.auth.api.authorization.kvstore.IAuthStore;
import com.vmturbo.auth.api.authorization.spring.SpringMethodSecurityExpressionHandler;
import com.vmturbo.components.common.BaseVmtComponentConfig;
import com.vmturbo.components.crypto.CryptoFacility;
import com.vmturbo.kvstore.PublicKeyStoreConfig;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
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
import org.springframework.security.config.annotation.method.configuration.*;

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

    /**
     * If true, use Kubernetes secrets to read in a master encryption key which is used to encrypt
     * and decrypt the internal, component-specific encryption keys.
     * If false, this data will be read from (legacy) persistent volumes.
     *
     * <p>Note: This feature flag is exposed in a static way to avoid having to refactor the
     * many static methods that already exist in {@link CryptoFacility}. This is expected to be a
     * short-lived situation, until enabling external secrets becomes the default.</p>
     */
    @Value("${" + BaseVmtComponentConfig.ENABLE_EXTERNAL_SECRETS_FLAG + ":false}")
    public void setKeyProviderStatic(boolean enableExternalSecrets){
        CryptoFacility.ENABLE_EXTERNAL_SECRETS = enableExternalSecrets;
        if (enableExternalSecrets) {
            CryptoFacility.encryptionKeyProvider =
                new EncryptionKeyProvider(authApiKvConfig.authKeyValueStore(), new MasterKeyReader());
        }
    }
}
