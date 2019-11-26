package com.vmturbo.securekvstore;

import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.vault.authentication.ClientAuthentication;
import org.springframework.vault.authentication.TokenAuthentication;
import org.springframework.vault.client.VaultEndpoint;
import org.springframework.vault.config.AbstractVaultConfiguration;
import org.springframework.vault.core.VaultTemplate;

/**
 * Configuration for the {@link VaultKeyValueStore} used by components.
 */
@Configuration
public class SecureKeyValueStoreConfig extends AbstractVaultConfiguration {

    @Value("${access_token:root}")
    private String accessToken;

    @Value("${scheme:https}")
    private String scheme;

    @Value("${vault_host:vault}")
    private String vaultHost;

    @Value("${vault_port:8200}")
    private int vaultPort;

    @Value("${component_type:default}")
    protected String componentType;

    @Value("${namespace:kv}")
    protected String namespace;

    @Value("${vaultKeyValueStoreRetryIntervalMillis:1000}")
    protected long vaultKeyValueStoreRetryIntervalMillis;

    @Override
    public VaultEndpoint vaultEndpoint() {
        final VaultEndpoint endpoint = new VaultEndpoint();
        endpoint.setHost(vaultHost);
        endpoint.setScheme(scheme);
        endpoint.setPort(vaultPort);
        return endpoint;
    }

    /**
     * Factory method to return a new {@link VaultKeyValueStore} instance.
     *
     * @return vault key value store instance.
     */
    @Bean
    public VaultKeyValueStore vaultKeyValueStore() {
        return new VaultKeyValueStore(new VaultTemplate(vaultEndpoint(), clientAuthentication()),
                namespace, componentType, vaultKeyValueStoreRetryIntervalMillis,
                TimeUnit.MILLISECONDS);
    }

    @Override
    public ClientAuthentication clientAuthentication() {
        return new TokenAuthentication(accessToken);
    }
}