package com.vmturbo.cost.component;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.auth.api.authorization.jwt.JwtClientInterceptor;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc.SupplyChainServiceBlockingStub;
import com.vmturbo.repository.api.impl.RepositoryClientConfig;

/**
 * Config class for SupplyChainServiceStub.
 */
@Configuration
@Import({RepositoryClientConfig.class})
public class SupplyChainServiceConfig {

    @Autowired
    private RepositoryClientConfig repositoryClientConfig;

    /**
     * Returns an instance of SupplyChainServiceBlockingStub.
     *
     * @return an instance of SupplyChainServiceBlockingStub.
     */
    @Bean
    public SupplyChainServiceBlockingStub supplyChainRpcService() {
        return SupplyChainServiceGrpc.newBlockingStub(repositoryClientConfig.repositoryChannel())
                .withInterceptors(new JwtClientInterceptor());
    }
}