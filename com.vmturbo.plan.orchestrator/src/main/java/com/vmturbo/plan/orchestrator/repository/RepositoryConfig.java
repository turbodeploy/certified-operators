package com.vmturbo.plan.orchestrator.repository;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc.RepositoryServiceBlockingStub;
import com.vmturbo.repository.api.impl.RepositoryClientConfig;

@Configuration
@Import(RepositoryClientConfig.class)
public class RepositoryConfig {
    @Autowired
    private RepositoryClientConfig repositoryClientConfig;

    @Bean
    public RepositoryServiceBlockingStub repositoryServiceBlockingStub() {
        return RepositoryServiceGrpc.newBlockingStub(repositoryClientConfig.repositoryChannel());
    }
}
