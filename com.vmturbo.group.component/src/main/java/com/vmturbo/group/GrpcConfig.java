package com.vmturbo.group;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.common.protobuf.search.SearchServiceGrpc;
import com.vmturbo.repository.api.impl.RepositoryClientConfig;

@Configuration
@Import({RepositoryClientConfig.class})
public class GrpcConfig {

    @Autowired
    private RepositoryClientConfig repositoryClientConfig;

    @Bean
    public SearchServiceGrpc.SearchServiceStub searchServiceStub() {
        return SearchServiceGrpc.newStub(repositoryClientConfig.repositoryChannel());
    }

    @Bean
    public SearchServiceGrpc.SearchServiceBlockingStub searchServiceBlockingStub() {
        return SearchServiceGrpc.newBlockingStub(repositoryClientConfig.repositoryChannel());
    }
}
