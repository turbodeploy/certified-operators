package com.vmturbo.auth.component.userscope;

import java.time.Clock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc.SupplyChainServiceBlockingStub;
import com.vmturbo.group.api.GroupClientConfig;
import com.vmturbo.repository.api.impl.RepositoryClientConfig;

/**
 *
 */
@Configuration
@Import({GroupClientConfig.class, RepositoryClientConfig.class})
public class UserScopeServiceConfig {
    private static final Logger logger = LogManager.getLogger(UserScopeServiceConfig.class);

    @Autowired
    private GroupClientConfig groupClientConfig;

    @Autowired
    private RepositoryClientConfig repositoryClientConfig;

    @Value("${userScopeServiceCacheEnabled:true}")
    private boolean userScopeServiceCacheEnabled;

    @Bean
    public Clock clock() {
        return Clock.systemUTC();
    }

    @Bean
    public GroupServiceBlockingStub groupRpcService() {
        return GroupServiceGrpc.newBlockingStub(groupClientConfig.groupChannel());
    }

    @Bean
    public SupplyChainServiceBlockingStub supplyChainRpcService() {
        return SupplyChainServiceGrpc.newBlockingStub(repositoryClientConfig.repositoryChannel());
    }

    @Bean
    public UserScopeService userScopeService() {
        UserScopeService userScopeService = new UserScopeService(groupRpcService(),
                supplyChainRpcService(), repositoryClientConfig.searchServiceClient(), clock());
        if (!userScopeServiceCacheEnabled) {
            userScopeService.setCacheEnabled(userScopeServiceCacheEnabled);
        }
        repositoryClientConfig.repository().addListener(userScopeService);
        return userScopeService;
    }

}
