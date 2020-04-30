package com.vmturbo.auth.api.authorization.scoping;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Lazy;

import com.vmturbo.auth.api.AuthClientConfig;
import com.vmturbo.auth.api.authorization.jwt.JwtClientInterceptor;
import com.vmturbo.common.protobuf.userscope.UserScopeServiceGrpc;
import com.vmturbo.common.protobuf.userscope.UserScopeServiceGrpc.UserScopeServiceBlockingStub;

/**
 *
 */
@Configuration
@Lazy
@Import({AuthClientConfig.class})
public class UserScopeServiceClientConfig {

    @Autowired
    private AuthClientConfig authClientConfig;

    @Bean
    public UserScopeServiceBlockingStub userScopeServiceClient() {
        return UserScopeServiceGrpc.newBlockingStub(authClientConfig.authClientChannel())
                .withInterceptors(new JwtClientInterceptor());
    }

}
