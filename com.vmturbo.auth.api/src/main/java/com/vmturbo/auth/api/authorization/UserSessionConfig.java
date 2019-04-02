package com.vmturbo.auth.api.authorization;

import java.time.Clock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.auth.api.authorization.scoping.UserScopeServiceClientConfig;

/**
 *
 */
@Configuration
@Import({UserScopeServiceClientConfig.class})
public class UserSessionConfig {
    private static final Logger logger = LogManager.getLogger(UserSessionConfig.class);

    @Autowired
    private UserScopeServiceClientConfig userScopeServiceClientConfig;

    // whether or not the user session context cache should be enabled.
    @Value("${userSessionContextCacheEnabled:true}")
    private boolean userSessionContextCacheEnabled;

    // number of seconds a fresh cache entry is considered valid before it needs to be synced with
    // the server.
    @Value("${sessionScopeCacheExpirationSecs:30}")
    private int sessionScopeCacheExpirationSecs;

    // number of seconds between invocations of the cleanup command. defaults to 5 mins.
    @Value("${sessionScopeCleanupIntervalSecs:300}")
    private int sessionScopeCleanupIntervalSecs;

    // when the cleanup procedure runs, it will purge any cached scope data that has expired after
    // this many seconds. Defaults to 5 minus
    @Value("${expiredScopePurgeThresholdSecs:300}")
    private int expiredScopePurgeThresholdSecs;

    @Bean
    public Clock clock() {
        return Clock.systemUTC();
    }

    @Bean
    public UserSessionContext userSessionContext() {
        if (userSessionContextCacheEnabled) {
            return new UserSessionContext(userScopeServiceClientConfig.userScopeServiceClient(), clock(),
                    sessionScopeCacheExpirationSecs, sessionScopeCleanupIntervalSecs, expiredScopePurgeThresholdSecs);
        } else {
            return new UserSessionContext(userScopeServiceClientConfig.userScopeServiceClient(), clock());
        }
    }

}
