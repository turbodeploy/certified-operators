package com.vmturbo.integrations.intersight;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.auth.api.AuthClientConfig;
import com.vmturbo.auth.api.authorization.jwt.JwtClientInterceptor;
import com.vmturbo.auth.api.licensing.LicenseCheckClientConfig;
import com.vmturbo.common.protobuf.licensing.LicenseManagerServiceGrpc;
import com.vmturbo.common.protobuf.licensing.LicenseManagerServiceGrpc.LicenseManagerServiceBlockingStub;
import com.vmturbo.integrations.intersight.licensing.IntersightLicenseClient;
import com.vmturbo.integrations.intersight.licensing.IntersightLicenseCountUpdater;
import com.vmturbo.integrations.intersight.licensing.IntersightLicenseSyncService;
import com.vmturbo.kvstore.KeyValueStoreConfig;

/**
 * Configuration for Intersight license sync.
 */
@Configuration
@Import({AuthClientConfig.class, LicenseCheckClientConfig.class, KeyValueStoreConfig.class,
        IntersightConnectionConfig.class})
public class IntersightLicenseSyncConfig {

    @Autowired
    private IntersightConnectionConfig intersightConnectionConfig;

    @Autowired
    private LicenseCheckClientConfig licenseCheckClientConfig;

    @Autowired
    private KeyValueStoreConfig keyValueStoreConfig;

    @Autowired
    private AuthClientConfig authClientConfig;

    @Value("${intersightLicenseSyncInitialDelaySeconds:10}")
    private int intersightLicenseSyncInitialDelaySeconds;

    @Value("${intersightLicenseSyncIntervalSeconds:60}")
    private int intersightLicenseSyncIntervalSeconds;

    // Once the licenses are known to be "in sync", we may lower the check interval to reduce
    // load on the APIs
    @Value("${intersightLicensePostSyncIntervalSeconds:300}")
    private int intersightLicensePostSyncIntervalSeconds;

    @Value("${intersightLicenseSyncRetrySeconds:60}")
    private int intersightLicenseSyncRetrySeconds;

    // if set to true, then the license sync service will install a fallback proxy license if no
    // active licenses are found.
    @Value("${intersightLicenseSyncUseFallbackLicense:true}")
    private boolean intersightLicenseSyncUseFallbackLicense;

    // turns the license sync on and off
    @Value("${intersightLicenseSyncEnabled:true}")
    private boolean intersightLicenseSyncEnabled;

    // turns the license count sync on and off
    @Value("${intersightLicenseCountSyncEnabled:true}")
    private boolean intersightLicenseCountSyncEnabled;

    // the default query filter to use when retrieving the licenses for syncing. We will target the
    // "IWO-*" license types.
    @Value("${intersightLicenseQueryFilter:startswith(LicenseType,'IWO-')}")
    private String intersightLicenseQueryFilter;

    @Bean
    public IntersightLicenseClient intersightLicenseClient() {
        return new IntersightLicenseClient(intersightConnectionConfig.getIntersightConnection(),
                intersightLicenseQueryFilter);
    }

    @Bean
    public JwtClientInterceptor jwtClientInterceptor() {
        return new JwtClientInterceptor();
    }

    @Bean
    public LicenseManagerServiceBlockingStub licenseManagerClient() {
        return LicenseManagerServiceGrpc.newBlockingStub(authClientConfig.authClientChannel())
                .withInterceptors(jwtClientInterceptor());
    }

    @Bean
    public IntersightLicenseSyncService intersightLicenseSyncService() {
        return new IntersightLicenseSyncService(intersightLicenseSyncEnabled, intersightLicenseSyncUseFallbackLicense,
                intersightLicenseClient(), intersightLicenseSyncInitialDelaySeconds, intersightLicenseSyncIntervalSeconds,
                intersightLicensePostSyncIntervalSeconds, intersightLicenseSyncRetrySeconds, licenseManagerClient());
    }

    @Bean
    public IntersightLicenseCountUpdater intersightLicenseCountUpdater() {
        return new IntersightLicenseCountUpdater(intersightLicenseCountSyncEnabled,
                keyValueStoreConfig.keyValueStore(),
                licenseCheckClientConfig.licenseCheckClient(),
                intersightLicenseClient(), intersightLicenseSyncIntervalSeconds, intersightLicenseSyncService());
    }
}
