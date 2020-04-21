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

    @Value("${intersightLicenseSyncRetrySeconds:60}")
    private int intersightLicenseSyncRetrySeconds;

    @Value("${intersightLicenseSyncEnabled:true}")
    private boolean intersightLicenseSyncEnabled;

    @Value("${intersightLicenseCountSyncEnabled:false}")
    private boolean intersightLicenseCountSyncEnabled;

    // the default query filter to use when retrieving the licenses for syncing
    @Value("${intersightLicenseQueryFilter:LicenseType eq 'CWOM-Essential'}")
    private String intersightLicenseQueryFilter;

    @Bean
    public IntersightLicenseClient intersightLicenseClient() {
        return new IntersightLicenseClient(intersightConnectionConfig.getIntersightConnection(), intersightLicenseQueryFilter);
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
        return new IntersightLicenseSyncService(intersightLicenseSyncEnabled, intersightLicenseClient(),
                intersightLicenseSyncInitialDelaySeconds, intersightLicenseSyncIntervalSeconds,
                intersightLicenseSyncRetrySeconds, licenseManagerClient());
    }

    @Bean
    public IntersightLicenseCountUpdater intersightLicenseCountUpdater() {
        return new IntersightLicenseCountUpdater(intersightLicenseCountSyncEnabled,
                keyValueStoreConfig.keyValueStore(),
                licenseCheckClientConfig.licenseCheckClient(),
                intersightLicenseClient(), intersightLicenseSyncIntervalSeconds);
    }
}
