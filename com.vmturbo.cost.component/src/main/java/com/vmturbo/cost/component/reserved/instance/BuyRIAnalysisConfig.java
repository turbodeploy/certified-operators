package com.vmturbo.cost.component.reserved.instance;

import java.util.concurrent.Executors;

import org.jooq.DSLContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.common.protobuf.cost.CostREST.BuyRIAnalysisServiceController;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc.RepositoryServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc.SettingServiceBlockingStub;
import com.vmturbo.cost.component.CostDBConfig;
import com.vmturbo.cost.component.IdentityProviderConfig;
import com.vmturbo.cost.component.pricing.PricingConfig;
import com.vmturbo.cost.component.rpc.RIBuyContextFetchRpcService;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.ReservedInstanceAnalysisConfig;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.ReservedInstanceAnalysisInvoker;
import com.vmturbo.group.api.GroupClientConfig;
import com.vmturbo.repository.api.impl.RepositoryClientConfig;


@Configuration
@Import({ComputeTierDemandStatsConfig.class,
        ReservedInstanceAnalysisConfig.class,
    ReservedInstanceConfig.class,
        GroupClientConfig.class,
        RepositoryClientConfig.class,
        PricingConfig.class,
        CostDBConfig.class})
public class BuyRIAnalysisConfig {

    @Value("${normalBuyRIAnalysisIntervalHours}")
    private long normalBuyRIAnalysisIntervalHours;

    @Value("${realtimeTopologyContextId}")
    private long realtimeTopologyContextId;

    @Autowired
    private CostDBConfig databaseConfig;

    @Autowired
    private ComputeTierDemandStatsConfig computeTierDemandStatsConfig;

    @Autowired
    private ReservedInstanceAnalysisConfig reservedInstanceAnalysisConfig;

    @Autowired
    private IdentityProviderConfig identityProviderConfig;

    @Autowired
    private GroupClientConfig groupClientConfig;

    @Autowired
    private RepositoryClientConfig repositoryClientConfig;

    @Autowired
    private ReservedInstanceConfig reservedInstanceConfig;

    @Autowired
    private PricingConfig pricingConfig;

    @Bean
    public BuyRIAnalysisScheduler buyReservedInstanceScheduler() {
        return new BuyRIAnalysisScheduler(Executors.newSingleThreadScheduledExecutor(),
                reservedInstanceAnalysisInvoker(), normalBuyRIAnalysisIntervalHours);
    }

    @Bean
    public BuyRIAnalysisRpcService buyReservedInstanceScheduleRpcService() {
        return new BuyRIAnalysisRpcService(buyReservedInstanceScheduler(),
                reservedInstanceAnalysisConfig.reservedInstanceAnalyzer(),
                computeTierDemandStatsConfig.riDemandStatsStore(),
                realtimeTopologyContextId);
    }

    @Bean
    public BuyRIAnalysisServiceController buyReservedInstanceScheduleServiceController() {
        return new BuyRIAnalysisServiceController(buyReservedInstanceScheduleRpcService());
    }

    @Bean
    public BuyReservedInstanceStore buyReservedInstanceStore() {
        return new BuyReservedInstanceStore(databaseConfig.dsl(),
                identityProviderConfig.identityProvider());
    }

    @Bean
    public BuyReservedInstanceRpcService buyReservedInstanceRpcService() {
        return new BuyReservedInstanceRpcService(buyReservedInstanceStore());
    }

    @Bean
    public SettingServiceBlockingStub settingServiceClient() {
        return SettingServiceGrpc.newBlockingStub(groupClientConfig.groupChannel());
    }

    @Bean
    public RepositoryServiceBlockingStub repositoryServiceClient() {
        return RepositoryServiceGrpc.newBlockingStub(repositoryClientConfig.repositoryChannel());
    }

    @Bean
    public ReservedInstanceAnalysisInvoker reservedInstanceAnalysisInvoker() {
        ReservedInstanceAnalysisInvoker reservedInstanceAnalysisInvoker =
        new ReservedInstanceAnalysisInvoker(reservedInstanceAnalysisConfig.reservedInstanceAnalyzer(),
                repositoryServiceClient(), settingServiceClient(),
                reservedInstanceAnalysisConfig.reservedInstanceBoughtStore(),
                pricingConfig.businessAccountPriceTableKeyStore(), realtimeTopologyContextId);
        groupClientConfig.settingsClient().addSettingsListener(reservedInstanceAnalysisInvoker);
        return reservedInstanceAnalysisInvoker;
    }

    public DSLContext getDsl() {
        return databaseConfig.dsl();
    }

    @Bean
    public RIBuyContextFetchRpcService riBuyContextFetchRpcService() {
        return new RIBuyContextFetchRpcService(reservedInstanceConfig.actionContextRIBuyStore());
    }
}