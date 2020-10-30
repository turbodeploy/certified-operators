package com.vmturbo.cost.component.reserved.instance;

import java.util.concurrent.Executors;

import org.jooq.DSLContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.cloud.commitment.analysis.CloudCommitmentAnalysisConfig;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.cost.CostREST.BuyRIAnalysisServiceController;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc.RepositoryServiceBlockingStub;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc.SearchServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc.SettingServiceBlockingStub;
import com.vmturbo.cost.calculation.topology.TopologyEntityCloudTopologyFactory;
import com.vmturbo.cost.calculation.topology.TopologyEntityCloudTopologyFactory.DefaultTopologyEntityCloudTopologyFactory;
import com.vmturbo.cost.component.CostDBConfig;
import com.vmturbo.cost.component.IdentityProviderConfig;
import com.vmturbo.cost.component.cca.CloudCommitmentAnalysisRunner;
import com.vmturbo.cost.component.cca.CloudCommitmentSettingsFetcher;
import com.vmturbo.cost.component.cca.configuration.CloudCommitmentAnalysisConfigurationHolder;
import com.vmturbo.cost.component.pricing.PricingConfig;
import com.vmturbo.cost.component.rpc.RIBuyContextFetchRpcService;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.ReservedInstanceAnalysisConfig;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.ReservedInstanceAnalysisInvoker;
import com.vmturbo.group.api.GroupClientConfig;
import com.vmturbo.group.api.GroupMemberRetriever;
import com.vmturbo.repository.api.impl.RepositoryClientConfig;

/**
 * Buy RI Analysis Configuration bean.
 */
@Configuration
@Import({ComputeTierDemandStatsConfig.class,
        ReservedInstanceAnalysisConfig.class,
        ReservedInstanceConfig.class,
        GroupClientConfig.class,
        RepositoryClientConfig.class,
        PricingConfig.class,
        CloudCommitmentAnalysisConfig.class,
        CostDBConfig.class})
public class BuyRIAnalysisConfig {

    @Value("${normalBuyRIAnalysisIntervalHours:336}")
    private long normalBuyRIAnalysisIntervalHours;

    @Value("${realtimeTopologyContextId}")
    private long realtimeTopologyContextId;

    @Value("${enableRIBuyAfterPricingChange: true}")
    private boolean enableRIBuyAfterPricingChange;

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

    @Autowired
    private CloudCommitmentAnalysisConfig cloudCommitmentAnalysisConfig;

    // Autowired from RepositoryClientConfig
    @Autowired
    private SearchServiceBlockingStub searchServiceBlockingStub;

    @Value("${disableRealtimeRIBuyAnalysis:false}")
    private boolean disableRealtimeRIBuyAnalysis;

    @Value("${stopAndRunRIBuyOnNewRequest: false}")
    private boolean stopAndRunRIBuyOnNewRequest;

    @Value("${allocation_terminated: false}")
    private boolean allocationTerminated;

    @Value("${allocation_suspended: false}")
    private boolean allocationSuspended;

    @Value("${min_Stability_millis: 0}")
    private int minStabilityMillis;

    @Value("${cca.scopeHistoricalDemandSelection:false}")
    private boolean scopeHistoricalDemandSelection;

    @Value("${allocationFlexible:false}")
    private boolean allocationFlexible;

    @Value("${minimumSavingsOverOnDemand: 80}")
    private float minimumsSavingsOverOnDemand;

    @Value("${maxDemandPercentage: 80}")
    private float maxDemandPercentage;

    /**
     * Gets Buy ReservedInstance Scheduler.
     *
     * @return Buy ReservedInstance Scheduler
     */
    @Bean
    public BuyRIAnalysisScheduler buyReservedInstanceScheduler() {
        return new BuyRIAnalysisScheduler(Executors.newSingleThreadScheduledExecutor(),
                reservedInstanceAnalysisInvoker(), normalBuyRIAnalysisIntervalHours);
    }

    /**
     * Gets Cloud Topology Factory.
     *
     * @return Cloud Topology Factory.
     */
    @Bean
    public TopologyEntityCloudTopologyFactory cloudTopologyFactory() {
        return new DefaultTopologyEntityCloudTopologyFactory(
                new GroupMemberRetriever(
                        GroupServiceGrpc.newBlockingStub(groupClientConfig.groupChannel())));
    }

    /**
     * Gets Buy Reserved Instance Schedule RPC Service.
     *
     * @return Buy Reserved Instance Schedule Rpc Service.
     */
    @Bean
    public BuyRIAnalysisRpcService buyReservedInstanceScheduleRpcService() {
        return new BuyRIAnalysisRpcService(buyReservedInstanceScheduler(),
                repositoryServiceClient(), cloudTopologyFactory(),
                reservedInstanceAnalysisConfig.reservedInstanceAnalyzer(),
                computeTierDemandStatsConfig.riDemandStatsStore(),
                cloudCommitmentSettingsFetcher(),
                cloudCommitmentAnalysisRunner(),
                realtimeTopologyContextId);
    }

    @Bean
    public CloudCommitmentSettingsFetcher cloudCommitmentSettingsFetcher() {
        return new CloudCommitmentSettingsFetcher(
                settingServiceClient(),
                cloudCommitmentAnalysisConfigurationHolder());
    }

    @Bean
    public CloudCommitmentAnalysisRunner cloudCommitmentAnalysisRunner() {
        return new CloudCommitmentAnalysisRunner(
                cloudCommitmentAnalysisConfig.cloudCommitmentAnalysisManager(),
                cloudCommitmentSettingsFetcher(),
                reservedInstanceConfig.planReservedInstanceStore(),
                repositoryServiceClient(),
                searchServiceBlockingStub,
                cloudTopologyFactory());
    }

    /**
     * Gets Buy Reserved Instance Schedule Service Controller.
     *
     * @return Buy Reserved Instance Schedule Service Controller.
     */
    @Bean
    public BuyRIAnalysisServiceController buyReservedInstanceScheduleServiceController() {
        return new BuyRIAnalysisServiceController(buyReservedInstanceScheduleRpcService());
    }

    /**
     * Gets Buy Reserved Instance Store.
     *
     * @return Buy Reserved Instance Store.
     */
    @Bean
    public BuyReservedInstanceStore buyReservedInstanceStore() {
        return new BuyReservedInstanceStore(databaseConfig.dsl(),
                identityProviderConfig.identityProvider());
    }

    /**
     * Gets Buy Reserved Instance Rpc Service.
     *
     * @return Buy Reserved Instance Rpc Service.
     */
    @Bean
    public BuyReservedInstanceRpcService buyReservedInstanceRpcService() {
        return new BuyReservedInstanceRpcService(buyReservedInstanceStore());
    }

    /**
     * Gets Settings Service Client.
     *
     * @return Settings Service Client.
     */
    @Bean
    public SettingServiceBlockingStub settingServiceClient() {
        return SettingServiceGrpc.newBlockingStub(groupClientConfig.groupChannel());
    }

    /**
     * Gets Repository Service Client.
     *
     * @return Repository Service Client.
     */
    @Bean
    public RepositoryServiceBlockingStub repositoryServiceClient() {
        return RepositoryServiceGrpc.newBlockingStub(repositoryClientConfig.repositoryChannel());
    }

    /**
     * Gets Reserved Instance Analysis Invoker.
     *
     * @return Reserved Instance Analysis Invoker.
     */
    @Bean
    public ReservedInstanceAnalysisInvoker reservedInstanceAnalysisInvoker() {
        ReservedInstanceAnalysisInvoker reservedInstanceAnalysisInvoker =
        new ReservedInstanceAnalysisInvoker(reservedInstanceAnalysisConfig.reservedInstanceAnalyzer(),
                repositoryServiceClient(), settingServiceClient(),
                reservedInstanceAnalysisConfig.reservedInstanceBoughtStore(),
                pricingConfig.businessAccountPriceTableKeyStore(), pricingConfig.priceTableStore(),
                realtimeTopologyContextId,
                enableRIBuyAfterPricingChange,
                disableRealtimeRIBuyAnalysis, stopAndRunRIBuyOnNewRequest);
        groupClientConfig.settingsClient().addSettingsListener(reservedInstanceAnalysisInvoker);
        return reservedInstanceAnalysisInvoker;
    }

    /**
     * Gets Dsl context.
     *
     * @return Dsl context.
     */
    public DSLContext getDsl() {
        return databaseConfig.dsl();
    }

    /**
     * Gets RI Buy Context Fetch Rpc Service.
     *
     * @return RI Buy Context Fetch Rpc Service.
     */
    @Bean
    public RIBuyContextFetchRpcService riBuyContextFetchRpcService() {
        return new RIBuyContextFetchRpcService(reservedInstanceConfig.actionContextRIBuyStore());
    }

    /**
     * Constructs the CloudCommitmentConfigurationHolder which contains the settings CCA uses.
     *
     * @return The CloudCommitmentAnalysisConfigurationHolder.
     */
    @Bean
    public CloudCommitmentAnalysisConfigurationHolder cloudCommitmentAnalysisConfigurationHolder() {
        return CloudCommitmentAnalysisConfigurationHolder.builder()
                .scopeHistoricalDemandSelection(scopeHistoricalDemandSelection)
                .allocationFlexible(allocationFlexible)
                .allocationSuspended(allocationSuspended)
                .minStabilityMillis(minStabilityMillis).build();
    }

}
