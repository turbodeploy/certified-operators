package com.vmturbo.api.component.external.api.mapper;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.api.component.communication.CommunicationConfig;
import com.vmturbo.api.component.external.api.mapper.SettingsMapper.Feature;
import com.vmturbo.api.component.external.api.mapper.aspect.CloudApplicationAspectMapper;
import com.vmturbo.api.component.external.api.mapper.aspect.ApplicationServiceAspectMapper;
import com.vmturbo.api.component.external.api.mapper.aspect.BusinessUserAspectMapper;
import com.vmturbo.api.component.external.api.mapper.aspect.CloudAspectMapper;
import com.vmturbo.api.component.external.api.mapper.aspect.CloudCommitmentAspectMapper;
import com.vmturbo.api.component.external.api.mapper.aspect.ComputeTierAspectMapper;
import com.vmturbo.api.component.external.api.mapper.aspect.ContainerPlatformContextAspectMapper;
import com.vmturbo.api.component.external.api.mapper.aspect.DatabaseAspectMapper;
import com.vmturbo.api.component.external.api.mapper.aspect.DatabaseServerAspectMapper;
import com.vmturbo.api.component.external.api.mapper.aspect.DatabaseServerTierAspectMapper;
import com.vmturbo.api.component.external.api.mapper.aspect.DatabaseTierAspectMapper;
import com.vmturbo.api.component.external.api.mapper.aspect.DesktopPoolAspectMapper;
import com.vmturbo.api.component.external.api.mapper.aspect.DiskArrayAspectMapper;
import com.vmturbo.api.component.external.api.mapper.aspect.EntityAspectMapper;
import com.vmturbo.api.component.external.api.mapper.aspect.LogicalPoolAspectMapper;
import com.vmturbo.api.component.external.api.mapper.aspect.MasterImageEntityAspectMapper;
import com.vmturbo.api.component.external.api.mapper.aspect.PhysicalMachineAspectMapper;
import com.vmturbo.api.component.external.api.mapper.aspect.PortsAspectMapper;
import com.vmturbo.api.component.external.api.mapper.aspect.RegionAspectMapper;
import com.vmturbo.api.component.external.api.mapper.aspect.StorageAspectMapper;
import com.vmturbo.api.component.external.api.mapper.aspect.StorageControllerAspectMapper;
import com.vmturbo.api.component.external.api.mapper.aspect.StorageTierAspectMapper;
import com.vmturbo.api.component.external.api.mapper.aspect.VirtualMachineAspectMapper;
import com.vmturbo.api.component.external.api.mapper.aspect.VirtualVolumeAspectMapper;
import com.vmturbo.api.component.external.api.mapper.aspect.VirtualVolumeEntityAspectMapper;
import com.vmturbo.api.component.external.api.mapper.aspect.WorkloadControllerAspectMapper;
import com.vmturbo.api.component.external.api.mapper.converter.CloudSavingsDetailsDtoConverter;
import com.vmturbo.api.component.external.api.mapper.converter.EntityUptimeDtoConverter;
import com.vmturbo.api.component.external.api.mapper.cost.BilledCostStatsMapper;
import com.vmturbo.api.component.external.api.service.ServiceConfig;
import com.vmturbo.api.component.external.api.util.BuyRiScopeHandler;
import com.vmturbo.api.component.external.api.util.MagicScopeGateway;
import com.vmturbo.api.component.external.api.util.TemplatesUtils;
import com.vmturbo.auth.api.authorization.UserSessionConfig;
import com.vmturbo.common.protobuf.cost.BuyReservedInstanceServiceGrpc;
import com.vmturbo.common.protobuf.cost.ReservedInstanceSpecServiceGrpc;
import com.vmturbo.components.api.tracing.Tracing;
import com.vmturbo.cost.api.CostClientConfig;
import com.vmturbo.repository.api.impl.RepositoryClientConfig;

@Configuration
@Import({CommunicationConfig.class, UserSessionConfig.class, RepositoryClientConfig.class})
public class MapperConfig {

    @Value("${groupBuildUseCaseFile}")
    private String groupBuilderUseCasesFile;

    @Value("${settingManagersFile}")
    private String settingManagersFile;

    @Value("${settingStyleFile}")
    private String settingStyleFile;

    @Value("${getVolumeAttachmentHistoryRpcFutureTimeoutSeconds:10}")
    private long getVolumeAttachmentHistoryRpcFutureTimeoutSeconds;

    /**
     * Enable 'Scale for Performance' and 'Scale for Savings' settings.
     */
    @Value("${enableCloudScaleEnhancement:true}")
    private boolean enableCloudScaleEnhancement;

    /**
     * Enable entity details support.
     */
    @Value("${entityDetailsEnabled:false}")
    private boolean entityDetailsEnabled;

    /**
     * Enable reservation related enhancements.
     */
    @Value("${enableReservationEnhancements:true}")
    private boolean enableReservationEnhancements;

    @Autowired
    private CommunicationConfig communicationConfig;

    /*
     * ServiceConfig is autowired because we need to access the PoliciesService from
     * ScenarioMapper. We don't Import it to avoid circular references between the
     * service and the mapper itself.
     */
    @Autowired
    private ServiceConfig serviceConfig;

    @Autowired
    private UserSessionConfig userSessionConfig;

    @Autowired
    private CostClientConfig costClientConfig;

    @Autowired
    private MapperConfig mapperConfig;

    @Autowired
    private RepositoryClientConfig repositoryClientConfig;

    @Bean
    public EntityUptimeDtoConverter entityUptimeDtoConverter() {
        return new EntityUptimeDtoConverter();
    }

    @Bean
    public CloudSavingsDetailsDtoConverter cloudSavingsDetailsDtoConverter() {
        return new CloudSavingsDetailsDtoConverter(entityUptimeDtoConverter());
    }

    @Bean
    public ActionSpecMapper actionSpecMapper() {
        return new ActionSpecMapper(
            actionSpecMappingContextFactory(),
            mapperConfig.reservedInstanceMapper(),
            communicationConfig.riBuyContextFetchStub(),
            communicationConfig.costServiceBlockingStub(),
            communicationConfig.reservedInstanceUtilizationCoverageServiceBlockingStub(),
            mapperConfig.buyRiScopeHandler(),
            communicationConfig.getRealtimeTopologyContextId(),
            uuidMapper(),
            cloudSavingsDetailsDtoConverter(),
            communicationConfig.groupExpander(),
            communicationConfig.useStableActionIdAsUuid());
    }

    @Bean
    public ActionSpecMappingContextFactory actionSpecMappingContextFactory() {
        return new ActionSpecMappingContextFactory(
            communicationConfig.policyRpcService(),
            executorService(),
            communicationConfig.repositoryApi(),
            entityAspectMapper(),
            virtualVolumeAspectMapper(),
            communicationConfig.getRealtimeTopologyContextId(),
            BuyReservedInstanceServiceGrpc.newBlockingStub(costClientConfig.costChannel()),
            ReservedInstanceSpecServiceGrpc.newBlockingStub(costClientConfig.costChannel()),
            communicationConfig.serviceEntityMapper(),
            communicationConfig.supplyChainRpcService(),
            serviceConfig.policiesService(),
            serviceConfig.reservedInstancesService(),
            communicationConfig.groupRpcService(),
            communicationConfig.settingPolicyRpcService(),
            settingsMapper());
    }

    @Bean
    public GroupUseCaseParser groupUseCaseParser() {
        return new GroupUseCaseParser(groupBuilderUseCasesFile);
    }

    @Bean
    public EntityFilterMapper entityFilterMapper() {
        return new EntityFilterMapper(
                groupUseCaseParser(),
                communicationConfig.thinTargetCache());
    }

    @Bean
    public GroupFilterMapper groupFilterMapper() {
        return new GroupFilterMapper();
    }

    /**
     * A definition of 'bean': a mapper for entity details.
     *
     * @return an instance of  {@link EntityDetailsMapper}.
     */
    @Bean
    public EntityDetailsMapper entityDetailsMapper() {
        return new EntityDetailsMapper(communicationConfig.thinTargetCache(), entityDetailsEnabled);
    }

    /**
     * Gets a group mapper that uses the default dependencies.
     *
     * @return a group mapper that uses the default dependencies.
     */
    @Bean
    public GroupMapper groupMapper() {
        return new GroupMapper(
            communicationConfig.supplyChainFetcher(),
            communicationConfig.groupExpander(),
            communicationConfig.repositoryApi(),
            entityFilterMapper(),
            groupFilterMapper(),
            communicationConfig.severityPopulator(),
            serviceConfig.businessAccountRetriever(),
            communicationConfig.costServiceStub(),
            communicationConfig.getRealtimeTopologyContextId(),
            communicationConfig.thinTargetCache(),
            cloudTypeMapper()
            );
    }

    @Bean
    public SettingsManagerMappingLoader settingManagerMappingLoader() {
        try {
            return new SettingsManagerMappingLoader(settingManagersFile);
        } catch (IOException e) {
            throw new RuntimeException("Failed to load setting managers file.", e);
        }
    }

    @Bean
    public SettingSpecStyleMappingLoader settingSpecStyleMappingLoader() {
        try {
            return new SettingSpecStyleMappingLoader(settingStyleFile);
        } catch (IOException e) {
            throw new RuntimeException("Failed to load setting style file.", e);
        }
    }

    @Bean
    public ScenarioMapper scenarioMapper() {
        return new ScenarioMapper(communicationConfig.repositoryApi(),
                templatesUtils(),
                serviceConfig.settingsService(),
                settingManagerMappingLoader().getMapping(),
                settingsMapper(),
                serviceConfig.policiesService(),
                communicationConfig.groupRpcService(),
                groupMapper(),
                uuidMapper());
    }

    @Bean
    public MarketMapper marketMapper() {
        return new MarketMapper(scenarioMapper());
    }

    @Bean
    public PaginationMapper paginationMapper() {
        return new PaginationMapper();
    }

    @Bean
    public PlanDestinationMapper planDestinationMapper() {
        return new PlanDestinationMapper(serviceConfig.businessAccountRetriever(),
                marketMapper(), communicationConfig.planRpcService());
    }

    @Bean
    public TagsPaginationMapper tagsPaginationMapper() { return new TagsPaginationMapper();}

    @Bean
    public StatsMapper statsMapper() {
        return new StatsMapper(paginationMapper(), communicationConfig.serviceEntityMapper());
    }

    @Bean
    public UuidMapper uuidMapper() {
        final UuidMapper uuidMapper =
                new UuidMapper(communicationConfig.getRealtimeTopologyContextId(),
                        magicScopeGateway(), communicationConfig.repositoryApi(),
                        communicationConfig.topologyProcessor(),
                        communicationConfig.planRpcService(),
                        communicationConfig.groupMemberRetriever(), communicationConfig.thinTargetCache(),
                        cloudTypeMapper());
        repositoryClientConfig.repository().addListener(uuidMapper);
        communicationConfig.groupExpander().setUuidMapper(uuidMapper);
        communicationConfig.supplyChainFetcher().setUuidMapper(uuidMapper);
        return uuidMapper;
    }

    @Bean
    public PolicyMapper policyMapper() {
        return new PolicyMapper(groupMapper());
    }

    @Bean
    public TemplateMapper templateMapper() {
        return new TemplateMapper();
    }

    /**
     * Bean for {@link TopologyDataDefinitionMapper}.
     *
     * @return topology data definition mapper
     */
    @Bean
    public TopologyDataDefinitionMapper topologyDataDefinitionMapper() {
        return new TopologyDataDefinitionMapper(entityFilterMapper(), serviceConfig.groupsService());
    }

    @Bean
    public CloudTypeMapper cloudTypeMapper() {
        return new CloudTypeMapper();
    }

    /**
     * Get {@link ReservedInstanceMapper} bean.
     *
     * @return {@link ReservedInstanceMapper} bean.
     */
    @Bean
    public ReservedInstanceMapper reservedInstanceMapper() {
        return new ReservedInstanceMapper(mapperConfig.cloudTypeMapper());
    }

    @Bean
    public TemplatesUtils templatesUtils() {
        return new TemplatesUtils(communicationConfig.templateServiceBlockingStub(),
                                  communicationConfig.templateSpecServiceBlockingStub(),
                                  templateMapper());
    }

    @Bean
    public SettingsMapper settingsMapper() {
        return new SettingsMapper(communicationConfig.settingRpcService(),
                communicationConfig.groupRpcService(),
                communicationConfig.settingPolicyRpcService(),
                settingManagerMappingLoader().getMapping(),
                settingSpecStyleMappingLoader().getMapping(),
                communicationConfig.scheduleRpcService(),
                scheduleMapper(),
                ImmutableMap.of(
                        Feature.CloudScaleEnhancement, enableCloudScaleEnhancement,
                        Feature.ServiceHorizontalScale, true));
    }

    @Bean
    public ScheduleMapper scheduleMapper() {
        return new ScheduleMapper();
    }

    @Bean
    public ReservationMapper reservationMapper() {
        return new ReservationMapper(communicationConfig.repositoryApi(),
            communicationConfig.templateServiceBlockingStub(),
            communicationConfig.groupRpcService(),
            communicationConfig.policyRpcService(),
            enableReservationEnhancements);
    }

    @Bean
    public DiscountMapper discountMapper() {
        return new DiscountMapper(communicationConfig.repositoryApi());
    }

    @Bean
    public StorageTierAspectMapper storageTierAspectMapper() {
        return new StorageTierAspectMapper();
    }

    @Bean
    public VirtualVolumeAspectMapper virtualVolumeAspectMapper() {
        return new VirtualVolumeAspectMapper(communicationConfig.costServiceBlockingStub(),
            communicationConfig.repositoryApi(), communicationConfig.historyRpcService(),
            communicationConfig.serviceEntityMapper(),
            getVolumeAttachmentHistoryRpcFutureTimeoutSeconds);
    }

    /**
     * Creates {@link VirtualVolumeEntityAspectMapper} to create aspects for Virtual Volume
     * instances.
     *
     * @return {@link VirtualVolumeEntityAspectMapper} instance.
     */
    @Bean
    public VirtualVolumeEntityAspectMapper virtualVolumeEntityAspectMapper() {
        return new VirtualVolumeEntityAspectMapper();
    }

    @Bean
    public CloudAspectMapper cloudAspectMapper() {
        return new CloudAspectMapper(communicationConfig.repositoryApi(),
                communicationConfig.reservedInstanceUtilizationCoverageServiceBlockingStub(),
                communicationConfig.groupRpcService(),
                communicationConfig.entityUptimeServiceBlockingStub(), executorService(),
                entityUptimeDtoConverter());
    }

    @Bean
    public ApplicationServiceAspectMapper applicationServiceAspectMapper() {
        return new ApplicationServiceAspectMapper();
    }

    @Bean
    public CloudApplicationAspectMapper cloudApplicationAspectMapper() {
        return new CloudApplicationAspectMapper();
    }

    @Bean
    public VirtualMachineAspectMapper virtualMachineMapper() {
        return new VirtualMachineAspectMapper(communicationConfig.repositoryApi(), businessUserAspectMapper());
    }

    /**
     * Get the {@link RegionAspectMapper}.
     *
     * @return the {@link RegionAspectMapper}
     */
    @Bean
    public RegionAspectMapper regionAspectMapper() {
        return new RegionAspectMapper(communicationConfig.supplyChainRpcService());
    }

    /**
     * Get the {@link DesktopPoolAspectMapper}.
     *
     * @return the {@link DesktopPoolAspectMapper}
     */
    @Bean
    public DesktopPoolAspectMapper desktopPoolAspectMapper() {
        return new DesktopPoolAspectMapper(communicationConfig.repositoryApi(),
                communicationConfig.groupRpcService());
    }

    /**
     * Get the {@link MasterImageEntityAspectMapper}.
     *
     * @return the {@link MasterImageEntityAspectMapper}
     */
    @Bean
    public MasterImageEntityAspectMapper masterImageEntityAspectMapper() {
        return new MasterImageEntityAspectMapper(communicationConfig.repositoryApi(),
                                                 communicationConfig.templateServiceBlockingStub());
    }

    @Bean
    public PhysicalMachineAspectMapper physicalMachineAspectMapper() {
        return new PhysicalMachineAspectMapper(communicationConfig.repositoryApi());
    }

    @Bean
    public StorageAspectMapper storageAspectMapper() {
        return new StorageAspectMapper();
    }

    @Bean
    public PortsAspectMapper portsAspectMapper() {
        return new PortsAspectMapper(communicationConfig.repositoryApi());
    }

    @Bean
    public DiskArrayAspectMapper diskArrayAspectMapper() {
        return new DiskArrayAspectMapper();
    }

    @Bean
    public LogicalPoolAspectMapper logicalPoolAspectMapper() {
        return new LogicalPoolAspectMapper();
    }

    @Bean
    public StorageControllerAspectMapper storageControllerAspectMapper() {
        return new StorageControllerAspectMapper();
    }

    @Bean
    public DatabaseAspectMapper databaseAspectMapper() {
        return new DatabaseAspectMapper();
    }

    @Bean
    public DatabaseServerAspectMapper databaseServerAspectMapper() {
        return new DatabaseServerAspectMapper();
    }

    @Bean
    public WorkloadControllerAspectMapper workloadControllerAspectMapper() {
        return new WorkloadControllerAspectMapper();
    }

    @Bean
    ComputeTierAspectMapper computeTierAspectMapper() {
        return new ComputeTierAspectMapper();
    }

    @Bean
    DatabaseTierAspectMapper databaseTierAspectMapper() {
        return new DatabaseTierAspectMapper();
    }

    @Bean
    DatabaseServerTierAspectMapper databaseServerTierAspectMapper() {
        return new DatabaseServerTierAspectMapper();
    }

    @Bean
    protected BusinessUserAspectMapper businessUserAspectMapper() {
        return new BusinessUserAspectMapper();
    }

    @Bean
    protected CloudCommitmentAspectMapper cloudCommitmentAspectMapper() {
        return new CloudCommitmentAspectMapper();
    }

    @Bean
    protected ContainerPlatformContextAspectMapper cloudNativeAspectMapper() {
        return new ContainerPlatformContextAspectMapper(communicationConfig.supplyChainRpcService(),
                communicationConfig.repositoryApi(), communicationConfig.getRealtimeTopologyContextId());
    }

    /**
     * Returns a common {@link EntityAspectMapper} of the combining aspect mappers of different
     * entities types.
     *
     * @return new instance of {@link EntityAspectMapper}.
     */
    @Bean
    public EntityAspectMapper entityAspectMapper() {
        return new EntityAspectMapper(storageTierAspectMapper(), virtualVolumeAspectMapper(),
                cloudAspectMapper(), virtualMachineMapper(), desktopPoolAspectMapper(),
                masterImageEntityAspectMapper(), physicalMachineAspectMapper(),
                storageAspectMapper(), diskArrayAspectMapper(), logicalPoolAspectMapper(),
                storageControllerAspectMapper(), portsAspectMapper(), databaseAspectMapper(),
                databaseServerAspectMapper(), regionAspectMapper(), workloadControllerAspectMapper(),
                computeTierAspectMapper(), databaseServerTierAspectMapper(), databaseTierAspectMapper(),
                businessUserAspectMapper(), virtualVolumeEntityAspectMapper(),
                cloudCommitmentAspectMapper(), cloudNativeAspectMapper(),
                applicationServiceAspectMapper(), cloudApplicationAspectMapper(),
                communicationConfig.getRealtimeTopologyContextId());
    }

    @Bean
    public WidgetsetMapper widgetsetMapper() {
        return new WidgetsetMapper(groupMapper(), communicationConfig.groupRpcService(),
                communicationConfig.repositoryApi());
    }

    @Bean
    public WorkflowMapper workflowMapper() {
        return new WorkflowMapper();
    }

    @Bean
    public BilledCostStatsMapper billedCostStatsMapper() {
        return new BilledCostStatsMapper(uuidMapper());
    }

    @Bean
    public BuyRiScopeHandler buyRiScopeHandler() {
        return new BuyRiScopeHandler();
    }

    @Bean(destroyMethod = "shutdownNow")
    public ExecutorService executorService() {
        return Tracing.traceAwareExecutor(Executors.newCachedThreadPool(new ThreadFactoryBuilder()
            .setNameFormat("MapperThread-%d")
            .build()));
    }

    @Bean
    public MagicScopeGateway magicScopeGateway() {
        final MagicScopeGateway gateway = new MagicScopeGateway(groupMapper(),
            communicationConfig.groupRpcService(),
            communicationConfig.getRealtimeTopologyContextId());
        repositoryClientConfig.repository().addListener(gateway);
        return gateway;
    }
}
