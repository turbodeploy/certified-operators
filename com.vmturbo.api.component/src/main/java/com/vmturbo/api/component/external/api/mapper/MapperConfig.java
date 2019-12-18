package com.vmturbo.api.component.external.api.mapper;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.api.component.communication.CommunicationConfig;
import com.vmturbo.api.component.external.api.mapper.aspect.CloudAspectMapper;
import com.vmturbo.api.component.external.api.mapper.aspect.DatabaseAspectMapper;
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
import com.vmturbo.api.component.external.api.service.ServiceConfig;
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
    public ActionSpecMapper actionSpecMapper() {
        return new ActionSpecMapper(
            actionSpecMappingContextFactory(),
            communicationConfig.serviceEntityMapper(),
            mapperConfig.reservedInstanceMapper(),
            communicationConfig.riBuyContextFetchStub(),
            communicationConfig.costServiceBlockingStub(),
            serviceConfig.statsQueryExecutor(),
            uuidMapper(),
            communicationConfig.reservedInstanceUtilizationCoverageServiceBlockingStub(),
            communicationConfig.reservedInstanceBoughtServiceBlockingStub(),
            communicationConfig.repositoryApi(),
            communicationConfig.getRealtimeTopologyContextId());
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
            communicationConfig.supplyChainRpcService());
    }

    @Bean
    public GroupUseCaseParser groupUseCaseParser() {
        return new GroupUseCaseParser(groupBuilderUseCasesFile);
    }

    @Bean
    public EntityFilterMapper entityFilterMapper() {
        return new EntityFilterMapper(groupUseCaseParser());
    }

    @Bean
    public GroupFilterMapper groupFilterMapper() {
        return new GroupFilterMapper();
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
            communicationConfig.topologyProcessor(),
            communicationConfig.repositoryApi(),
            entityFilterMapper(),
            groupFilterMapper(),
            communicationConfig.severityPopulator(),
            serviceConfig.businessAccountRetriever(),
            communicationConfig.costServiceBlockingStub(),
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
                settingManagerMappingLoader().getMapping(),
                settingsMapper(),
                serviceConfig.policiesService(),
                communicationConfig.groupRpcService(),
                groupMapper());
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
    public StatsMapper statsMapper() {
        return new StatsMapper(paginationMapper());
    }

    @Bean
    public UuidMapper uuidMapper() {
        return new UuidMapper(communicationConfig.getRealtimeTopologyContextId(),
            magicScopeGateway(),
            communicationConfig.repositoryApi(),
            communicationConfig.topologyProcessor(),
            communicationConfig.planRpcService(),
            communicationConfig.groupRpcService(),
            communicationConfig.groupExpander());
    }

    @Bean
    public PolicyMapper policyMapper() {
        return new PolicyMapper(groupMapper());
    }

    @Bean
    public TemplateMapper templateMapper() {
        return new TemplateMapper();
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
                settingSpecStyleMappingLoader().getMapping());
    }

    public ScheduleMapper scheduleMapper() {
        return new ScheduleMapper();
    }

    @Bean
    public ReservationMapper reservationMapper() {
        return new ReservationMapper(communicationConfig.repositoryApi(),
            communicationConfig.templateServiceBlockingStub(),
            communicationConfig.groupRpcService(),
            communicationConfig.policyRpcService(),
            communicationConfig.deploymentProfileServiceBlockingStub());
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
            communicationConfig.repositoryApi());
    }

    @Bean
    public CloudAspectMapper cloudAspectMapper() {
        return new CloudAspectMapper(communicationConfig.repositoryApi());
    }

    @Bean
    public VirtualMachineAspectMapper virtualMachineMapper() {
        return new VirtualMachineAspectMapper(communicationConfig.repositoryApi());
    }

    /**
     * Get the {@link RegionAspectMapper}.
     *
     * @return the {@link RegionAspectMapper}
     */
    @Bean
    public RegionAspectMapper regionAspectMapper() {
        return new RegionAspectMapper();
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
    public EntityAspectMapper entityAspectMapper() {
        return new EntityAspectMapper(storageTierAspectMapper(), virtualVolumeAspectMapper(),
            cloudAspectMapper(), virtualMachineMapper(), desktopPoolAspectMapper(),
            masterImageEntityAspectMapper(), physicalMachineAspectMapper(),
            storageAspectMapper(), diskArrayAspectMapper(), logicalPoolAspectMapper(),
            storageControllerAspectMapper(), portsAspectMapper(), databaseAspectMapper(),
            regionAspectMapper());
    }

    @Bean
    public WidgetsetMapper widgetsetMapper() {
        return new WidgetsetMapper(groupMapper(), communicationConfig.groupRpcService());
    }

    @Bean
    public WorkflowMapper workflowMapper() {
        return new WorkflowMapper();
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
