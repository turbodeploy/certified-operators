package com.vmturbo.api.component.external.api.mapper;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.vmturbo.api.component.communication.CommunicationConfig;
import com.vmturbo.api.component.external.api.mapper.aspect.CloudAspectMapper;
import com.vmturbo.api.component.external.api.mapper.aspect.DatabaseAspectMapper;
import com.vmturbo.api.component.external.api.mapper.aspect.DiskArrayAspectMapper;
import com.vmturbo.api.component.external.api.mapper.aspect.EntityAspectMapper;
import com.vmturbo.api.component.external.api.mapper.aspect.LogicalPoolAspectMapper;
import com.vmturbo.api.component.external.api.mapper.aspect.PhysicalMachineAspectMapper;
import com.vmturbo.api.component.external.api.mapper.aspect.PortsAspectMapper;
import com.vmturbo.api.component.external.api.mapper.aspect.StorageAspectMapper;
import com.vmturbo.api.component.external.api.mapper.aspect.StorageTierAspectMapper;
import com.vmturbo.api.component.external.api.mapper.aspect.VirtualDisksAspectMapper;
import com.vmturbo.api.component.external.api.mapper.aspect.VirtualMachineAspectMapper;
import com.vmturbo.api.component.external.api.mapper.aspect.VirtualVolumeAspectMapper;
import com.vmturbo.api.component.external.api.service.ServiceConfig;
import com.vmturbo.api.component.external.api.util.TemplatesUtils;
import com.vmturbo.auth.api.authorization.UserSessionConfig;

@Configuration
@Import({CommunicationConfig.class, UserSessionConfig.class})
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

    @Bean
    public ActionSpecMapper actionSpecMapper() {
        return new ActionSpecMapper(communicationConfig.repositoryApi(),
                        communicationConfig.policyRpcService(), executorService(),
                        communicationConfig.getRealtimeTopologyContextId());
    }

    @Bean
    public GroupUseCaseParser groupUseCaseParser() {
        return new GroupUseCaseParser(groupBuilderUseCasesFile);
    }

    @Bean
    public GroupMapper groupMapper() {
        return new GroupMapper(groupUseCaseParser(),
                    communicationConfig.supplyChainFetcher(),
                    communicationConfig.groupExpander(),
                    userSessionConfig.userSessionContext());
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
            communicationConfig.planRpcService(),
            communicationConfig.groupRpcService());
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
    public ReservedInstanceMapper reservedInstanceMapper() {
        return new ReservedInstanceMapper(communicationConfig.repositoryApi());
    }

    @Bean
    public TemplatesUtils templatesUtils() {
        return new TemplatesUtils(communicationConfig.templateServiceBlockingStub(),
                                  communicationConfig.templateSpecServiceBlockingStub(),
                                  templateMapper());
    }

    @Bean
    public SettingsMapper settingsMapper() {
        return new SettingsMapper(communicationConfig.groupChannel(),
                settingManagerMappingLoader().getMapping(),
                settingSpecStyleMappingLoader().getMapping());
    }

    @Bean
    public ReservationMapper reservationMapper() {
        return new ReservationMapper(communicationConfig.repositoryApi(),
                communicationConfig.templateServiceBlockingStub(),
                communicationConfig.groupRpcService(),
                communicationConfig.policyRpcService());
    }

    @Bean
    public BusinessUnitMapper businessUnitMapper() {
        return new BusinessUnitMapper(communicationConfig.getRealtimeTopologyContextId());
    }

    @Bean
    public StorageTierAspectMapper storageTierAspectMapper() {
        return new StorageTierAspectMapper();
    }

    @Bean
    public VirtualDisksAspectMapper virtualDisksAspectMapper() {
        return new VirtualDisksAspectMapper();
    }

    @Bean
    public VirtualVolumeAspectMapper virtualVolumeAspectMapper() {
        return new VirtualVolumeAspectMapper(communicationConfig.searchServiceBlockingStub(),
            communicationConfig.costServiceBlockingStub());
    }

    @Bean
    public CloudAspectMapper cloudAspectMapper() {
        return new CloudAspectMapper();
    }

    @Bean
    public VirtualMachineAspectMapper virtualMachineMapper() {
        return new VirtualMachineAspectMapper();
    }

    @Bean
    public PhysicalMachineAspectMapper physicalMachineAspectMapper() {
        return new PhysicalMachineAspectMapper(communicationConfig.searchServiceBlockingStub());
    }

    @Bean
    public StorageAspectMapper storageAspectMapper() {
        return new StorageAspectMapper();
    }
    @Bean
    public PortsAspectMapper portsAspectMapper() {
        return new PortsAspectMapper();
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
    public DatabaseAspectMapper databaseAspectMapper() {
        return new DatabaseAspectMapper();
    }

    @Bean
    public EntityAspectMapper entityAspectMapper() {
        return new EntityAspectMapper(storageTierAspectMapper(), virtualVolumeAspectMapper(),
                cloudAspectMapper(), virtualMachineMapper(), physicalMachineAspectMapper(),
                storageAspectMapper(), portsAspectMapper(), diskArrayAspectMapper(),
            logicalPoolAspectMapper(), databaseAspectMapper(), virtualDisksAspectMapper());
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
        return Executors.newCachedThreadPool(new ThreadFactoryBuilder()
                        .setNameFormat("MapperThread-%d").build());
    }
}
