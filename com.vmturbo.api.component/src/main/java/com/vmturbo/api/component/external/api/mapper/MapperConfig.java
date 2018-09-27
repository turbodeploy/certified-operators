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
import com.vmturbo.api.component.external.api.service.ServiceConfig;
import com.vmturbo.api.component.external.api.util.TemplatesUtils;

@Configuration
@Import({CommunicationConfig.class})
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

    @Bean
    public ActionSpecMapper actionSpecMapper() {
        return new ActionSpecMapper(communicationConfig.repositoryApi(),
                        communicationConfig.policyRpcService(), executorService());
    }

    @Bean
    public GroupUseCaseParser groupUseCaseParser() {
        return new GroupUseCaseParser(groupBuilderUseCasesFile);
    }

    @Bean
    public GroupMapper groupMapper() {
        return new GroupMapper(groupUseCaseParser(),
                    communicationConfig.supplyChainFetcher(),
                    communicationConfig.groupExpander());
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
        return new UuidMapper(communicationConfig.getRealtimeTopologyContextId());
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


    @Bean(destroyMethod = "shutdownNow")
    public ExecutorService executorService() {
        return Executors.newCachedThreadPool(new ThreadFactoryBuilder()
                        .setNameFormat("MapperThread-%d").build());
    }
}
