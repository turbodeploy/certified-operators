package com.vmturbo.api.component.external.api.mapper;

import java.io.IOException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.api.component.communication.CommunicationConfig;
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

    @Bean
    public ActionSpecMapper actionSpecMapper() {
        return new ActionSpecMapper(communicationConfig.repositoryApi());
    }

    @Bean
    public GroupUseCaseParser groupUseCaseParser() {
        return new GroupUseCaseParser(groupBuilderUseCasesFile);
    }

    @Bean
    public GroupMapper groupMapper() {
        return new GroupMapper(groupUseCaseParser());
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
                settingsMapper());
    }

    @Bean
    public MarketMapper marketMapper() {
        return new MarketMapper(scenarioMapper());
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
}
