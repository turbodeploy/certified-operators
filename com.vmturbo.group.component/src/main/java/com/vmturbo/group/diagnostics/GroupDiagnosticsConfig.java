package com.vmturbo.group.diagnostics;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.components.common.DiagnosticsWriter;
import com.vmturbo.components.common.diagnostics.RecursiveZipReaderFactory;
import com.vmturbo.components.common.diagnostics.RecursiveZipReaderFactory.DefaultRecursiveZipReaderFactory;
import com.vmturbo.group.group.GroupConfig;
import com.vmturbo.group.policy.PolicyConfig;
import com.vmturbo.group.setting.SettingConfig;

/**
 * Configuration for group component diagnostics.
 */
@Configuration
@Import({GroupConfig.class,
        PolicyConfig.class,
        SettingConfig.class})
public class GroupDiagnosticsConfig {

    @Autowired
    private SettingConfig settingConfig;

    @Autowired
    private GroupConfig groupConfig;

    @Autowired
    private PolicyConfig policyConfig;

    @Bean
    public DiagnosticsWriter diagnosticsWriter() {
        return new DiagnosticsWriter();
    }

    @Bean
    public RecursiveZipReaderFactory recursiveZipReaderFactory() {
        return new DefaultRecursiveZipReaderFactory();
    }

    @Bean
    public GroupDiagnosticsHandler diagsHandler() {
        return new GroupDiagnosticsHandler(groupConfig.groupStore(), policyConfig.policyStore(),
            settingConfig.settingStore(), recursiveZipReaderFactory(), diagnosticsWriter());
    }

    @Bean
    public GroupDiagnosticsController diagnosticsController() {
        return new GroupDiagnosticsController(diagsHandler());
    }
}
