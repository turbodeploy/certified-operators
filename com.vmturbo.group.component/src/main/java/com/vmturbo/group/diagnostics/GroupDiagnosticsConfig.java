package com.vmturbo.group.diagnostics;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.components.common.diagnostics.DiagnosticsWriter;
import com.vmturbo.components.common.diagnostics.DiagsZipReaderFactory;
import com.vmturbo.components.common.diagnostics.DiagsZipReaderFactory.DefaultDiagsZipReader;
import com.vmturbo.group.group.GroupConfig;
import com.vmturbo.group.policy.PolicyConfig;
import com.vmturbo.group.schedule.ScheduleConfig;
import com.vmturbo.group.setting.SettingConfig;

/**
 * Configuration for group component diagnostics.
 */
@Configuration
@Import({GroupConfig.class,
        PolicyConfig.class,
        SettingConfig.class,
        ScheduleConfig.class})
public class GroupDiagnosticsConfig {

    @Autowired
    private SettingConfig settingConfig;

    @Autowired
    private GroupConfig groupConfig;

    @Autowired
    private PolicyConfig policyConfig;

    @Autowired
    private ScheduleConfig scheduleConfig;

    @Bean
    public DiagnosticsWriter diagnosticsWriter() {
        return new DiagnosticsWriter();
    }

    @Bean
    public DiagsZipReaderFactory recursiveZipReaderFactory() {
        return new DefaultDiagsZipReader();
    }

    @Bean
    public GroupDiagnosticsHandler diagsHandler() {
        return new GroupDiagnosticsHandler(groupConfig.groupStore(), policyConfig.policyStore(),
            settingConfig.settingStore(), scheduleConfig.scheduleStore(), recursiveZipReaderFactory(),
            diagnosticsWriter());
    }

    @Bean
    public GroupDiagnosticsController diagnosticsController() {
        return new GroupDiagnosticsController(diagsHandler());
    }
}
