package com.vmturbo.group.diagnostics;

import java.sql.SQLException;

import com.google.common.collect.Lists;

import io.prometheus.client.CollectorRegistry;

import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.components.common.diagnostics.DiagnosticsControllerImportable;
import com.vmturbo.components.common.diagnostics.DiagnosticsHandlerImportable;
import com.vmturbo.components.common.diagnostics.DiagsZipReaderFactory;
import com.vmturbo.components.common.diagnostics.DiagsZipReaderFactory.DefaultDiagsZipReader;
import com.vmturbo.components.common.diagnostics.PrometheusDiagnosticsProvider;
import com.vmturbo.group.DbAccessConfig;
import com.vmturbo.group.group.GroupConfig;
import com.vmturbo.group.group.GroupDaoDiagnostics;
import com.vmturbo.group.group.pagination.GroupPaginationConfig;
import com.vmturbo.group.policy.PolicyConfig;
import com.vmturbo.group.schedule.ScheduleConfig;
import com.vmturbo.group.service.RpcConfig;
import com.vmturbo.group.setting.SettingConfig;
import com.vmturbo.group.topologydatadefinition.TopologyDataDefinitionConfig;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;

/**
 * Configuration for group component diagnostics.
 */
@Configuration
@Import({GroupConfig.class,
        GroupPaginationConfig.class,
        PolicyConfig.class,
        SettingConfig.class,
        ScheduleConfig.class,
        TopologyDataDefinitionConfig.class,
        RpcConfig.class})
public class GroupDiagnosticsConfig {

    @Autowired
    private SettingConfig settingConfig;

    @Autowired
    private GroupConfig groupConfig;

    @Autowired
    private GroupPaginationConfig groupPaginationConfig;

    @Autowired
    private PolicyConfig policyConfig;

    @Autowired
    private TopologyDataDefinitionConfig topologyDataDefConfig;

    @Autowired
    private ScheduleConfig scheduleConfig;

    @Autowired
    private RpcConfig rpcConfig;

    @Autowired
    private DbAccessConfig databaseConfig;

    @Bean
    public DiagsZipReaderFactory recursiveZipReaderFactory() {
        return new DefaultDiagsZipReader();
    }

    @Bean
    public PrometheusDiagnosticsProvider prometheusDiagnisticsProvider() {
        return new PrometheusDiagnosticsProvider(CollectorRegistry.defaultRegistry);
    }

    @Bean
    public DiagnosticsHandlerImportable diagsHandler() {
        try {
            return new TransactionalDiagnosticsHandlerImportable(recursiveZipReaderFactory(),
                    Lists.newArrayList(groupStoreDiagnostics(), scheduleConfig.scheduleStore(),
                            policyConfig.policyStore(),
                            settingConfig.settingStore(),
                            topologyDataDefConfig.topologyDataDefinitionStore(),
                            topologyDataDefConfig.persistentTopologyDataDefinitionIdentityStore(),
                            prometheusDiagnisticsProvider()),
                    databaseConfig.dsl());
        } catch (SQLException | UnsupportedDialectException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new BeanCreationException("Failed to create DiagnosticsHandlerImportable", e);
        }
    }

    @Bean
    public GroupDaoDiagnostics groupStoreDiagnostics() {
        return new GroupDaoDiagnostics(rpcConfig.transactionProvider(),
                groupPaginationConfig.groupPaginationParams());
    }

    @Bean
    public DiagnosticsControllerImportable diagnosticsController() {
        return new DiagnosticsControllerImportable(diagsHandler());
    }
}
