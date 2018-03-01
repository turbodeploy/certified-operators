package com.vmturbo.group.diagnostics;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.components.common.DiagnosticsWriter;
import com.vmturbo.components.common.diagnostics.RecursiveZipReaderFactory;
import com.vmturbo.components.common.diagnostics.RecursiveZipReaderFactory.DefaultRecursiveZipReaderFactory;
import com.vmturbo.group.ArangoDBConfig;

/**
 * Configuration for group component diagnostics.
 */
@Configuration
@Import({ArangoDBConfig.class})
public class GroupDiagnosticsConfig {

    @Autowired
    private ArangoDBConfig arangoDBConfig;

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
        return new GroupDiagnosticsHandler(arangoDBConfig.groupStore(), arangoDBConfig.policyStore(),
            recursiveZipReaderFactory(), diagnosticsWriter());
    }

    @Bean
    public GroupDiagnosticsController diagnosticsController() {
        return new GroupDiagnosticsController(diagsHandler());
    }
}
