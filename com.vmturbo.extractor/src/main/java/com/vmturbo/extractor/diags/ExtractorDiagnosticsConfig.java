package com.vmturbo.extractor.diags;

import java.util.Arrays;

import io.prometheus.client.CollectorRegistry;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.vmturbo.components.common.diagnostics.DiagnosticsController;
import com.vmturbo.components.common.diagnostics.DiagnosticsControllerImportable;
import com.vmturbo.components.common.diagnostics.DiagnosticsHandlerImportable;
import com.vmturbo.components.common.diagnostics.DiagsZipReaderFactory;
import com.vmturbo.components.common.diagnostics.DiagsZipReaderFactory.DefaultDiagsZipReader;
import com.vmturbo.components.common.diagnostics.PrometheusDiagnosticsProvider;

/**
 * Configuration for extractor component diagnostics collection.
 */
@Configuration
public class ExtractorDiagnosticsConfig {

    /**
     * Zip reader factory.
     *
     * @return zip reader factory
     */
    @Bean
    public DiagsZipReaderFactory diagsZipReaderFactory() {
        return new DefaultDiagsZipReader();
    }

    /**
     * Collects diagnostics from all relevant diagnosable objects (which are injected as
     * constructor arguments).
     *
     * @return The {@link DiagnosticsControllerImportable}.
     */
    @Bean
    public DiagnosticsHandlerImportable diagnosticsHandler() {
        return new DiagnosticsHandlerImportable(diagsZipReaderFactory(),
            Arrays.asList(new PrometheusDiagnosticsProvider(CollectorRegistry.defaultRegistry)));
    }

    /**
     * The controller that enables the REST endpoints for diag collection.
     *
     * @return The controller.
     */
    @Bean
    public DiagnosticsController diagnosticsController() {
        return new DiagnosticsControllerImportable(diagnosticsHandler());
    }
}
