package com.vmturbo.history.diagnostics;

import java.time.Clock;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.components.common.DiagnosticsWriter;
import com.vmturbo.history.db.HistoryDbConfig;

/**
 * Configuration for custom diagnostics for the history component.
 */
@Configuration
@Import({HistoryDbConfig.class})
public class HistoryDiagnosticsConfig {

    @Autowired
    private HistoryDbConfig historyDbConfig;

    @Bean
    public HistoryDiagnostics historyDiagnostics() {
        return new HistoryDiagnostics(Clock.systemUTC(), historyDbConfig.historyDbIO(),
            diagnosticsWriter());
    }

    @Bean
    public DiagnosticsWriter diagnosticsWriter() {
        return new DiagnosticsWriter();
    }

    @Bean
    public HistoryDiagnosticsController diagnosticsController() {
        return new HistoryDiagnosticsController(historyDiagnostics());
    }
}
