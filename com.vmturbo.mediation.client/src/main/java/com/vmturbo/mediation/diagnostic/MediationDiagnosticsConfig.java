package com.vmturbo.mediation.diagnostic;

import javax.annotation.Nonnull;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Configuration for mediation components diagnostics.
 * Here we create the handler  and inject it with the location of the diagnostics file in order
 * to create diagnostics zip capturing the mediation component state.
 */
@Configuration
public class MediationDiagnosticsConfig {

    @Value("${diags_tmpdir_path:/tmp/diags/}")
    @Nonnull
    private String envTmpDiagsDir;

    /**
     * Creating a new diagnostics handler.
     * @return a new diagnostics.
     */
    @Bean
    public MediationDiagnosticsHandler diagsHandler() {
        return new MediationDiagnosticsHandler(getEnvTmpDiagsDir());
    }

    @Nonnull
    protected String getEnvTmpDiagsDir() {
        return envTmpDiagsDir;
    }
}
