package com.vmturbo.topology.processor.communication.it;

import javax.annotation.Nonnull;

import com.vmturbo.mediation.diagnostic.MediationDiagnosticsConfig;

/**
 * Mock configuration for test cases.
 */
public class MediationDiagnosticsConfigTest extends MediationDiagnosticsConfig {

    @Override
    @Nonnull
    protected String getEnvTmpDiagsDir() {
        return "/tmp/diags/";
    }

}
