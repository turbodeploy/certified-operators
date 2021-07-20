package com.vmturbo.voltron;

import com.vmturbo.common.api.utils.EnvironmentUtils;
import com.vmturbo.voltron.VoltronConfiguration.MediationComponent;

/**
 * This is a template for a workbench class to use with Voltron for local development.
 *
 * <p/>If you want to customize it, make a COPY, don't add the copy to git, and do whatever you want with it :)
 */
public class Workbench {

    private Workbench() {
    }

    /**
     * Entrypoint for the run configuration.
     *
     * @param args Args (not used).
     */
    public static void main(String[] args) {
        final String dataPath = EnvironmentUtils.requireEnvProperty("DATA_PATH");
        final String uxPath = EnvironmentUtils.requireEnvProperty("UX_PATH");
        final String apiComponentPath = EnvironmentUtils.requireEnvProperty("API_COMPONENT_PATH");
        final String namespace = EnvironmentUtils.getOptionalEnvProperty("NAMESPACE")
            .orElse("workbench");

        VoltronsContainer voltronsContainer = Voltron.start(namespace, VoltronConfiguration.newBuilder()
            .setUxPath(uxPath)
            .setDataPath(dataPath)
            .setExternalSwaggerPath(new SwaggerSetup(apiComponentPath).copyResourcesIntoDataPath(dataPath))
            .addPlatformComponents()
            .addMediationComponent(MediationComponent.MEDIATION_VC)
            .setCleanOnExit(false)
            .build());
    }
}
