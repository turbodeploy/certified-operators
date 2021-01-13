package com.vmturbo.topology.processor.migration;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;
import javax.annotation.ParametersAreNonnullByDefault;

import com.google.common.collect.Maps;

import com.vmturbo.platform.sdk.common.util.Pair;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.targets.GroupScopeResolver;
import com.vmturbo.topology.processor.targets.TargetStore;

/**
 * More targets and probes migration like 'V_01_01_05'. It includes:
 * - Cloud Foundry.
 */
public class V_01_01_07__Target_Common_Proxy_Settings_CloudFoundry extends V_01_01_05__Target_Common_Proxy_Settings {

    /**
     * Constructor.
     *
     * @param targetStore        - target store holding all targets.
     * @param probeStore         - remote probe store holding probe information.
     * @param groupScopeResolver - helper class for resolving group
     */
    @ParametersAreNonnullByDefault
    public V_01_01_07__Target_Common_Proxy_Settings_CloudFoundry(TargetStore targetStore, ProbeStore probeStore,
                                                               GroupScopeResolver groupScopeResolver) {
        super(targetStore, probeStore, groupScopeResolver);
    }

    /**
     * Rename configuration.
     * --------------------
     * TYPE           CLASS (AccountDefinition)                                   FIELDS TO UPDATE
     * ----           -------------------------                                   ----------------
     * Cloud Foundry  com.vmturbo.mediation.cloudfoundry.sdk.CloudFoundryAccount  proxy,port
     *
     * @return configuration map.
     */
    @Nonnull
    @Override
    protected Map<SDKProbeType, List<Pair<String, String>>> createRenameConfig() {
        final Map<SDKProbeType, List<Pair<String, String>>> map = Maps.newHashMap();
        map.put(SDKProbeType.CLOUD_FOUNDRY,
                Arrays.asList(HOST_RENAME, PORT_RENAME));
        return map;
    }
}
