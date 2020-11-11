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
 * Additional targets and probes migration like 'V_01_01_05'. It includes the next types:
 * - AWS Lambda;
 * - Azure Wasted Volumes;
 * - Intersight;
 * - Intersight UCS;
 * - Intersight Hyperflex.
 */
public class V_01_01_06__Target_Common_Proxy_Settings_Additional extends V_01_01_05__Target_Common_Proxy_Settings {

    /**
     * Constructor.
     *
     * @param targetStore        - target store holding all targets.
     * @param probeStore         - remote probe store holding probe information.
     * @param groupScopeResolver - helper class for resolving group
     */
    @ParametersAreNonnullByDefault
    public V_01_01_06__Target_Common_Proxy_Settings_Additional(TargetStore targetStore, ProbeStore probeStore,
                                                               GroupScopeResolver groupScopeResolver) {
        super(targetStore, probeStore, groupScopeResolver);
    }

    /**
     * Rename configuration.
     * --------------------
     * TYPE                     CLASS (AccountDefinition)                                 FIELDS TO UPDATE          FIELDS TO KEEP
     * ----                     -------------------------                                 ----------------          --------------
     * AWS Lambda               com.vmturbo.mediation.aws.client.AwsAccountBase           proxy,port,proxyUser
     * Azure Wasted Volumes     com.vmturbo.mediation.azure.AzureAccount                  proxy,port,proxyUser
     * Intersight               com.vmturbo.mediation.intersight.base.IntersightAccount   proxy                      proxyPort
     * Intersight UCS           com.vmturbo.mediation.intersight.base.IntersightAccount   proxy                      proxyPort
     * Intersight Hyperflex     com.vmturbo.mediation.intersight.base.IntersightAccount   proxy                      proxyPort
     *
     * @return configuration map.
     */
    @Nonnull
    @Override
    protected Map<SDKProbeType, List<Pair<String, String>>> createRenameConfig() {
        final Map<SDKProbeType, List<Pair<String, String>>> map = Maps.newHashMap();
        map.put(SDKProbeType.AWS_LAMBDA,
                Arrays.asList(HOST_RENAME, PORT_RENAME, USER_RENAME));
        map.put(SDKProbeType.AZURE_STORAGE_BROWSE,
                Arrays.asList(HOST_RENAME, PORT_RENAME, USER_RENAME));
        map.put(SDKProbeType.INTERSIGHT,
                Arrays.asList(HOST_RENAME));
        map.put(SDKProbeType.INTERSIGHT_HX,
                Arrays.asList(HOST_RENAME));
        map.put(SDKProbeType.INTERSIGHT_UCS,
                Arrays.asList(HOST_RENAME));
        return map;
    }
}
