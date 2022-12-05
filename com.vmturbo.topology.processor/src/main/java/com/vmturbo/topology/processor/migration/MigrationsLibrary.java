package com.vmturbo.topology.processor.migration;

import java.util.Objects;
import java.util.SortedMap;

import javax.annotation.Nonnull;

import org.jooq.DSLContext;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedMap;

import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;
import com.vmturbo.components.common.migration.Migration;
import com.vmturbo.identity.store.IdentityStore;
import com.vmturbo.kvstore.KeyValueStore;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.TargetSpec;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.identity.services.IdentityServiceUnderlyingStore;

import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.targets.GroupScopeResolver;
import com.vmturbo.topology.processor.targets.TargetDao;
import com.vmturbo.topology.processor.targets.TargetStore;

public class MigrationsLibrary {

    private final ProbeStore probeStore;

    private final DSLContext dslContext;

    private final StatsHistoryServiceBlockingStub statsHistoryClient;

    private final IdentityServiceUnderlyingStore identityServiceUnderlyingStore;

    private final IdentityProvider identityProvider;

    private final KeyValueStore keyValueStore;

    private final TargetStore targetStore;

    private final TargetDao targetDao;

    private final IdentityStore<TargetSpec> targetIdentityStore;

    private final GroupScopeResolver groupScopeResolver;

    public MigrationsLibrary(@Nonnull DSLContext dslContext,
                             @Nonnull ProbeStore probeStore,
                             @Nonnull StatsHistoryServiceBlockingStub statsHistoryClient,
                             @Nonnull IdentityProvider identityProvider,
                             @Nonnull KeyValueStore keyValueStore,
                             @Nonnull TargetStore targetStore,
                             @Nonnull TargetDao targetDao,
                             @Nonnull IdentityStore<TargetSpec> targetIdentityStore,
                             @Nonnull GroupScopeResolver groupScopeResolver) {

        this.dslContext = Objects.requireNonNull(dslContext);
        this.probeStore = Objects.requireNonNull(probeStore);
        this.statsHistoryClient = Objects.requireNonNull(statsHistoryClient);
        this.identityServiceUnderlyingStore = Objects.requireNonNull(identityProvider.getStore());
        this.identityProvider = Objects.requireNonNull(identityProvider);
        this.keyValueStore = keyValueStore;
        this.targetStore = Objects.requireNonNull(targetStore);
        this.targetDao = Objects.requireNonNull(targetDao);
        this.targetIdentityStore = Objects.requireNonNull(targetIdentityStore);
        this.groupScopeResolver = Objects.requireNonNull(groupScopeResolver);
    }

    public SortedMap<String, Migration> getMigrationsList(){
        ImmutableSortedMap.Builder<String, Migration> builder =
            ImmutableSortedMap.<String, Migration>naturalOrder();
        builder.put("V_01_00_00__Probe_Metadata_Change_Migration",
            new V_01_00_00__Probe_Metadata_Change_Migration(probeStore,
                dslContext, statsHistoryClient,
                identityServiceUnderlyingStore,
                identityProvider))
            .put("V_01_00_01__Vim_Probe_Storage_Browsing_Migration",
                new V_01_00_01__Vim_Probe_Storage_Browsing_Migration(probeStore,
                    identityServiceUnderlyingStore,
                    identityProvider))
            .put("V_01_00_02__TargetSpec_Fix_Derived_Targets_Migration",
                new V_01_00_02__TargetSpec_Fix_Derived_Targets_Migration(keyValueStore))
            .put("V_01_00_03__Target_Oid_Replace_Ip_With_Address",
                new V_01_00_03__Target_Oid_Replace_Ip_With_Address(probeStore, targetStore,
                    targetIdentityStore, targetDao))
            .put("V_01_00_04__Remove_Orphaned_Targets_Migration",
                new V_01_00_04__RemoveOrphanedTargetsMigration(targetStore, probeStore, targetDao))
            .put("V_01_00_05__Standalone_AWS_Billing",
                new V_01_00_05__Standalone_AWS_Billing(keyValueStore, identityProvider))
            .put("V_01_01_01__Add_UI_Category_and_license_To_Probes_Migration",
                        new V_01_01_00__Add_UI_Category_and_license_To_Probes_Migration(keyValueStore))
            .put("V_01_01_02__Target_Oid_Fields_To_Lowercase",
                new V_01_01_02__Target_Oid_Fields_To_Lowercase(probeStore, targetStore,
                    targetIdentityStore, targetDao))
            .put("V_01_01_03__Target_IsProxySecure_Flag",
                    new V_01_01_03__Target_IsProxySecure_Flag(targetStore, probeStore, groupScopeResolver))
            .put("V_01_01_04__Fix_Orphaned_Target_Removal_Migration",
                new V_01_01_04__Fix_Orphaned_Target_Removal_Migration(targetStore, probeStore, targetDao))
            .put("V_01_01_05__Target_Common_Proxy_Settings",
                new V_01_01_05__Target_Common_Proxy_Settings(targetStore, probeStore, groupScopeResolver))
            .put("V_01_01_06__Target_Common_Proxy_Settings_Additional",
                new V_01_01_06__Target_Common_Proxy_Settings_Additional(targetStore, probeStore, groupScopeResolver))
            .put("V_01_01_07__Target_Common_Proxy_Settings_CloudFoundry",
                new V_01_01_07__Target_Common_Proxy_Settings_CloudFoundry(targetStore, probeStore, groupScopeResolver))
            .put("V_01_01_08__AWS_Add_AccountType",
                new V_01_01_08__AWS_Add_AccountType(keyValueStore))
            .put("V_01_01_09__VV_Metadata_Change_Migration",
                new V_01_01_09__VV_Metadata_Change_Migration(probeStore, dslContext, identityProvider))
            .put("V_01_01_10__Dynatrace_Add_Vm_Metrics_Flag",
                new V_01_01_10__Dynatrace_Add_Vm_Metrics_Flag(targetStore, probeStore))
            .put("V_01_01_12__Instana_Add_Vm_Metrics_Flag",
                new V_01_01_12__Instana_Add_Vm_Metrics_Flag(targetStore, probeStore))
            .put("V_01_01_13__Remove_GCP_Beta_Migration",
                new V_01_01_13__Remove_GCP_Beta_Migration(keyValueStore, probeStore, targetStore))
            .put("V_01_01_14__GCP_Remove_Cost_Probe_Targets",
                new V_01_01_14__GCP_Remove_Cost_Probe_Targets(keyValueStore))
            .put("V_01_01_15__NewRelic_Add_Vm_Metrics_Flag",
                    new V_01_01_15__NewRelic_Add_Vm_Metrics_Flag(targetStore, probeStore))
            .put("V_01_01_16__GCP_Remove_Billing_Probe_Targets",
                    new V_01_01_16__GCP_Remove_Billing_Probe_Targets(keyValueStore))
            .put("V_01_01_17__AppDynamics_Add_Vm_Metrics_Flag",
                    new V_01_01_11__AppDynamics_Add_Vm_Metrics_Flag(targetStore, probeStore))
            .put("V_01_01_18__GCP_BillingProbe_RenameStandardDatasetTable_Names",
                    new V_01_01_18__GCP_BillingProbe_RenameStandardDatasetTable_Names(keyValueStore))
            .put("V_01_01_19__GCP_Billing_Probe_Resource_Level_Flag",
                    new V_01_01_19__GCP_Billing_Probe_Resource_Level_Flag(keyValueStore))
            .put("V_01_01_20__OracleProbes_Remove_Vm_Metrics_Flag",
                new V_01_01_20__OracleProbes_Remove_Vm_Metrics_Flag(targetStore, probeStore))
            .put("V_01_01_21__MysqlProbes_Remove_Vm_Metrics_Flag",
                new V_01_01_21__MysqlProbes_Remove_Vm_Metrics_Flag(targetStore, probeStore))
            .put("V_01_01_22__MssqlProbes_Remove_Vm_Metrics_Flag",
                new V_01_01_22__MssqlProbes_Remove_Vm_Metrics_Flag(targetStore, probeStore))
            .put("V_01_01_23__Vcenter_Enable_Guest_Metrics_Flag",
                new V_01_01_23__Vcenter_Enable_Guest_Metrics_Flag(keyValueStore, ImmutableSet.of(SDKProbeType.VCENTER)));
        return builder.build();
    }
}
