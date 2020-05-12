package com.vmturbo.topology.processor.migration;


import java.util.Objects;
import java.util.SortedMap;

import javax.annotation.Nonnull;

import org.jooq.DSLContext;

import com.google.common.collect.ImmutableSortedMap;

import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;
import com.vmturbo.components.common.migration.Migration;
import com.vmturbo.identity.store.IdentityStore;
import com.vmturbo.kvstore.KeyValueStore;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.TargetSpec;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.identity.services.IdentityServiceUnderlyingStore;
import com.vmturbo.topology.processor.probes.ProbeStore;
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

    public MigrationsLibrary(@Nonnull DSLContext dslContext,
                             @Nonnull ProbeStore probeStore,
                             @Nonnull StatsHistoryServiceBlockingStub statsHistoryClient,
                             @Nonnull IdentityServiceUnderlyingStore identityServiceUnderlyingStore,
                             @Nonnull IdentityProvider identityProvider,
                             @Nonnull KeyValueStore keyValueStore,
                             @Nonnull TargetStore targetStore,
                             @Nonnull TargetDao targetDao,
                             @Nonnull IdentityStore<TargetSpec> targetIdentityStore) {

        this.dslContext = Objects.requireNonNull(dslContext);
        this.probeStore = Objects.requireNonNull(probeStore);
        this.statsHistoryClient = Objects.requireNonNull(statsHistoryClient);
        this.identityServiceUnderlyingStore = Objects.requireNonNull(identityServiceUnderlyingStore);
        this.identityProvider = Objects.requireNonNull(identityProvider);
        this.keyValueStore = keyValueStore;
        this.targetStore = Objects.requireNonNull(targetStore);
        this.targetDao = Objects.requireNonNull(targetDao);
        this.targetIdentityStore = Objects.requireNonNull(targetIdentityStore);
    }

    public SortedMap<String, Migration> getMigrationsList(){
        return ImmutableSortedMap.of(
            "V_01_00_00__Probe_Metadata_Change_Migration",
            new V_01_00_00__Probe_Metadata_Change_Migration(probeStore,
                dslContext, statsHistoryClient,
                identityServiceUnderlyingStore,
                identityProvider),
            "V_01_00_01__Vim_Probe_Storage_Browsing_Migration",
            new V_01_00_01__Vim_Probe_Storage_Browsing_Migration(probeStore,
                identityServiceUnderlyingStore,
                identityProvider),
            "V_01_00_02__TargetSpec_Fix_Derived_Targets_Migration",
            new V_01_00_02__TargetSpec_Fix_Derived_Targets_Migration(keyValueStore),
            "V_01_00_03__Target_Oid_Replace_Ip_With_Address",
            new V_01_00_03__Target_Oid_Replace_Ip_With_Address(probeStore, targetStore,
                targetIdentityStore),
            "V_01_00_04__Remove_Orphaned_Targets_Migration",
            new V_01_00_04__RemoveOrphanedTargetsMigration(targetStore, probeStore, targetDao)
        );
    }
}
