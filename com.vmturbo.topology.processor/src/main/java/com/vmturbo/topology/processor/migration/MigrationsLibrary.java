package com.vmturbo.topology.processor.migration;


import java.util.Objects;
import java.util.SortedMap;

import javax.annotation.Nonnull;

import org.jooq.DSLContext;

import com.google.common.collect.ImmutableSortedMap;

import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;
import com.vmturbo.components.common.migration.Migration;
import com.vmturbo.kvstore.KeyValueStore;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.identity.services.IdentityServiceUnderlyingStore;
import com.vmturbo.topology.processor.probes.ProbeStore;

public class MigrationsLibrary {

    private final ProbeStore probeStore;

    private final DSLContext dslContext;

    private final StatsHistoryServiceBlockingStub statsHistoryClient;

    private final IdentityServiceUnderlyingStore identityServiceUnderlyingStore;

    private final IdentityProvider identityProvider;

    private final KeyValueStore keyValueStore;

    public MigrationsLibrary(@Nonnull DSLContext dslContext,
                             @Nonnull ProbeStore probeStore,
                             @Nonnull StatsHistoryServiceBlockingStub statsHistoryClient,
                             @Nonnull IdentityServiceUnderlyingStore identityServiceUnderlyingStore,
                             @Nonnull IdentityProvider identityProvider,
                             @Nonnull KeyValueStore keyValueStore) {

        this.dslContext = Objects.requireNonNull(dslContext);
        this.probeStore = Objects.requireNonNull(probeStore);
        this.statsHistoryClient = Objects.requireNonNull(statsHistoryClient);
        this.identityServiceUnderlyingStore = Objects.requireNonNull(identityServiceUnderlyingStore);
        this.identityProvider = Objects.requireNonNull(identityProvider);
        this.keyValueStore = keyValueStore;
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
            new V_01_00_02__TargetSpec_Fix_Derived_Targets_Migration(keyValueStore)
        );
    }
}
