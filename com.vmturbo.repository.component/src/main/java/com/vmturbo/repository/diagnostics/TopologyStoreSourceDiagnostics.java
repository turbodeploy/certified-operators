package com.vmturbo.repository.diagnostics;

import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.vmturbo.components.common.diagnostics.DiagnosticsAppender;
import com.vmturbo.components.common.diagnostics.DiagnosticsException;
import com.vmturbo.components.common.diagnostics.StringDiagnosable;
import com.vmturbo.repository.listener.realtime.LiveTopologyStore;
import com.vmturbo.repository.listener.realtime.SourceRealtimeTopology;

/**
 * Class to operate with source topology store diagnostics.
 */
public class TopologyStoreSourceDiagnostics implements StringDiagnosable {

    /**
     * File name inside diagnostics to store topology.
     */
    private static final String REALTIME_TOPOLOGY_STORE_DUMP_FILE =
            "live.realtime.source.entities";

    private final LiveTopologyStore liveTopologyStore;

    /**
     * Constructs diagnostics.
     *
     * @param liveTopologyStore live topology store to use
     */
    public TopologyStoreSourceDiagnostics(@Nonnull LiveTopologyStore liveTopologyStore) {
        this.liveTopologyStore = Objects.requireNonNull(liveTopologyStore);
    }

    @Override
    public void collectDiags(@Nonnull DiagnosticsAppender appender) throws DiagnosticsException {
        final Optional<SourceRealtimeTopology> topology = liveTopologyStore.getSourceTopology();
        if (topology.isPresent()) {
            topology.get().collectDiags(appender);
        }
    }

    @Nonnull
    @Override
    public String getFileName() {
        return REALTIME_TOPOLOGY_STORE_DUMP_FILE;
    }
}
