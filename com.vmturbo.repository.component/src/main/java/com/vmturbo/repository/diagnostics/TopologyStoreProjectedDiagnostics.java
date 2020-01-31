package com.vmturbo.repository.diagnostics;

import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.vmturbo.components.common.diagnostics.DiagnosticsAppender;
import com.vmturbo.components.common.diagnostics.DiagnosticsException;
import com.vmturbo.components.common.diagnostics.StringDiagnosable;
import com.vmturbo.repository.listener.realtime.LiveTopologyStore;
import com.vmturbo.repository.listener.realtime.ProjectedRealtimeTopology;

/**
 * Class to operate with projected topology store diagnostics.
 */
public class TopologyStoreProjectedDiagnostics implements StringDiagnosable {

    /**
     * File name inside diagnostics to store topology.
     */
    private static final String REALTIME_PROJECTED_TOPOLOGY_STORE_DUMP_FILE =
            "projected.realtime.source.entities";

    private final LiveTopologyStore liveTopologyStore;

    /**
     * Constructs diagnostics.
     *
     * @param liveTopologyStore live topology store to use
     */
    public TopologyStoreProjectedDiagnostics(@Nonnull LiveTopologyStore liveTopologyStore) {
        this.liveTopologyStore = Objects.requireNonNull(liveTopologyStore);
    }

    @Override
    public void collectDiags(@Nonnull DiagnosticsAppender appender) throws DiagnosticsException {
        final Optional<ProjectedRealtimeTopology> topology =
                liveTopologyStore.getProjectedTopology();
        if (topology.isPresent()) {
            topology.get().collectDiags(appender);
        }
    }

    @Nonnull
    @Override
    public String getFileName() {
        return REALTIME_PROJECTED_TOPOLOGY_STORE_DUMP_FILE;
    }
}
