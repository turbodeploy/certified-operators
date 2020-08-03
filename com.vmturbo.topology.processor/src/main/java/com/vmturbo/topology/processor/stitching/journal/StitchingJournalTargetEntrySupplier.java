package com.vmturbo.topology.processor.stitching.journal;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.Stitching.JournalEntry.TargetEntry;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.TargetSpec;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.stitching.StitchingContext;
import com.vmturbo.topology.processor.targets.TargetStore;

/**
 * Supplies {@link TargetEntry} objects for entry in the {@link StitchingJournal}.
 */
public class StitchingJournalTargetEntrySupplier {
    final TargetStore targetStore;
    final ProbeStore probeStore;
    final StitchingContext stitchingContext;

    public StitchingJournalTargetEntrySupplier(@Nonnull final TargetStore targetStore,
                                               @Nonnull final ProbeStore probeStore,
                                               @Nonnull final StitchingContext stitchingContext)
    {
        this.targetStore = Objects.requireNonNull(targetStore);
        this.probeStore = Objects.requireNonNull(probeStore);
        this.stitchingContext = Objects.requireNonNull(stitchingContext);
    }

    /**
     * Get a list of {@link TargetEntry}s for the targets in the target store.
     *
     * @return A list of {@link TargetEntry}s for the targets in the target store.
     */
    public List<TargetEntry> getTargetEntries() {
        final Map<Long, Integer> targetEntityCounts = stitchingContext.targetEntityCounts();

        return targetStore.getAll().stream()
            .map(target -> {
                final TargetSpec spec = target.getSpec();

                return TargetEntry.newBuilder()
                    .setTargetId(target.getId())
                    .setTargetName(target.getDisplayName())
                    .setProbeName(probeStore.getProbe(spec.getProbeId())
                        .map(ProbeInfo::getProbeType)
                        .orElse("<unknown>"))
                    .setEntityCount(Optional.ofNullable(targetEntityCounts.get(target.getId()))
                        .orElse(0))
                    .build();
            })
            .collect(Collectors.toList());
    }
}
