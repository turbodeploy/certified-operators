package com.vmturbo.topology.processor.stitching.journal;

import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.Stitching.EntityFilter;
import com.vmturbo.common.protobuf.topology.Stitching.EntityTypeFilter;
import com.vmturbo.common.protobuf.topology.Stitching.FilteredJournalRequest;
import com.vmturbo.common.protobuf.topology.Stitching.OperationFilter;
import com.vmturbo.common.protobuf.topology.Stitching.ProbeTypeFilter;
import com.vmturbo.common.protobuf.topology.Stitching.TargetFilter;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.stitching.journal.JournalFilter;
import com.vmturbo.stitching.journal.JournalFilter.FilterByEntity;
import com.vmturbo.stitching.journal.JournalFilter.FilterByEntityType;
import com.vmturbo.stitching.journal.JournalFilter.FilterByOperation;
import com.vmturbo.stitching.journal.JournalFilter.FilterByTargetId;
import com.vmturbo.stitching.journal.JournalFilter.IncludeAllFilter;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetStore;

/**
 * A factory for {@link com.vmturbo.stitching.journal.JournalFilter}s.
 */
public class JournalFilterFactory {
    private final ProbeStore probeStore;
    private final TargetStore targetStore;

    private final Logger logger = LogManager.getLogger();

    public JournalFilterFactory(@Nonnull final ProbeStore probeStore,
                                @Nonnull final TargetStore targetStore) {
        this.probeStore = Objects.requireNonNull(probeStore);
        this.targetStore = Objects.requireNonNull(targetStore);
    }

    public JournalFilter filterFor(@Nonnull final FilteredJournalRequest filteredJournalRequest) {
        switch (filteredJournalRequest.getFilterTypeCase()) {
            case ENTITY_FILTER:
                return filterFor(filteredJournalRequest.getEntityFilter());
            case ENTITY_TYPE_FILTER:
                return filterFor(filteredJournalRequest.getEntityTypeFilter());
            case PROBE_TYPE_FILTER:
                return filterFor(filteredJournalRequest.getProbeTypeFilter());
            case TARGET_FILTER:
                return filterFor(filteredJournalRequest.getTargetFilter());
            case OPERATION_FILTER:
                return filterFor(filteredJournalRequest.getOperationFilter());
            case INCLUDE_ALL_FILTER:
                return includeAllFilter();
            default:
                throw new IllegalArgumentException("Unknown filter type case: " +
                    filteredJournalRequest.getFilterTypeCase());
        }
    }

    public JournalFilter filterFor(@Nonnull final EntityFilter entityFilter) {
        return new FilterByEntity(entityFilter);
    }

    public JournalFilter filterFor(@Nonnull final EntityTypeFilter entityTypeFilter) {
        return new FilterByEntityType(entityTypeFilter);
    }

    public JournalFilter filterFor(@Nonnull final ProbeTypeFilter probeTypeFilter) {
        Set<Long> targetIds;

        switch (probeTypeFilter.getCategoryOrTypeCase()) {
            case PROBE_NAMES:
                targetIds = probeTypeFilter.getProbeNames().getNamesList().stream()
                    .map(probeName -> {
                        final Optional<Long> probeId = probeStore.getProbeIdForType(probeName);
                        if (!probeId.isPresent()) {
                            logger.warn("Unknown probe named: " + probeName);
                        }
                        return probeId;
                    }).filter(Optional::isPresent)
                    .map(Optional::get)
                    .flatMap(probeId -> targetStore.getProbeTargets(probeId).stream()
                        .map(Target::getId))
                    .collect(Collectors.toSet());
                break;
            case PROBE_CATEGORY_NAMES:
                targetIds = probeTypeFilter.getProbeCategoryNames().getNamesList().stream()
                    .map(this::probeCategoryForName)
                    .flatMap(categoryName -> probeStore.getProbeIdsForCategory(categoryName).stream())
                    .flatMap(probeId -> targetStore.getProbeTargets(probeId).stream()
                        .map(Target::getId))
                    .collect(Collectors.toSet());
                break;
            default:
                throw new IllegalArgumentException("Unknown CategoryOrTypeCase: " +
                    probeTypeFilter.getCategoryOrTypeCase());
        }

        return new FilterByTargetId(targetIds);
    }

    public JournalFilter filterFor(@Nonnull final TargetFilter targetFilter) {
        return new FilterByTargetId(new HashSet<>(targetFilter.getTargetIdsList()));
    }

    public JournalFilter filterFor(@Nonnull final OperationFilter operationFilter) {
        return new FilterByOperation(operationFilter);
    }

    public IncludeAllFilter includeAllFilter() {
        return new IncludeAllFilter();
    }

    private ProbeCategory probeCategoryForName(@Nonnull final String categoryName) {
        final Optional<ProbeCategory> category = Stream.of(ProbeCategory.values())
            .filter(name -> name.getCategory().equalsIgnoreCase(categoryName))
            .findFirst();

        if (category.isPresent()) {
            return category.get();
        } else {
            logger.warn("Unknown probe cateogry: " + categoryName);
            return ProbeCategory.UNKNOWN;
        }
    }
}
