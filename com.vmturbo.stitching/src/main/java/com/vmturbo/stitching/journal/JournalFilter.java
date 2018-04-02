package com.vmturbo.stitching.journal;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

import com.vmturbo.common.protobuf.topology.Stitching.EntityFilter;
import com.vmturbo.common.protobuf.topology.Stitching.EntityTypeFilter;
import com.vmturbo.common.protobuf.topology.Stitching.OperationFilter;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * A filter to restrict the {@link JournalableEntity} objects to enter into a {@link IStitchingJournal}.
 */
public interface JournalFilter {

    /**
     * Check if this operation and changes that it makes should be entered into
     * the stitching journal.
     *
     * If it returns true, permits changes made by the operation to be entered
     * into the journal. Otherwise, changes made by the operation are not entered
     * into the journal
     *
     * @param operation The operation to check.
     * @return Whether this operation and changes that it makes should be entered into
     *         the stitching journal.
     */
    boolean shouldEnter(@Nonnull final JournalableOperation operation);

    /**
     * Check if this entity or changes made to it should be entered into the stitching
     * journal.
     *
     * If it returns true, permits changes made to the entity to be entered into the
     * journal. Otherwise, the entity will not be entered into the journal.
     *
     * @param entity The entity to check.
     * @return Whether the entity or changes made to it should be entered into the stitching
     *         journal.
     */
    boolean shouldEnter(@Nonnull final JournalableEntity<?> entity);

    /**
     * Filter no entries out of the journal. That is, allow all entries into the journal.
     */
    @Immutable
    class IncludeAllFilter implements JournalFilter {
        @Override
        public boolean shouldEnter(@Nonnull JournalableOperation operation) {
            return true;
        }

        @Override
        public boolean shouldEnter(@Nonnull JournalableEntity<?> entity) {
            return true;
        }
    }

    /**
     * Only allow entities with a given OID or displayName.
     */
    @Immutable
    class FilterByEntity implements JournalFilter {
        private final Set<Long> allowedOids;
        private final Set<String> allowedDisplayNames;

        public FilterByEntity(@Nonnull final EntityFilter entityFilter) {
            allowedOids = new HashSet<>(entityFilter.getOidsList());
            allowedDisplayNames = new HashSet<>(entityFilter.getDisplayNamesList());
        }

        @Override
        public boolean shouldEnter(@Nonnull JournalableOperation operation) {
            return true;
        }

        @Override
        public boolean shouldEnter(@Nonnull JournalableEntity<?> entity) {
            return allowedOids.contains(entity.getOid()) ||
                allowedDisplayNames.contains(entity.getDisplayName());
        }
    }

    /**
     * Only allow entities with a given entityType.
     */
    @Immutable
    class FilterByEntityType implements JournalFilter {
        private final Set<EntityType> allowedEntityTypes;

        public FilterByEntityType(@Nonnull final EntityTypeFilter entityTypeFilter) {
            final List<EntityType> entityTypes = new ArrayList<>();
            entityTypeFilter.getEntityTypeNamesList().stream()
                .map(EntityType::valueOf)
                .forEach(entityTypes::add);
            entityTypeFilter.getEntityTypeNumbersList().stream()
                .map(EntityType::forNumber)
                .forEach(entityTypes::add);

            if (entityTypeFilter.getEntityTypeNamesCount() > 0) {
                allowedEntityTypes = EnumSet.copyOf(entityTypes);
            } else {
                allowedEntityTypes = Collections.emptySet();
            }
        }

        @Override
        public boolean shouldEnter(@Nonnull JournalableOperation operation) {
            return true;
        }

        @Override
        public boolean shouldEnter(@Nonnull JournalableEntity<?> entity) {
            return allowedEntityTypes.contains(entity.getJournalableEntityType());
        }
    }

    /**
     * Only allow entries related to entities discovered by given targets.
     */
    @Immutable
    class FilterByTargetId implements JournalFilter {
        private final Set<Long> allowedTargetIds;

        public FilterByTargetId(@Nonnull final Set<Long> allowedTargetIds) {
            this.allowedTargetIds = Objects.requireNonNull(allowedTargetIds);
        }

        @Override
        public boolean shouldEnter(@Nonnull JournalableOperation operation) {
            return true;
        }

        @Override
        public boolean shouldEnter(@Nonnull JournalableEntity<?> entity) {
            return entity.getDiscoveringTargetIds()
                .anyMatch(allowedTargetIds::contains);
        }
    }

    /**
     * Only allow entries related to changes performed by given operations.
     */
    @Immutable
    class FilterByOperation implements JournalFilter {
        private final Set<String> operationNames;

        public FilterByOperation(@Nonnull final OperationFilter operationFilter) {
            operationNames = new HashSet<>(operationFilter.getOperationNamesList());
        }

        @Override
        public boolean shouldEnter(@Nonnull JournalableOperation operation) {
            return operationNames.contains(operation.getOperationName()) ||
                operationNames.contains(operation.getClass().getSimpleName());
        }

        @Override
        public boolean shouldEnter(@Nonnull JournalableEntity<?> entity) {
            return true;
        }
    }
}
