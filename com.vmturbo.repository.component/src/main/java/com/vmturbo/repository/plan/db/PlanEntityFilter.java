package com.vmturbo.repository.plan.db;

import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.common.protobuf.repository.RepositoryDTO.RequestDetails;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyEntitiesRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.TopologyEntityFilter;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.TopologyType;
import com.vmturbo.common.protobuf.topology.ApiEntityType;

/**
 * Utility class to represent the various ways we can filter entities to return in plans.
 */
public class PlanEntityFilter {

    private final Set<Long> targetIds;
    private final Set<Short> targetTypes;
    private final boolean unplacedOnly;

    PlanEntityFilter(@Nullable final Set<Long> targetIds,
            @Nullable final Set<Short> targetTypes,
            final boolean unplacedOnly) {
        this.targetIds = targetIds;
        this.targetTypes = targetTypes;
        this.unplacedOnly = unplacedOnly;
    }

    /**
     * Converts various gRPC inputs into the relevatn {@link PlanEntityFilter}.
     */
    public static class PlanEntityFilterConverter {

        /**
         * Create a new {@link PlanEntityFilter} from a {@link TopologyEntityFilter}.
         *
         * {@link PlanEntityStore#getTopologySelection(long)} or
         * {@link PlanEntityStore#getTopologySelection(long, TopologyType)}
         * @param topologyEntityFilter The {@link TopologyEntityFilter}.
         * @return The {@link PlanEntityFilter}.
         */
        @Nonnull
        public PlanEntityFilter newPlanFilter(@Nonnull final TopologyEntityFilter topologyEntityFilter) {
            Set<Short> targetTypes = null;
            if (!topologyEntityFilter.getEntityTypesList().isEmpty()) {
                targetTypes = topologyEntityFilter.getEntityTypesList().stream()
                        .map(Integer::shortValue)
                        .collect(Collectors.toSet());
            }

            return new PlanEntityFilter(null, targetTypes, topologyEntityFilter.getUnplacedOnly());
        }

        /**
         * Create a new {@link PlanEntityFilter} from a {@link RequestDetails} in a plan stats
         * call.
         *
         * @param requestDetails The {@link RequestDetails}.
         * @return The {@link PlanEntityFilter}.
         */
        @Nonnull
        public PlanEntityFilter newPlanFilter(@Nonnull final RequestDetails requestDetails) {
            Set<Long> targetIds = null;
            if (requestDetails.hasEntityFilter()) {
                targetIds = new HashSet<>(requestDetails.getEntityFilter().getEntityIdsList());
            }

            Set<Short> targetTypes = null;
            if (requestDetails.hasRelatedEntityType()) {
                targetTypes = Collections.singleton((short)ApiEntityType.fromString(requestDetails.getRelatedEntityType()).typeNumber());
            }
            return new PlanEntityFilter(targetIds, targetTypes, false);
        }

        /**
         * Create a new {@link PlanEntityFilter} from a {@link RetrieveTopologyEntitiesRequest}.
         *
         * @param req The {@link RetrieveTopologyEntitiesRequest}.
         * @return The {@link PlanEntityFilter}.
         */
        @Nonnull
        public PlanEntityFilter newPlanFilter(@Nonnull final RetrieveTopologyEntitiesRequest req) {
            Set<Long> targetIds = null;
            if (!req.getEntityOidsList().isEmpty()) {
                targetIds = new HashSet<>(req.getEntityOidsList());
            }

            Set<Short> targetTypes = null;
            if (!req.getEntityTypeList().isEmpty()) {
                targetTypes = req.getEntityTypeList().stream()
                        .map(Integer::shortValue)
                        .collect(Collectors.toSet());
            }
            return new PlanEntityFilter(targetIds, targetTypes, false);
        }

    }

    /**
     * If true, return only entities that are unplaced.
     *
     * @return Whether or not only unplaced entities are requested.
     */
    public boolean getUnplacedOnly() {
        return unplacedOnly;
    }

    /**
     * Get the entity IDs targetted by this filter.
     *
     * @return An {@link Optional} containing the entity ids, or an empty optional to target all
     *         entities.
     */
    public Optional<Set<Long>> getTargetEntities() {
        return Optional.ofNullable(targetIds);
    }

    /**
     * Get the entity types targetted by this filter.
     *
     * @return An {@link Optional} containing the entity types, or an empty optional to target all
     *         entity types. We use {@link Short} to map to the database columns.
     */
    public Optional<Set<Short>> getTargetTypes() {
        return Optional.ofNullable(targetTypes);
    }
}
