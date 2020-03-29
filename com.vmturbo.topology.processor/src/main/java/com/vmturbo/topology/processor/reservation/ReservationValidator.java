package com.vmturbo.topology.processor.reservation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupFilter;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyRequest;
import com.vmturbo.common.protobuf.group.PolicyServiceGrpc.PolicyServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.ReservationDTO.Reservation;
import com.vmturbo.common.protobuf.topology.ApiEntityType;

/**
 * Utility class to validate reservation constraints.
 * Used by the {@link ReservationManager} to avoid putting entities into the topology that will
 * have failing constraints further down the pipeline.
 */
public class ReservationValidator {

    private final GroupServiceBlockingStub groupService;

    private final PolicyServiceBlockingStub policyService;

    ReservationValidator(@Nonnull final GroupServiceBlockingStub groupService,
                         @Nonnull final PolicyServiceBlockingStub policyService) {
        this.groupService = Objects.requireNonNull(groupService);
        this.policyService = Objects.requireNonNull(policyService);
    }

    private void visitReservations(@Nonnull final List<Reservation> reservations,
                                   @Nonnull final ReservationConstraintVisitor visitor) {
        reservations.forEach(reservation -> {
            final long rId = reservation.getId();
            reservation.getConstraintInfoCollection().getReservationConstraintInfoList().forEach(constraint -> {
                final long constraintId = constraint.getConstraintId();
                switch (constraint.getType()) {
                    case CLUSTER:
                        visitor.onClusterConstraint(rId, constraintId);
                        break;
                    case DATA_CENTER:
                        visitor.onEntityConstraint(rId, ApiEntityType.DATACENTER, constraintId);
                        break;
                    case VIRTUAL_DATA_CENTER:
                        visitor.onEntityConstraint(rId, ApiEntityType.VIRTUAL_DATACENTER, constraintId);
                        break;
                    case NETWORK:
                        visitor.onEntityConstraint(rId, ApiEntityType.NETWORK, constraintId);
                        break;
                    case POLICY:
                        visitor.onPolicyConstraint(rId, constraintId);
                        break;
                    default:
                        // Nothing.
                }
            });
        });
    }

    /**
     * Validate a collection of reservations in bulk. Involves RPCs to other components.
     *
     * @param reservations Stream of {@link Reservation} objects.
     * @param entityPresenceCheck Function to check if an entity is present in the topology.
     * @return The {@link ValidationErrors} in the input reservations.
     */
    @Nonnull
    public ValidationErrors validateReservations(@Nonnull final Stream<Reservation> reservations,
                                     @Nonnull final Function<Long, Boolean> entityPresenceCheck) {
        final Set<Long> requiredClusters = new HashSet<>();
        final Set<Long> requiredEntities = new HashSet<>();
        final Set<Long> requiredPolicies = new HashSet<>();
        final List<Reservation> allReservations = reservations.collect(Collectors.toList());
        visitReservations(allReservations, new ReservationConstraintVisitor() {
            @Override
            public void onClusterConstraint(final long reservationId, final long clusterId) {
                requiredClusters.add(clusterId);
            }

            @Override
            public void onEntityConstraint(final long reservationId, @Nonnull final ApiEntityType type, final long entityId) {
                requiredEntities.add(entityId);
            }

            @Override
            public void onPolicyConstraint(final long reservationId, final long policyId) {
                requiredPolicies.add(policyId);
            }
        });

        final Set<Long> presentClusters;
        if (!requiredClusters.isEmpty()) {
            presentClusters = new HashSet<>();
            groupService.getGroups(GetGroupsRequest.newBuilder()
                .setGroupFilter(GroupFilter.newBuilder()
                    .addAllId(requiredClusters)
                    .setIncludeHidden(true))
                .build()).forEachRemaining(cluster -> presentClusters.add(cluster.getId()));
        } else {
            presentClusters = Collections.emptySet();
        }

        final Set<Long> presentEntities;
        if (!requiredEntities.isEmpty()) {
            presentEntities = requiredEntities.stream()
                .filter(entityPresenceCheck::apply)
                .collect(Collectors.toSet());
        } else {
            presentEntities = Collections.emptySet();
        }

        final Set<Long> presentPolicies;
        if (!requiredPolicies.isEmpty()) {
            presentPolicies = new HashSet<>();
            policyService.getAllPolicies(PolicyRequest.getDefaultInstance())
                .forEachRemaining(resp -> {
                    if (resp.hasPolicy()) {
                        presentPolicies.add(resp.getPolicy().getId());
                    }
                });
        } else {
            presentPolicies = Collections.emptySet();
        }

        final ValidationErrors errors = new ValidationErrors( );
        visitReservations(allReservations, new ReservationConstraintVisitor() {
            @Override
            public void onClusterConstraint(final long reservationId, final long clusterId) {
                if (!presentClusters.contains(clusterId)) {
                    errors.recordReferenceError(reservationId, "cluster", clusterId);
                }
            }

            @Override
            public void onEntityConstraint(final long reservationId, final ApiEntityType type, final long entityId) {
                if (!presentEntities.contains(entityId)) {
                    errors.recordReferenceError(reservationId, type.name(), entityId);
                }
            }

            @Override
            public void onPolicyConstraint(final long reservationId, final long policyId) {
                if (!presentPolicies.contains(policyId)) {
                    errors.recordReferenceError(reservationId, "policy", policyId);
                }
            }
        });
        return errors;
    }

    /**
     * Utility class to contain and work with errors encountered during reservation validation.
     */
    static class ValidationErrors {
        private final Map<Long, List<String>> errors = new HashMap<>();

        private void recordReferenceError(final long reservationId, final String referenceType, final long referencedId) {
            errors.computeIfAbsent(reservationId, k -> new ArrayList<>())
                .add("Invalid reference to missing " + referenceType + " - " + referencedId);
        }

        /**
         * Check if there are errors.
         *
         * @return True if there are validation errors.
         */
        public boolean isEmpty() {
            return errors.isEmpty();
        }

        /**
         * Get the errors, broken down by reservation.
         *
         * @return Map of reservation ID to errors encountered for that reservation.
         */
        @Nonnull
        public Map<Long, List<String>> getErrorsByReservation() {
            return errors;
        }
    }

    /**
     * Utility to help reuse some common code when looping over reservation constraints.
     */
    private interface ReservationConstraintVisitor {

        void onClusterConstraint(long reservationId, long clusterId);

        void onEntityConstraint(long reservationId, ApiEntityType type, long entityId);

        void onPolicyConstraint(long reservationId, long policyId);
    }
}
