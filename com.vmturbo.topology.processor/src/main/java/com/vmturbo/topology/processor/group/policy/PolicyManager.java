package com.vmturbo.topology.processor.group.policy;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.GroupProtoUtil;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupFilter;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.group.PolicyDTO.Policy;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyInfo.PolicyDetailCase;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyRequest;
import com.vmturbo.common.protobuf.group.PolicyServiceGrpc.PolicyServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.ReservationDTO.GetAllReservationsRequest;
import com.vmturbo.common.protobuf.plan.ReservationDTO.Reservation;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationStatus;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationTemplateCollection.ReservationTemplate;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationTemplateCollection.ReservationTemplate.ReservationInstance;
import com.vmturbo.common.protobuf.plan.ReservationServiceGrpc.ReservationServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ReservationConstraintInfo;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.PlanChanges;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.PlanChanges.PolicyChange;
import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.proactivesupport.DataMetricTimer;
import com.vmturbo.stitching.DiscoveryOriginBuilder;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.processor.group.GroupResolver;
import com.vmturbo.topology.processor.group.policy.application.PlacementPolicy;
import com.vmturbo.topology.processor.group.policy.application.PolicyApplicator;
import com.vmturbo.topology.processor.group.policy.application.PolicyFactory;

/**
 * Responsible for the application of policies that affect the operation of the market.
 *
 * Policies are enforced by placing commodities on entities that control the behavior
 * of the affected entities.
 */
public class PolicyManager {

    private static final Logger logger = LogManager.getLogger();

    /**
     * The policy service from which to retrieve policy definitions.
     */
    private final PolicyServiceBlockingStub policyService;

    private final GroupServiceBlockingStub groupServiceBlockingStub;

    private final ReservationServiceBlockingStub reservationService;

    /**
     * The factory to use to create policies.
     */
    private final PolicyFactory policyFactory;

    private final PolicyApplicator policyApplicator;

    /**
     * The factory to create policy for initial placement and reservation purpose.
     */
    private final ReservationPolicyFactory reservationPolicyFactory;

    private static final DataMetricSummary POLICY_APPLICATION_SUMMARY = DataMetricSummary.builder()
        .withName("tp_policy_application_duration_seconds")
        .withHelp("Duration of applying policies to a topology.")
        .build()
        .register();

    /**
     * Create a new policy manager.
     *
     * @param policyService The service to use to retrieve policy definitions.
     * @param groupServiceBlockingStub Stub to access the remote group service.
     * @param policyFactory The factory used to create policies.
     */
    public PolicyManager(@Nonnull final PolicyServiceBlockingStub policyService,
                         @Nonnull final GroupServiceBlockingStub groupServiceBlockingStub,
                         @Nonnull final PolicyFactory policyFactory,
                         @Nonnull final ReservationPolicyFactory reservationPolicyFactory,
                         @Nonnull final ReservationServiceBlockingStub reservationService,
                         @Nonnull final PolicyApplicator policyApplicator) {
        this.policyService = Objects.requireNonNull(policyService);
        this.groupServiceBlockingStub = Objects.requireNonNull(groupServiceBlockingStub);
        this.policyFactory = Objects.requireNonNull(policyFactory);
        this.reservationPolicyFactory = Objects.requireNonNull(reservationPolicyFactory);
        this.reservationService = Objects.requireNonNull(reservationService);
        this.policyApplicator = Objects.requireNonNull(policyApplicator);
    }

    /**
     * Handler for policies that reference invalid groups.
     */
    @FunctionalInterface
    private interface InvalidGroupHandler {
        void policyWithMissingGroups(@Nonnull Policy policy, @Nonnull Set<Long> missingGroups);
    }

    private int clearPoliciesWithMissingGroups(@Nonnull final List<Policy> policies,
                                               @Nonnull final Map<Long, Grouping> existingGroupsById,
                                               @Nonnull final InvalidGroupHandler invalidGroupHandler) {
        final MutableInt numRemoved = new MutableInt(0);
        policies.removeIf(policy -> {
            final Set<Long> policyGroups = GroupProtoUtil.getPolicyGroupIds(policy);
            if (!existingGroupsById.keySet().containsAll(policyGroups)) {
                final Set<Long> missingGroups = Sets.difference(policyGroups, existingGroupsById.keySet());
                invalidGroupHandler.policyWithMissingGroups(policy, missingGroups);
                numRemoved.increment();
                return true;
            }
            return false;
        });
        return numRemoved.intValue();
    }

    /**
     * Apply policies from the policy service, possibly modified by plan changes,
     * to the entities in the topology graph.
     *
     * @param graph The topology graph on which to apply the policies.
     * @param groupResolver The resolver for the groups that the policy applies to.
     * @param changes list of plan changes to be applied to the policies
     * @return Map from (type of policy) -> (num of policies of the type)
     */
    public PolicyApplicator.Results applyPolicies(@Nonnull final TopologyGraph<TopologyEntity> graph,
                                                  @Nonnull final GroupResolver groupResolver,
                                                  @Nonnull final List<ScenarioChange> changes) {
        try (DataMetricTimer timer = POLICY_APPLICATION_SUMMARY.startTimer()) {
            final long startTime = System.currentTimeMillis();
            final List<Policy> livePolicies = new ArrayList<>();
            policyService.getAllPolicies(PolicyRequest.getDefaultInstance())
                .forEachRemaining(policyResp -> {
                    if (policyResp.hasPolicy()) {
                        livePolicies.add(policyResp.getPolicy());
                    }
                });
            final List<Policy> planOnlyPolicies = planOnlyPolicies(changes);
            final Map<Long, Grouping> groupsById = policyGroupsById(Stream.concat(livePolicies.stream(), planOnlyPolicies.stream()));

            int invalidPolicyCount = 0;
            invalidPolicyCount += clearPoliciesWithMissingGroups(livePolicies, groupsById, (policy, missingGroups) -> {
                if (policy.hasTargetId()) {
                    // TODO (roman, Feb 3 2020) OM-55089: Mark entities discovered by the target non-controllable.
                    throw new IllegalStateException(String.format(
                        "Policy %d (%s) discovered by target %d refers to non-existent groups %s.",
                        policy.getId(), policy.getPolicyInfo().getName(), policy.getTargetId(), missingGroups));
                } else {
                    logger.warn("User policy {} ({}) refers to non-existent groups {}." +
                        " Not applying the policy.", policy.getId(), policy.getPolicyInfo().getName(), missingGroups);
                }
            });

            invalidPolicyCount += clearPoliciesWithMissingGroups(planOnlyPolicies, groupsById, (policy, missingGroups) -> {
                logger.warn("Plan-only policy {} ({}) refers to non-existent groups {}." +
                    " Not applying the policy.", policy.getId(), policy.getPolicyInfo().getName(), missingGroups);
            });

            final Map<PolicyDetailCase, Integer> policyTypeCounts = Maps.newEnumMap(PolicyDetailCase.class);

            final List<PlacementPolicy> policiesToApply = new ArrayList<>();

            final Map<Long, Set<Long>> policyConstraintMap =
                handleReservationConstraints(graph, changes, policiesToApply);

            getServerPolicies(changes, livePolicies, groupsById, policyConstraintMap)
                .forEach(policiesToApply::add);

            getPlanOnlyPolicies(planOnlyPolicies, groupsById)
                .forEach(policiesToApply::add);

            final PolicyApplicator.Results results =
                policyApplicator.applyPolicies(policiesToApply, groupResolver, graph);

            final long durationMs = System.currentTimeMillis() - startTime;
            logger.info("Completed application of {} policies in {}ms.", policyTypeCounts.size(), durationMs);

            if (!results.getErrors().isEmpty()) {
                logger.error(results.getErrors().size() + " policies could not be applied " +
                    "(error messages printed at debug level).");
                logger.debug(() -> results.getErrors().entrySet().stream()
                    .map(entry -> entry.getKey().getPolicyDefinition().getId() + " : " + entry.getValue().getMessage())
                    .collect(Collectors.joining("\n")));
            }

            results.addInvalidPolicyCount(invalidPolicyCount);

            return results;
        }
    }

    /**
     * Handle initial placement and reservation constraints logic. For policy constraint, initial
     * placement and reservation entity will be added to related service policy's default consumer.
     * For Cluster, DataCenter, VDC constraints, it will be handled separately by adding a BindToGroup
     * policy to related consumer and providers.
     *
     * @param graph The topology graph on which to apply the policies.
     * @param scenarioChanges list of plan changes to be applied to the policies
     * @param policies The list of policies to add the reservation policies to. THIS METHOD MODIFIES THIS ARGUMENT.
     * @return a Map the key is policy id, value is related initial placement or reservation entity ids.
     */
    @VisibleForTesting
    Map<Long, Set<Long>> handleReservationConstraints(@Nonnull final TopologyGraph<TopologyEntity> graph,
                                              @Nonnull final List<ScenarioChange> scenarioChanges,
                                              @Nonnull List<PlacementPolicy> policies) {
        final Map<Long, Set<Long>> policyConstraintMap = new HashMap<>();

        final GetAllReservationsRequest allReservationsRequest = GetAllReservationsRequest
                .newBuilder().build();
        final Iterable<Reservation> allReservations = () ->
                reservationService.getAllReservations(allReservationsRequest);
        final Iterable<Reservation> activeReservations =
                StreamSupport.stream(allReservations.spliterator(), false)
                        .filter(reservation -> reservation.getStatus() == ReservationStatus.RESERVED ||
                                reservation.getStatus() == ReservationStatus.PLACEMENT_FAILED ||
                                reservation.getStatus() == ReservationStatus.INPROGRESS)
                        .collect(Collectors.toSet());

       final List<Long> initialPlacementPolicyConstraint = scenarioChanges.stream()
                .filter(ScenarioChange::hasPlanChanges)
                .flatMap(change -> change.getPlanChanges().getInitialPlacementConstraintsList().stream())
                .filter(ReservationConstraintInfo::hasType)
                .filter(constraint -> constraint.getType() == ReservationConstraintInfo.Type.POLICY)
                .map(ReservationConstraintInfo::getConstraintId)
                .collect(Collectors.toList());
        final Set<Long> initialPlacementEntityIds = initialPlacementPolicyConstraint.isEmpty() ?
                Collections.emptySet() : getNotDiscoveredEntityIds(graph);
        // add initial placement policy constraint to map
        initialPlacementPolicyConstraint.stream()
                .forEach(policyConstraint ->
                        policyConstraintMap.computeIfAbsent(policyConstraint,
                                constraint -> new HashSet<>()).addAll(initialPlacementEntityIds));
        // get and add reservation policy constraint to map
        final Map<Long, Set<Long>> reservationPolicyConstraint =
                getReservationPolicyConstraint(activeReservations);
        reservationPolicyConstraint.entrySet().stream()
                .forEach(entry -> policyConstraintMap.computeIfAbsent(entry.getKey(),
                        constraint -> new HashSet<>()).addAll(entry.getValue()));

        policies.addAll(createPoliciesForReservation(graph, activeReservations));
        createPolicyForInitialPlacement(graph, scenarioChanges,
                initialPlacementEntityIds.isEmpty() ? getNotDiscoveredEntityIds(graph) :
                Collections.emptySet()).ifPresent(policies::add);

        return policyConstraintMap;
    }

    /**
     * Get policy constraint for active reservations.
     *
     * @param activeReservations all active reservations.
     * @return a Map which key is policy constraint id, value is a set of related reservation entity ids.
     */
    private Map<Long, Set<Long>> getReservationPolicyConstraint(
            @Nonnull Iterable<Reservation> activeReservations) {
        final Map<Long, Set<Long>> reservationPolicyConstraintMap = new HashMap<>();
        StreamSupport.stream(activeReservations.spliterator(), false)
                .forEach(reservation ->
                    reservation.getConstraintInfoCollection().getReservationConstraintInfoList().stream()
                            .filter(ReservationConstraintInfo::hasType)
                            .filter(constraint ->
                                    constraint.getType() == ReservationConstraintInfo.Type.POLICY)
                            .forEach(constraint ->
                                reservationPolicyConstraintMap.computeIfAbsent(
                                        constraint.getConstraintId(), key -> new HashSet<>())
                                        .addAll(getReservationEntityId(reservation))));
        return reservationPolicyConstraintMap;
    }

    /**
     * Get all reservation entity id belong to input {@link Reservation}.
     *
     * @param reservation input {@link Reservation}.
     * @return a set of reservation entity ids.
     */
    private Set<Long> getReservationEntityId(@Nonnull final Reservation reservation) {
        return reservation.getReservationTemplateCollection().getReservationTemplateList().stream()
                .map(ReservationTemplate::getReservationInstanceList)
                .flatMap(List::stream)
                .map(ReservationInstance::getEntityId)
                .collect(Collectors.toSet());
    }

    private Map<Long, Grouping> policyGroupsById(@Nonnull final Stream<Policy> policies) {
        final Set<Long> groupIds = policies.map(GroupProtoUtil::getPolicyGroupIds)
            .flatMap(Set::stream)
            .collect(Collectors.toSet());

        final Map<Long, Grouping> groupsById = new HashMap<>(groupIds.size());
        if (!groupIds.isEmpty()) {
            groupServiceBlockingStub.getGroups(GetGroupsRequest.newBuilder()
                    .setGroupFilter(
                            GroupFilter.newBuilder().addAllId(groupIds).setIncludeHidden(true))
                    .setReplaceGroupPropertyWithGroupMembershipFilter(true)
                    .build()).forEachRemaining(group -> groupsById.put(group.getId(), group));

            if (groupsById.size() != groupIds.size()) {
                logger.warn("The following groups are missing from the groups service. " +
                    "Policies involving those groups will not apply: {}", Sets.difference(groupIds, groupsById.keySet()));
            }
        }
        return groupsById;
    }

    private Stream<PlacementPolicy> getServerPolicies(
                    @Nonnull List<ScenarioChange> changes,
                    @Nonnull List<Policy> livePolicies,
                    @Nonnull Map<Long, Grouping> groupsById,
                    @Nonnull Map<Long, Set<Long>> policyConstraintMap) {
        // Map from policy ID to whether it is enabled or disabled in the plan
        Map<Long, Boolean> policyOverrides = changes.stream()
                .filter(ScenarioChange::hasPlanChanges)
                .filter(change -> change.getPlanChanges().hasPolicyChange())
                .map(change -> change.getPlanChanges().getPolicyChange())
                .filter(PolicyChange::hasPolicyId)
                .collect(Collectors.toMap(
                        PolicyChange::getPolicyId, PolicyChange::getEnabled));

        return livePolicies.stream()
            .map(policyProto -> {
                PlacementPolicy policy = policyFactory.newPolicy(policyProto, groupsById,
                    policyConstraintMap.getOrDefault(policyProto.getId(),
                        Collections.emptySet()), Collections.emptySet());
                final Policy policyDef = policy.getPolicyDefinition();
                final long policyId = policyDef.getId();
                final Boolean enabledOverride = policyOverrides.get(policyId);
                if (enabledOverride != null) {
                    final Boolean enabled = policyDef.getPolicyInfo().getEnabled();
                    if (enabled != enabledOverride) {
                        // Create a copy of the policy with the 'enable' field taken from the plan
                        final Policy altPolicyDef = policyDef.toBuilder()
                            .setPolicyInfo(policyDef.getPolicyInfo().toBuilder()
                                .setEnabled(enabledOverride))
                            .build();
                        policy = policyFactory.newPolicy(altPolicyDef, groupsById,
                            policyConstraintMap.getOrDefault(altPolicyDef.getId(),
                                Collections.emptySet()), Collections.emptySet());
                    }
                }
                return policy;
            });
    }

    private Stream<PlacementPolicy> getPlanOnlyPolicies(
                    @Nonnull List<Policy> planOnlyPolicies,
                    @Nonnull Map<Long, Grouping> groupsById) {
        return planOnlyPolicies.stream()
            .map(policyDefinition -> {
                // Policy definition needs an ID, but plan-only policies don't have one
                policyDefinition = Policy.newBuilder(policyDefinition).setId(0).build();
                return policyFactory.newPolicy(policyDefinition, groupsById,
                    Collections.emptySet(), Collections.emptySet());
            });
    }

    private List<Policy> planOnlyPolicies(@Nonnull final List<ScenarioChange> changes) {
        return changes.stream()
            .filter(ScenarioChange::hasPlanChanges)
            .filter(change -> change.getPlanChanges().hasPolicyChange())
            .map(change -> change.getPlanChanges().getPolicyChange())
            .filter(PolicyChange::hasPlanOnlyPolicy)
            .map(PolicyChange::getPlanOnlyPolicy)
            .collect(Collectors.toList());
    }

    /**
     * Create and apply BindToGroup policy for initial placement. If there are ReservationConstraintInfo in
     * PlanChanges, it will call ReservationPolicyFactory to create BindToGroup policy and apply
     * this policy.
     *
     * @param graph The topology graph on which to apply the policies.
     * @param scenarioChanges list of plan changes to be applied to the policies.
     */
    private Optional<PlacementPolicy> createPolicyForInitialPlacement(
                @Nonnull final TopologyGraph<TopologyEntity> graph,
                @Nonnull final List<ScenarioChange> scenarioChanges,
                @Nonnull final Set<Long> consumers) {
        final List<ReservationConstraintInfo> constraints = scenarioChanges.stream()
                .filter(ScenarioChange::hasPlanChanges)
                .map(ScenarioChange::getPlanChanges)
                .map(PlanChanges::getInitialPlacementConstraintsList)
                .flatMap(List::stream)
                .filter(ReservationConstraintInfo::hasType)
                // filter out policy constraint, because all policy constraint will been applied along
                // with service policy.
                .filter(constraint -> constraint.getType() != ReservationConstraintInfo.Type.POLICY)
                .collect(Collectors.toList());
        if (!constraints.isEmpty()) {
            return Optional.of(reservationPolicyFactory.generatePolicyForInitialPlacement(
                graph, constraints, consumers));
        } else {
            return Optional.empty();
        }
    }

    private List<PlacementPolicy> createPoliciesForReservation(@Nonnull final TopologyGraph<TopologyEntity> graph,
                                            @Nonnull Iterable<Reservation> activeReservations) {
        return StreamSupport.stream(activeReservations.spliterator(), false)
            .filter(reservation ->  reservation.getConstraintInfoCollection()
                    .getReservationConstraintInfoList().stream()
                        // filter out policy constraint, because all policy constraint will been applied
                        // along with service policy.
                        .filter(ReservationConstraintInfo::hasType)
                        .filter(constraint -> constraint.getType()
                                != ReservationConstraintInfo.Type.POLICY)
                        .count() > 0)
            .map(reservation -> {
                final List<ReservationConstraintInfo> constraintInfos =
                        reservation.getConstraintInfoCollection()
                                .getReservationConstraintInfoList();
                return reservationPolicyFactory.generatePolicyForReservation(graph,
                                constraintInfos, reservation);
            })
            .collect(Collectors.toList());
    }


    /**
     * Get all entities which created from templates. It use {@link TopologyEntity}
     * {@link DiscoveryOriginBuilder} to tell which entity is created from templates.
     * Because for all template entities, discovery information is not present.
     *
     * @param graph The topology graph contains all entities and relations between entities.
     * @return a set of entities id which created from templates.
     */
    private Set<Long> getNotDiscoveredEntityIds(@Nonnull final TopologyGraph<TopologyEntity> graph) {
        // only cloned or template entities' last update time are never been updated,
        // for initial placement, it only has template entities, so we use it to tell which
        // entities are created from templates.
        return graph.entities()
                .filter(entity -> !entity.hasDiscoveryOrigin())
                .map(TopologyEntity::getOid)
                .collect(Collectors.toSet());
    }
}
