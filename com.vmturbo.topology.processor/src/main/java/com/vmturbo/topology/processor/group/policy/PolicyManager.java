package com.vmturbo.topology.processor.group.policy;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

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
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyResponse;
import com.vmturbo.common.protobuf.group.PolicyServiceGrpc.PolicyServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.ReservationDTO.GetReservationByStatusRequest;
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
            final Iterator<PolicyResponse> policyIter =
                    policyService.getAllPolicies(PolicyRequest.newBuilder().build());
            final ImmutableList<PolicyResponse> policyResponses = ImmutableList.copyOf(policyIter);
            List<Policy> planOnlyPolicies = planOnlyPolicies(changes);
            final Map<Long, Grouping> groupsById = groupsById(policyResponses, planOnlyPolicies);
            final Map<PolicyDetailCase, Integer> policyTypeCounts = Maps.newEnumMap(PolicyDetailCase.class);

            final List<PlacementPolicy> policiesToApply = new ArrayList<>();

            final Map<Long, Set<Long>> policyConstraintMap =
                handleReservationConstraints(graph, changes, policiesToApply);

            getServerPolicies(changes, policyResponses, groupsById, policyConstraintMap)
                .forEach(policiesToApply::add);

            getPlanOnlyPolicies(planOnlyPolicies, groupsById)
                .forEach(policiesToApply::add);

            final PolicyApplicator.Results results =
                policyApplicator.applyPolicies(policiesToApply, groupResolver, graph);

            final long durationMs = System.currentTimeMillis() - startTime;
            logger.info("Completed application of {} policies in {}ms.", policyTypeCounts.size(), durationMs);

            if (!results.errors().isEmpty()) {
                logger.error(results.errors().size() + " policies could not be applied " +
                    "(error messages printed at debug level).");
                logger.debug(() -> results.errors().entrySet().stream()
                    .map(entry -> entry.getKey().getPolicyDefinition().getId() + " : " + entry.getValue().getMessage())
                    .collect(Collectors.joining("\n")));
            }

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
        // for all current active reservation, their status are RESERVED, and also
        // for reservations which just started, their status also have been
        // updated to RESERVED during Reservation stage.
        GetReservationByStatusRequest request = GetReservationByStatusRequest.newBuilder()
                .setStatus(ReservationStatus.RESERVED)
                .build();
        final Iterable<Reservation> activeReservations = () -> reservationService.getReservationByStatus(request);
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

    private Map<Long, Grouping> policyGroupsById(Collection<Policy> policies) {
        Set<Long> groupIds = policies.stream()
                        .map(GroupProtoUtil::getPolicyGroupIds)
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
                // Some desired groups are not found.
                // Throw an exception for now.
                // TODO (roman, Oct 20 2017): We can just not apply the policies that
                // are missing groups.
                throw new IllegalStateException("Policies have non-existing groups.");
            }
        }
        return groupsById;
    }

    /**
     * Obtain all {@link Group}s referenced by policies.
     *
     * @param policyResponses policies persisted in the policy service.
     * @param otherPolicies other policies, such as plan-only policies, that are not
     * persisted in the policy service.
     * @return mapping from group oid to group
     */
    private Map<Long, Grouping> groupsById(List<PolicyResponse> policyResponses, List<Policy> otherPolicies) {
        Set<Policy> policies = policyResponses.stream()
                        .filter(PolicyResponse::hasPolicy)
                        .map(PolicyResponse::getPolicy)
                        .collect(Collectors.toSet());
        policies.addAll(otherPolicies);
        return policyGroupsById(policies);
    }

    private Stream<PlacementPolicy> getServerPolicies(
                    @Nonnull List<ScenarioChange> changes,
                    @Nonnull List<PolicyResponse> policyResponses,
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

        return policyResponses.stream()
            .map(response -> {
                PlacementPolicy policy = policyFactory.newPolicy(response.getPolicy(), groupsById,
                    policyConstraintMap.getOrDefault(response.getPolicy().getId(),
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

    private List<Policy> planOnlyPolicies(List<ScenarioChange> changes) {
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
