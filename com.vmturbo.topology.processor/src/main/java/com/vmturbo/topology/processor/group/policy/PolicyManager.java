package com.vmturbo.topology.processor.group.policy;

import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.STORAGE_TIER_VALUE;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.STORAGE_VALUE;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.grpc.StatusRuntimeException;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.GroupProtoUtil;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupFilter;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.group.PolicyDTO.Policy;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyInfo.PolicyDetailCase;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyRequest;
import com.vmturbo.common.protobuf.group.PolicyServiceGrpc.PolicyServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProjectType;
import com.vmturbo.common.protobuf.plan.ReservationDTO.GetAllReservationsRequest;
import com.vmturbo.common.protobuf.plan.ReservationDTO.Reservation;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationStatus;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationTemplateCollection.ReservationTemplate;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationTemplateCollection.ReservationTemplate.ReservationInstance;
import com.vmturbo.common.protobuf.plan.ReservationServiceGrpc.ReservationServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ReservationConstraintInfo;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.PlanChanges.PolicyChange;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.TopologyMigration;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.TopologyMigration.MigrationReference;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.proactivesupport.DataMetricTimer;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.processor.entity.EntityNotFoundException;
import com.vmturbo.topology.processor.group.GroupResolver;
import com.vmturbo.topology.processor.group.policy.application.BindToGroupPolicy;
import com.vmturbo.topology.processor.group.policy.application.PlacementPolicy;
import com.vmturbo.topology.processor.group.policy.application.PolicyApplicator;
import com.vmturbo.topology.processor.group.policy.application.PolicyFactory;
import com.vmturbo.topology.processor.topology.CloudMigrationPlanUtils;

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
     * @param topologyInfo Information about the topology under construction.
     * @param sourceEntityOids OIDs of source entities that need to be migrated.
     * @return Map from (type of policy) -> (num of policies of the type)
     */
    public PolicyApplicator.Results applyPolicies(@Nonnull final TopologyGraph<TopologyEntity> graph,
                                                  @Nonnull final GroupResolver groupResolver,
                                                  @Nonnull final List<ScenarioChange> changes,
                                                  @Nonnull final TopologyInfo topologyInfo,
                                                  @Nonnull final Set<Long> sourceEntityOids) {
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

            // Reservations affect policies because we use policies to enforce reservation
            // constraints.
            final ReservationResults reservationResults =
                handleReservationConstraints(graph, changes, topologyInfo);

            final List<PlacementPolicy> policiesToApply =
                new ArrayList<>(reservationResults.getReservationPolicies());

            // Bind sources (consumers) to destinations (providers) in case of migration
            // NOTE: the resulting BindToGroupPolicy binds source entities to destination (PM/tier).
            policiesToApply.addAll(getMigrationPolicies(graph, topologyInfo, changes,
                    sourceEntityOids));

            // If we are doing migration, on-prem policies should be excluded.
            // TODO Uncomment the if condition after integrating with Joby's patch (OM-59350)
            //if (context.getPolicyGroups().isEmpty()) {
                getServerPolicies(changes, livePolicies, groupsById, reservationResults)
                        .forEach(policiesToApply::add);
            //}

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
     * Create a {@link Grouping} from a list of {@link MigrationReference} and a given type.
     * The resulting {@link Grouping} is never submitted to the group component, nor persisted elsewhere.
     *
     * @param entities a list of {@link MigrationReference} from which a single group is created
     * @param destinationEntityType the type of the group to be created
     * @return a {@link Grouping} to be used for creating a {@link PlacementPolicy}
     */
    private Grouping getStaticMigrationGroup(
            @Nonnull final List<MigrationReference> entities, int destinationEntityType) {
        boolean areGroups = true;
        Map<Boolean, Set<Long>> areGroupsToSourceOids = entities.stream()
                .collect(Collectors.groupingBy(aSource -> aSource.hasGroupType(),
                        Collectors.mapping(MigrationReference::getOid, Collectors.toSet())));

        Set<Long> staticGroupMemberOids = Sets.newHashSet();
        final Set<Long> groupEntities = areGroupsToSourceOids.get(areGroups);
        if (CollectionUtils.isNotEmpty(groupEntities)) {
            // Add the members of expanded groups
            groupServiceBlockingStub.getMembers(GetMembersRequest.newBuilder()
                    .addAllId(groupEntities)
                    .build())
                    .forEachRemaining(sourceGroup -> staticGroupMemberOids.addAll(sourceGroup.getMemberIdList()));
        }
        final Set<Long> nonGroupEntities = areGroupsToSourceOids.get(!areGroups);
        if (CollectionUtils.isNotEmpty(nonGroupEntities)) {
            // Add individual entities
            staticGroupMemberOids.addAll(nonGroupEntities);
        }
        return ReservationPolicyFactory.generateStaticGroup(
                staticGroupMemberOids,
                destinationEntityType);
    }

    /**
     * Gets a placement policy to place the workloads (VM/DBS) being migrated, onto the cloud tiers.
     * Create groups corresponding to the source and destination arguments of the {@param MigrateObjectApiDTO}, and add
     * cloud migration policies and {@link ScenarioChange.SettingOverride}s. To facilitate a cloud migration using
     * segmentation commodities, create source and destination groupings (unregistered with the group component) with
     * which to create a BindToGroupPolicy. This will enforce constraints that bind the consumers (workloads) to the set
     * provider(s) (regions).
     *
     * @param topologyMigration the object symbolizing an entity/group to migrate
     * @param sourceEntityOids OIDs of source entities that need to be migrated.
     * @return a list of {@link ScenarioChange} corresponding to the requested cloud migration plan
     */
    private PlacementPolicy getWorkloadMigrationPolicy(
            @Nonnull final TopologyMigration topologyMigration,
            @Nonnull final Set<Long> sourceEntityOids) {
        final EntityType workloadType = topologyMigration.getDestinationEntityType().equals(
                TopologyMigration.DestinationEntityType.VIRTUAL_MACHINE)
                ? EntityType.VIRTUAL_MACHINE
                : EntityType.DATABASE_SERVER;

        return getMigrationPolicy(sourceEntityOids.stream()
                        .map(sourceId -> MigrationReference.newBuilder().setOid(sourceId).build())
                        .collect(Collectors.toList()),
                workloadType,
                topologyMigration.getDestinationList(), EntityType.PHYSICAL_MACHINE);
    }

    @Nonnull
    private PlacementPolicy getMigrationPolicy(@Nonnull final List<MigrationReference> sourceRefs,
                                               EntityType sourceType,
                                               @Nonnull final List<MigrationReference> destinationRefs,
                                               EntityType destinationType) {
        final Grouping source = getStaticMigrationGroup(sourceRefs, sourceType.getNumber());
        final Grouping destination = getStaticMigrationGroup(destinationRefs,
                destinationType.getNumber());

        final Policy bindToGroupPolicy = ReservationPolicyFactory.generateBindToGroupPolicy(
                destination.getId(), source.getId());
        return new BindToGroupPolicy(bindToGroupPolicy,
                new PolicyFactory.PolicyEntities(source),
                new PolicyFactory.PolicyEntities(destination));
    }

    /**
     * Given a list of {@link ScenarioChange}s, identify those associated with cloud migrations, and generate
     * {@link BindToGroupPolicy} objects to enable them in the plan market.
     *
     * @param graph The topology graph.
     * @param topologyInfo Info about the plan topology.
     * @param scenarioChanges the current list of {@link ScenarioChange} objects generated in ScenarioMapper
     * @param sourceEntityOids OIDs of source entities that need to be migrated.
     * @return additional {@link ScenarioChange}s that should be applied in the current plan context
     */
    @VisibleForTesting
    List<PlacementPolicy> getMigrationPolicies(
            @Nonnull final TopologyGraph<TopologyEntity> graph,
            @Nonnull final TopologyInfo topologyInfo,
            @Nonnull final List<ScenarioChange> scenarioChanges,
            @Nonnull final Set<Long> sourceEntityOids) {
        List<PlacementPolicy> placementPolicies = Lists.newArrayList();
        for (ScenarioChange change : scenarioChanges) {
            if (change.hasTopologyMigration()) {
                placementPolicies.add(getWorkloadMigrationPolicy(change.getTopologyMigration(),
                        sourceEntityOids));
                placementPolicies.addAll(getVolumeMigrationPolicies(graph, topologyInfo,
                        sourceEntityOids));
            }
        }
        return placementPolicies;
    }

    /**
     * Gets a set of segmentation policies related to cloud migration of volumes. Source volumes
     * are forced into cloud storage tiers via these segmentation policies.
     *
     * @param graph Topology graph.
     * @param topologyInfo Info about plan topology.
     * @param sourceEntityOids Ids of source entities that are being migrated.
     * @return Set of placement policies, one for each volume attached to the source VM.
     */
    @Nonnull
    private Set<PlacementPolicy> getVolumeMigrationPolicies(
            @Nonnull final TopologyGraph<TopologyEntity> graph,
            @Nonnull final TopologyInfo topologyInfo,
            @Nonnull final Set<Long> sourceEntityOids) {
        Set<PlacementPolicy> volumePolicies = new HashSet<>();
        Set<Long> applicableCloudTiers = getCloudStorageTiers(graph, topologyInfo);
        final List<MigrationReference> destinationTierRefs = applicableCloudTiers.stream()
                .map(id -> MigrationReference.newBuilder().setOid(id).build())
                .collect(Collectors.toList());
        for (Long oid : sourceEntityOids) {
            final TopologyEntity sourceEntity = graph.getEntity(oid)
                    .orElseThrow(() -> new EntityNotFoundException("Missing Cloud Migration "
                            + "source entity " + oid));
            final Map<Long, Set<Long>> volumesByProvider = getStorageProviders(sourceEntity);
            final List<MigrationReference> sourceVolumeRefs = volumesByProvider.values()
                    .stream()
                    .flatMap(Collection::stream)
                    .map(id -> MigrationReference.newBuilder().setOid(id).build())
                    .collect(Collectors.toList());
            // TODO: Types needs to be double-checked, may not work for cloud-to-cloud.
            volumePolicies.add(getMigrationPolicy(sourceVolumeRefs, EntityType.VIRTUAL_VOLUME,
                    destinationTierRefs, EntityType.STORAGE));
        }
        return volumePolicies;
    }

    /**
     * Gets a set of applicable cloud storage tiers, taking into account the tiers that are
     * restricted based on the plan type. E.g for Lift_n_Shift, we only allow GP2 or ManagedPremium.
     *
     * @param graph Topology graph.
     * @param topologyInfo Info about plan topology.
     * @return Ids of applicable cloud storage tiers based on plan type.
     */
    @Nonnull
    private Set<Long> getCloudStorageTiers(@Nonnull final TopologyGraph<TopologyEntity> graph,
                                   @Nonnull final TopologyInfo topologyInfo) {
        return graph.entitiesOfType(EntityType.STORAGE_TIER)
                .filter(storageTier -> CloudMigrationPlanUtils.includeCloudStorageTier(storageTier,
                        topologyInfo))
                .map(TopologyEntity::getOid)
                .collect(Collectors.toSet());
    }

    /**
     * Gets a map of storage provider ids to the set of volume ids that are buying from that.
     *
     * @param entity Source entity being migrated.
     * @return Provider to volumes map.
     */
    @Nonnull
    private Map<Long, Set<Long>> getStorageProviders(@Nonnull final TopologyEntity entity) {
        final Map<Long, Set<Long>> volumesByProvider = new HashMap<>();
        TopologyEntityDTO.Builder dtoBuilder = entity.getTopologyEntityDtoBuilder();
        dtoBuilder.getCommoditiesBoughtFromProvidersList()
                .stream()
                .filter(commBought -> commBought.getProviderEntityType() == STORAGE_VALUE
                        || commBought.getProviderEntityType() == STORAGE_TIER_VALUE)
                .filter(commBought -> commBought.hasProviderId()
                                && commBought.hasVolumeId())
                .forEach(commBought -> {
                    volumesByProvider.computeIfAbsent(commBought.getProviderId(),
                            k -> new HashSet<>()).add(commBought.getVolumeId());
                });
        return volumesByProvider;
    }

    /**
     * Get the reservation status set that are active for the current topology.
     * @param topologyInfo the topologyInfo for which we need to find the active reservation status.
     * @return the set of active reservation statuses.
     */
    @Nonnull
    private Set<ReservationStatus> activeReservationTypes(@Nonnull final TopologyInfo topologyInfo) {
        final Set<ReservationStatus> ret;
        if (topologyInfo.getTopologyType() == TopologyType.REALTIME
                || topologyInfo.getPlanInfo().getPlanProjectType() ==
                PlanProjectType.CLUSTER_HEADROOM) {
            // In the cluster headroom plan we only want to consider already-placed reservations,
            // because they don't count as "placed".
            ret = ImmutableSet.of(
                    // We want to account for existing already-placed reservations...
                    ReservationStatus.RESERVED,
                    // As well as reservations that couldn't be placed earlier, but MAY be able to
                    // be placed now.
                    ReservationStatus.PLACEMENT_FAILED);

        } else if (topologyInfo.getPlanInfo().getPlanProjectType() ==
                PlanProjectType.RESERVATION_PLAN) {
            // In the reservation plan we only want to look at the constraints of the in-progress
            // reservations.
            ret = ImmutableSet.of(ReservationStatus.INPROGRESS);
        } else {
            // In other plans, we don't want to account for reservations at all.
            ret = Collections.emptySet();
        }

        return ret;
    }

    /**
     * Handle initial placement and reservation constraints logic. For policy constraint, initial
     * placement and reservation entity will be added to related service policy's default consumer.
     * For Cluster, DataCenter, VDC constraints, it will be handled separately by adding a BindToGroup
     * policy to related consumer and providers.
     *
     * @param graph The topology graph on which to apply the policies.
     * @param scenarioChanges list of plan changes to be applied to the policies
     * @param topologyInfo Information about the topology under construction.
     * @return a Map the key is policy id, value is related initial placement or reservation entity ids.
     */
    @VisibleForTesting
    ReservationResults handleReservationConstraints(@Nonnull final TopologyGraph<TopologyEntity> graph,
                                                    @Nonnull final List<ScenarioChange> scenarioChanges,
                                                    @Nonnull final TopologyInfo topologyInfo) {

        final GetAllReservationsRequest allReservationsRequest = GetAllReservationsRequest
                .newBuilder().build();
        final List<Reservation> activeReservations = new ArrayList<>();

        final Set<ReservationStatus> activeReservationTypes = activeReservationTypes(topologyInfo);

        try {
            reservationService.getAllReservations(allReservationsRequest).forEachRemaining(reservation -> {
                if (activeReservationTypes.contains(reservation.getStatus())) {
                    activeReservations.add(reservation);
                }
            });
        } catch (StatusRuntimeException e) {
            if (topologyInfo.getPlanInfo().getPlanProjectType() != PlanProjectType.RESERVATION_PLAN) {
                // In the live pipeline not having reservations is bad, but we shouldn't stop the
                // broadcast completely if there is a plan orchestrator error or if the plan orchestrator is down.
                logger.error("Failed to fetch reservations." +
                    " Will continue to run broadcast without reservation constraints in topology. Error: {}", e.getMessage());
            } else {
                // In a reservation plan, failure to respect reservation constraints is fatal.
                throw e;
            }
        }

        final ReservationResults results = new ReservationResults();

        // get and add reservation policy constraint to map
        getReservationPolicyConstraint(activeReservations, results);

        createPoliciesForReservation(graph, activeReservations, results);
        return results;
    }

    /**
     * Get policy constraint for active reservations.
     *
     * @param activeReservations all active reservations.
     * @param reservationResults container for the results of the reservations. Modified by the function!
     */
    private void getReservationPolicyConstraint(@Nonnull final List<Reservation> activeReservations,
                                                @Nonnull final ReservationResults reservationResults) {
        activeReservations.forEach(reservation -> {
            final Set<Long> reservationEntityIds = getReservationEntityId(reservation);
            reservation.getConstraintInfoCollection().getReservationConstraintInfoList().stream()
                .filter(ReservationConstraintInfo::hasType)
                .filter(constraint ->
                    constraint.getType() == ReservationConstraintInfo.Type.POLICY)
                .forEach(constraint -> {
                    reservationResults.addExtraConsumers(constraint.getConstraintId(), reservationEntityIds);
                });
        });
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
                    @Nonnull ReservationResults reservationResults) {
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
                    reservationResults.getExtraConsumersForPolicy(policyProto.getId()),
                    Collections.emptySet());
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
                            reservationResults.getExtraConsumersForPolicy(altPolicyDef.getId()),
                            Collections.emptySet());
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

    private void createPoliciesForReservation(@Nonnull final TopologyGraph<TopologyEntity> graph,
                                              @Nonnull List<Reservation> activeReservations,
                                              @Nonnull final ReservationResults reservationResults) {
        activeReservations.stream()
            .filter(reservation -> reservation.getConstraintInfoCollection()
                .getReservationConstraintInfoList().stream()
                // filter out policy constraint, because all policy constraint will been applied
                // along with service policy.
                .filter(ReservationConstraintInfo::hasType)
                .anyMatch(constraint -> constraint.getType() != ReservationConstraintInfo.Type.POLICY))
            .forEach(reservation -> {
                final List<ReservationConstraintInfo> constraintInfos =
                        reservation.getConstraintInfoCollection()
                            .getReservationConstraintInfoList();
                try {
                    final PlacementPolicy policy = reservationPolicyFactory.generatePolicyForReservation(graph,
                        constraintInfos, reservation);
                    reservationResults.addPolicy(policy);
                } catch (RuntimeException e) {
                    // We don't expect failures here, because the reservation constraints should
                    // have been validated by the ReservationManager. We may still get errors if the
                    // topology is modified between the reservation stage and the policy stage in
                    // a way that makes previously-valid constraints invalid. But if a single reservation
                    // fails we don't want to stop the whole broadcast!
                    logger.error("Failed to generate policy for reservation " + reservation.getId(), e);
                    reservationResults.recordError(reservation.getId(), e.getMessage());
                }
            });
    }

    /**
     * Data object to contain the results of handling reservation constraints.
     */
    public static class ReservationResults {

        /**
         * The extra consumers that will be tacked on to specific policies.
         * i.e. if a reserved entity is constrained by an existing policy, that reserved entity
         * will be added there.
         */
        private final Map<Long, Set<Long>> extraConsumersForPolicies = new HashMap<>();

        /**
         * Additional policies required to satisfy reservations.
         * e.g. if a reserved entity is constrained by a network, we create a new policy to ensure
         * that it will only be placed on hosts related to the network.
         */
        private final List<PlacementPolicy> additionalPolicies = new ArrayList<>();

        /**
         * Errors encountered during reservation handling, arranged by reservation ID.
         */
        private final Map<Long, List<String>> errorsByReservationId = new HashMap<>();

        void addExtraConsumers(final Long policyId, @Nonnull final Set<Long> extraConsumers) {
            extraConsumersForPolicies.computeIfAbsent(policyId, k -> new HashSet<>())
                .addAll(extraConsumers);
        }

        void addPolicy(final PlacementPolicy policy) {
            additionalPolicies.add(policy);
        }

        void recordError(final Long reservationId, @Nonnull final String errMsg) {
            errorsByReservationId.computeIfAbsent(reservationId, k -> new ArrayList<>()).add(errMsg);
        }

        @Nonnull
        Set<Long> getExtraConsumersForPolicy(final long policyId) {
            return extraConsumersForPolicies.getOrDefault(policyId, Collections.emptySet());
        }

        @Nonnull
        List<? extends PlacementPolicy> getReservationPolicies() {
            return additionalPolicies;
        }

        boolean hasErrors() {
            return !errorsByReservationId.isEmpty();
        }
    }
}
