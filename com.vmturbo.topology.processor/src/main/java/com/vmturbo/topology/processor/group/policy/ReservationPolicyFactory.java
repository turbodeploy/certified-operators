package com.vmturbo.topology.processor.group.policy;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

import com.vmturbo.common.protobuf.group.GroupDTO;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupDTO.MemberType;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers.StaticMembersByType;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.group.PolicyDTO.Policy;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyInfo;
import com.vmturbo.common.protobuf.plan.ReservationDTO.Reservation;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationTemplateCollection.ReservationTemplate;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationTemplateCollection.ReservationTemplate.ReservationInstance;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ReservationConstraintInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.processor.group.policy.application.BindToGroupPolicy;
import com.vmturbo.topology.processor.group.policy.application.PlacementPolicy;
import com.vmturbo.topology.processor.group.policy.application.PolicyFactory.PolicyEntities;
import com.vmturbo.topology.processor.topology.TopologyEditorException;

/**
 * Generate BindToGroup policy for initial placement and Reservation. For each
 * {@link ReservationConstraintInfo}, it represents one type of constraint which could be Cluster,
 * Data center, Virtual data center and it will get target members for each constraint type, and
 * perform intersection between each target members to get provider groups. And also it tries to get
 * all entities created from templates as consumer group. Finally, it creates BindToGroup policy for
 * each provider group and consumer group.
 */
public class ReservationPolicyFactory {
    private final Logger logger = LogManager.getLogger();

    /**
     * The mediation layer uses this prefix for the keys of the network commodities.
     */
    private static final String NETWORK_COMMODITY_NAME_PREFIX = "Network::";

    private final GroupServiceBlockingStub groupServiceBlockingStub;

    public ReservationPolicyFactory(@Nonnull final GroupServiceBlockingStub groupServiceBlockingStub) {
        this.groupServiceBlockingStub = Objects.requireNonNull(groupServiceBlockingStub);
    }

    public PlacementPolicy generatePolicyForReservation(
            @Nonnull final TopologyGraph<TopologyEntity> graph,
            @Nonnull final List<ReservationConstraintInfo> constraints,
            @Nonnull final Reservation reservation) {
        // when we support different template types for reservation, we should generate generic provider group
        final Grouping pmProviderGroup = generateProviderGroup(graph, constraints);
        final Grouping consumerGroup = generateStaticGroup(new HashSet<>(), EntityType.VIRTUAL_MACHINE_VALUE);
        final Set<Long> consumerList = generateConsumerList(reservation);
        final Policy bindToGroupPolicyPM = generateBindToGroupPolicy(pmProviderGroup, consumerGroup);
        return new BindToGroupPolicy(bindToGroupPolicyPM, new PolicyEntities(
                consumerGroup, consumerList), new PolicyEntities(pmProviderGroup));
    }

    /**
     * generate a list of consumers for the new policy created for the reservation.
     * The list of consumers is in fact the reserved instances of the reservation.
     * @param reservation the reservation of interest.
     * @return a set of oids of all the reservation instances (consumers of the policy).
     */
    private Set<Long> generateConsumerList(@Nonnull final Reservation reservation) {
        return reservation.getReservationTemplateCollection()
                .getReservationTemplateList().stream()
                .map(ReservationTemplate::getReservationInstanceList)
                .flatMap(List::stream)
                .map(ReservationInstance::getEntityId)
                .collect(Collectors.toSet());
    }

    private Grouping generateProviderGroup(@Nonnull final TopologyGraph<TopologyEntity> graph,
                                        @Nonnull final List<ReservationConstraintInfo> constraints) {
        final Map<Integer, Set<TopologyEntity>> providersMap =
                performIntersectionWithConstraints(graph, constraints);
        // Right now, it only needs PM as providers, since initial placement only allow
        // virtual machine template, later on if add more template type, the provider types should be
        // configurable.
        final Set<Long> pmEntities =
                providersMap.getOrDefault(EntityType.PHYSICAL_MACHINE_VALUE, Collections.emptySet())
                        .stream()
                        .map(TopologyEntity::getOid)
                        .collect(Collectors.toSet());
        return generateStaticGroup(pmEntities, EntityType.PHYSICAL_MACHINE_VALUE);
    }

    /**
     * For each {@link ReservationConstraintInfo}, it will get one Map which key is entity type and
     * value is entity belongs to that constraint and entity type. Then it will perform intersection
     * between each Map to get final provider entities.
     *
     * @param graph The topology graph contains all entities and relations between entities.
     * @param constraints a list of {@link ReservationConstraintInfo}.
     * @return a Map which key is entity type and value is a set of entity which match all constraints.
     */
    private Map<Integer, Set<TopologyEntity>> performIntersectionWithConstraints(
            @Nonnull final TopologyGraph<TopologyEntity> graph,
            @Nonnull final List<ReservationConstraintInfo> constraints) {
        final Map<Integer, Set<TopologyEntity>> providersMap = new HashMap<>();
        for (ReservationConstraintInfo constraint : constraints) {
            final Map<Integer, Set<TopologyEntity>> providers = getProviderMembersOfConstraint(graph,
                    constraint);
            for (int entityType : providers.keySet()) {
                if (providersMap.containsKey(entityType)) {
                    final Set<TopologyEntity> originalProviders = providersMap.get(entityType);
                    final Set<TopologyEntity> newProviders = providers.get(entityType);
                    originalProviders.retainAll(newProviders);
                    providersMap.put(entityType, originalProviders);
                } else {
                    providersMap.put(entityType, providers.get(entityType));
                }
            }
        }
        return providersMap;
    }

    /**
     * Try to get target members for input {@link ReservationConstraintInfo}. And the constraint could be
     * Cluster, Data center and Virtual data center. Right now, the target entity type only could be
     * Physical machine, because right now initial placement only allow running with Virtual machine templates.
     *
     * @param graph The topology graph contains all entities and relations between entities.
     * @param constraint {@link ReservationConstraintInfo}
     * @return a Map which key is entity type and value is a set of entity which match input constraints.
     */
    @VisibleForTesting
    Map<Integer, Set<TopologyEntity>> getProviderMembersOfConstraint(
            @Nonnull final TopologyGraph<TopologyEntity> graph,
            @Nonnull final ReservationConstraintInfo constraint) {
        switch (constraint.getType()) {
            case CLUSTER:
                final GetMembersRequest request = GetMembersRequest.newBuilder()
                        .addId(constraint.getConstraintId())
                        .build();
                final GetMembersResponse response =
                        groupServiceBlockingStub.getMembers(request).next();
                final Set<TopologyEntity> topologyEntities = response.getMemberIdList().stream()
                        .map(graph::getEntity)
                        .flatMap(entity -> entity.isPresent() ? Stream.of(entity.get()) :
                                Stream.empty())
                        .collect(Collectors.toSet());
                return topologyEntities.stream()
                        .collect(Collectors.groupingBy(TopologyEntity::getEntityType,
                                Collectors.toSet()));
            case DATA_CENTER:
                final TopologyEntity entityDataCenter = graph.getEntity(constraint.getConstraintId())
                        .orElseThrow(() -> TopologyEditorException.notFoundEntityException(
                                constraint.getConstraintId()));
                // Right now (12/20/2017), it's hard code to only get PM as targets,
                // because initial placement only allow running with VM template, later on if adding
                // new template type, we can add more target types and make types are configurable.
                final Set<TopologyEntity> hostTargets = getConnectedEntityByType(Sets.newHashSet(entityDataCenter),
                        EntityType.PHYSICAL_MACHINE_VALUE, TopologyEntity::getConsumers);
                return hostTargets.stream()
                        .collect(Collectors.groupingBy(TopologyEntity::getEntityType,
                                Collectors.toSet()));
            case VIRTUAL_DATA_CENTER:
                final TopologyEntity entityVDC = graph.getEntity(constraint.getConstraintId())
                        .orElseThrow(() -> TopologyEditorException.notFoundEntityException(
                                constraint.getConstraintId()));
                final Set<TopologyEntity> pmTargets = getConnectedEntityByType(Sets.newHashSet(entityVDC),
                        EntityType.PHYSICAL_MACHINE_VALUE, TopologyEntity::getProviders);
                return pmTargets.stream()
                        .collect(Collectors.groupingBy(TopologyEntity::getEntityType,
                                Collectors.toSet()));
            case NETWORK:
                final TopologyEntity network = graph.getEntity(constraint.getConstraintId())
                    .orElseThrow(() -> TopologyEditorException.notFoundEntityException(
                        constraint.getConstraintId()));
                // At the time of this writing (Oct 1 2019) PMs are not directly connected to
                // networks. The relationship is expressed with a network commodity, with the name
                // of the network in the key.
                //
                // It's not worth to add the connections just to support the reservation
                // use case, because network constraints are not frequently used. So we just
                // iterate over all hosts in the environment and find the ones that sell the target
                // commodity.
                final String networkCommodityKey = NETWORK_COMMODITY_NAME_PREFIX + network.getDisplayName();
                Set<TopologyEntity> matchingHosts = graph.entitiesOfType(EntityType.PHYSICAL_MACHINE_VALUE)
                    .filter(host -> hostSellsNetwork(networkCommodityKey, host))
                    .collect(Collectors.toSet());
                logger.debug("Looking for hosts that sell network commodity {}. Found: {}",
                    () -> networkCommodityKey,
                    () -> matchingHosts.stream()
                        .map(TopologyEntity::toString)
                        .collect(Collectors.joining(", ")));
                return ImmutableMap.of(EntityType.PHYSICAL_MACHINE_VALUE, matchingHosts);
            case POLICY:
                // The "policy" case is handled separately.
                // When a reservation is constrained by an existing policy we don't create a NEW
                // policy to the reservation, but add the reservation's entity to the existing
                // policy (currently (Feb 2020) in the PolicyManager).
                // We can safely skip handling it here.
                logger.debug("Ignoring policy constraint (policy {}) during provider lookup.",
                    constraint.getConstraintId());
                return Collections.emptyMap();
            default:
                throw new IllegalArgumentException("Constraint " + constraint.getType() +
                        " type is not support.");
        }
    }

    private boolean hostSellsNetwork(@Nonnull final String networkCommodityName,
                                     @Nonnull final TopologyEntity entity) {
        Preconditions.checkArgument(entity.getEntityType() == EntityType.PHYSICAL_MACHINE_VALUE);
        return entity.getTopologyEntityDtoBuilder().getCommoditySoldListList().stream()
            .map(CommoditySoldDTO::getCommodityType)
            .filter(commSoldType -> commSoldType.getType() == CommodityType.NETWORK_VALUE)
            .anyMatch(commSoldType -> commSoldType.getKey().equals(networkCommodityName));
    }

    /**
     * Use BFS to traversing topology graph, start from input a set of entity and end with entities which
     * type is input target type. And also traversing direction is controlled by input function
     * interface.
     *
     * @param entities a set of {@link TopologyEntity} which will be start points.
     * @param targetType target entity type.
     * @param convert function interface to control transverse direction.
     * @return a set of {@link TopologyEntity}.
     */
    private Set<TopologyEntity> getConnectedEntityByType(
            @Nonnull final Set<TopologyEntity> entities,
            @Nonnull final Integer targetType,
            Function<TopologyEntity, List<TopologyEntity>> convert) {
        final Queue<TopologyEntity> queue = new LinkedList<>(entities);
        final Set<TopologyEntity> targetEntities = new HashSet<>();
        // use BFS to traversing topology graph and based on "convert" function interface to get
        // direction. Once it reaches target type entity, it will not traversing further.
        while (!queue.isEmpty()) {
            final TopologyEntity topEntity = queue.poll();
            if (targetType == topEntity.getEntityType()) {
                targetEntities.add(topEntity);
            } else {
                convert.apply(topEntity).stream()
                        .forEach(queue::offer);
            }
        }
        return targetEntities;
    }

    /**
     * Generate a static group for provider and consumer of BindToGroup policy.
     *
     * @param members a set of ids of group members.
     * @param entityType entity type of group.
     * @return {@link Grouping}.
     */
    private Grouping generateStaticGroup(@Nonnull final Set<Long> members, final int entityType) {
        //TODO (mahdi) this group is super weird. It does not a saved in group component
        final Grouping staticGroup = Grouping.newBuilder()
                .setId(IdentityGenerator.next())
                .addExpectedTypes(MemberType.newBuilder().setEntity(entityType))
                .setDefinition(GroupDefinition.newBuilder()
                    .setType(GroupType.REGULAR)
                    .setStaticGroupMembers(StaticMembers.newBuilder()
                        .addMembersByType(StaticMembersByType.newBuilder()
                            .setType(MemberType.newBuilder().setEntity(entityType))
                            .addAllMembers(members))))
                .setOrigin(GroupDTO.Origin.newBuilder().setSystem(
                    GroupDTO.Origin.System.newBuilder()
                        .setDescription("Reservation policy generated group")).build())
                .build();
        return staticGroup;
    }

    private Policy generateBindToGroupPolicy(@Nonnull final Grouping providerGroup,
                                             @Nonnull final Grouping consumerGroup) {
        final Policy bindToGroupPolicy = Policy.newBuilder()
                .setId(IdentityGenerator.next())
                .setPolicyInfo(PolicyInfo.newBuilder()
                    .setEnabled(true)
                    .setBindToGroup(PolicyInfo.BindToGroupPolicy.newBuilder()
                            .setConsumerGroupId(consumerGroup.getId())
                            .setProviderGroupId(providerGroup.getId())))
                .build();
        return bindToGroupPolicy;
    }
}