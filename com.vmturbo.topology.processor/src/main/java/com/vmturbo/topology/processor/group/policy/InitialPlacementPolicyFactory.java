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
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.group.GroupDTO.Group.Type;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticGroupMembers;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.group.PolicyDTO.Policy;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.PlanChanges.InitialPlacementConstraint;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.processor.topology.TopologyEditorException;
import com.vmturbo.topology.processor.topology.TopologyGraph;

/**
 * Generate BindToGroup policy for initial placement. For each {@link InitialPlacementConstraint},
 * it represents one type of constraint which could be Cluster, Data center, Virtual data center and
 * it will get target members for each constraint type, and perform intersection between each target
 * members to get provider groups. And also it tries to get all entities created from templates as
 * consumer group. Finally, it creates BindToGroup policy for each provider group and consumer group.
 */
public class InitialPlacementPolicyFactory {
    private final Logger logger = LogManager.getLogger();

    private final GroupServiceBlockingStub groupServiceBlockingStub;

    public InitialPlacementPolicyFactory(@Nonnull final GroupServiceBlockingStub groupServiceBlockingStub) {
        this.groupServiceBlockingStub = Objects.requireNonNull(groupServiceBlockingStub);
    }

    /**
     * Generate BindToGroup policy for initial placement. And based on input the list of
     * {@link InitialPlacementConstraint}, it creates provider group. And also it creates consumer
     * group for all entities created from templates.
     *
     * @param graph The topology graph contains all entities and relations between entities.
     * @param constraints a list of {@link InitialPlacementConstraint}.
     * @return {@link BindToGroupPolicy}.
     */
    public PlacementPolicy generatePolicy(@Nonnull final TopologyGraph graph,
                                          @Nonnull final List<InitialPlacementConstraint> constraints) {
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
        final Set<Long> consumers = getConsumerMembersOfConstraint(graph);
        final Group pmProviderGroup = generateStaticGroup(pmEntities, EntityType.PHYSICAL_MACHINE_VALUE);
        final Group consumerGroup = generateStaticGroup(consumers, EntityType.VIRTUAL_MACHINE_VALUE);
        final Policy bindToGroupPolicyPM = generateBindToGroupPolicy(pmProviderGroup, consumerGroup);
        return new BindToGroupPolicy(bindToGroupPolicyPM, consumerGroup, pmProviderGroup);
    }

    /**
     * For each {@link InitialPlacementConstraint}, it will get one Map which key is entity type and
     * value is entity belongs to that constraint and entity type. Then it will perform intersection
     * between each Map to get final provider entities.
     *
     * @param graph The topology graph contains all entities and relations between entities.
     * @param constraints a list of {@link InitialPlacementConstraint}.
     * @return a Map which key is entity type and value is a set of entity which match all constraints.
     */
    private Map<Integer, Set<TopologyEntity>> performIntersectionWithConstraints(
            @Nonnull final TopologyGraph graph,
            @Nonnull final List<InitialPlacementConstraint> constraints) {
        final Map<Integer, Set<TopologyEntity>> providersMap = new HashMap<>();
        for (InitialPlacementConstraint constraint : constraints) {
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
     * Try to get target members for input {@link InitialPlacementConstraint}. And the constraint could be
     * Cluster, Data center and Virtual data center. Right now, the target entity type only could be
     * Physical machine, because right now initial placement only allow running with Virtual machine templates.
     *
     * @param graph The topology graph contains all entities and relations between entities.
     * @param constraint {@link InitialPlacementConstraint}
     * @return a Map which key is entity type and value is a set of entity which match input constraints.
     */
    @VisibleForTesting
    Map<Integer, Set<TopologyEntity>> getProviderMembersOfConstraint(
            @Nonnull final TopologyGraph graph,
            @Nonnull final InitialPlacementConstraint constraint) {
        switch (constraint.getType()) {
            case CLUSTER:
                final GetMembersRequest request = GetMembersRequest.newBuilder()
                        .setId(constraint.getConstraintId())
                        .build();
                final GetMembersResponse response = groupServiceBlockingStub.getMembers(request);
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
            default:
                throw new IllegalArgumentException("Constraint " + constraint.getType() +
                        " type is not support.");
        }
    }

    /**
     * Get all entities which created from templates. It use {@link TopologyEntity} last update time
     * to tell which entity is created from templates. Because for all template entities, those last
     * update time will be TopologyEntity.NEVER_UPDATED_TIME.
     *
     * @param graph The topology graph contains all entities and relations between entities.
     * @return a set of entities id which created from templates.
     */
    @VisibleForTesting
    Set<Long> getConsumerMembersOfConstraint(@Nonnull final TopologyGraph graph) {
        // only cloned or template entities' last update time are never been updated,
        // for initial placement, it only has template entities, so we use it to tell which
        // entities are created from templates.
        return graph.entities()
                .filter(entity -> !entity.getDiscoveryInformation().isPresent())
                .map(TopologyEntity::getOid)
                .collect(Collectors.toSet());
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
     * @return {@link Group}.
     */
    private Group generateStaticGroup(@Nonnull final Set<Long> members, final int entityType) {
        final Group staticGroup = Group.newBuilder()
                .setId(IdentityGenerator.next())
                .setType(Type.GROUP)
                .setGroup(GroupInfo.newBuilder()
                        .setStaticGroupMembers(StaticGroupMembers.newBuilder()
                                .addAllStaticMemberOids(members))
                        .setEntityType(entityType))
                .build();
        return staticGroup;
    }

    private Policy generateBindToGroupPolicy(@Nonnull final Group providerGroup,
                                             @Nonnull final Group consumerGroup) {
        final Policy bindToGroupPolicy = Policy.newBuilder()
                .setId(IdentityGenerator.next())
                .setEnabled(true)
                .setBindToGroup(Policy.BindToGroupPolicy.newBuilder()
                        .setConsumerGroupId(consumerGroup.getId())
                        .setProviderGroupId(providerGroup.getId()))
                .build();
        return bindToGroupPolicy;
    }
}