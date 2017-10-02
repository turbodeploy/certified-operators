package com.vmturbo.group.persistent;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Sets;

import com.vmturbo.common.protobuf.group.GroupDTO.Cluster;
import com.vmturbo.common.protobuf.group.GroupDTO.ClusterInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.group.GroupDTO.Group.Origin;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupInfo;
import com.vmturbo.common.protobuf.group.PolicyDTO.InputPolicy;
import com.vmturbo.common.protobuf.group.PolicyDTO.Policy;
import com.vmturbo.group.identity.IdentityProvider;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * The {@link TargetCollectionUpdate} represents the specification for an update
 * to the discovered collections (i.e. clusters and groups) for a specific target.
 * Note - the "Collection" concept isn't something that really exists in the system.
 * This abstract class exists because there are lots of commonalities in the way we
 * process clusters and groups.
 *
 * <p>The purpose of this class is to separate the logic for how to apply the update
 * from the {@link ClusterStore} and {@link GroupStore}, since the update logic is
 * independent of the storage logic.
 *
 * @param <InstanceType> Instances of the collection have this type, e.g. {@link Group}.
 * @param <SpecType> Specifications for instances of the collection have this type,
 *                  e.g. {@link GroupInfo}.
 */
abstract class TargetCollectionUpdate<InstanceType, SpecType> {

    private static final Logger logger = LogManager.getLogger();

    private final List<SpecType> groupsToAdd;

    private final Map<Long, SpecType> groupUpdates;

    private final Set<Long>  groupsToRemove;

    private final IdentityProvider identityProvider;

    protected final long targetId;

    // A mapping from entity name to entity oid
    protected BiMap<String, Long> oidMap = HashBiMap.create();

    /**
     * Create a new "update" given the existing collections and a list of new specs representing
     * the new discovered collections for a particular target. The update needs to be applied
     * (via {@link TargetCollectionUpdate#apply(StoreInstance, RemoveInstance)}) in order
     * to take effect.
     *
     * @param targetId The ID of the target, the groups of which the update applies to.
     * @param identityProvider The {@link IdentityProvider} to use to assign IDs to newly
     *                         created instances.
     * @param newDiscoveredCollections The new list of discovered collections. This list overrides
     *                                 the existing list - i.e. after applying the update only the
     *                                 instances with specs in this list will exist.
     * @param existingCollections The existing collections for this target.
     */
    protected TargetCollectionUpdate(final long targetId,
                           @Nonnull final IdentityProvider identityProvider,
                           @Nonnull final List<SpecType> newDiscoveredCollections,
                           @Nonnull final Collection<InstanceType> existingCollections) {
        this.targetId = targetId;
        this.identityProvider = identityProvider;

        groupsToRemove = new HashSet<>();
        final Map<SpecKey, InstanceType> existingByKey = existingCollections.stream()
                .collect(Collectors.toMap(instance -> {
                    SpecType spec = infoGetter().apply(instance);
                    return new SpecKey(infoNameGetter().apply(spec), memberTypeGetter().apply(spec));
                },
                Function.identity(),
                // In case of conflicts, print a warning and just pick one.
                // This should never happen, because we shouldn't have saved colliding collections.
                (one, other) -> {
                    logger.error("Retrieved two existing colliding collections for the " +
                            "same target. Updating the one with the greater ID.:\n{}\nand\n\n{}", one, other);
                    // We want to make an effort to clean up if there are existing duplicates.
                    // If there are two duplicate instances, keep the one with the higher key.
                    final long oneId = idGetter().apply(one);
                    final long otherId = idGetter().apply(other);
                    groupsToRemove.add(oneId > otherId ? otherId : oneId);
                    return oneId > otherId ? one : other;
                }));

        final Map<SpecKey, SpecType> newByKey = newDiscoveredCollections.stream()
                .collect(Collectors.toMap(
                    spec -> new SpecKey(infoNameGetter().apply(spec),
                        memberTypeGetter().apply(spec)),
                    Function.identity(),
                    (one, other) -> {
                        logger.warn("Discovered two colliding collections for the " +
                                "same target:\n{}\nand\n\n{}", one, other);
                        return one;
                    }));

        existingByKey.entrySet().stream().forEach(e ->
            oidMap.put(e.getKey().name, idGetter().apply(e.getValue())));

        // Here we iterate over the same thing three times. It's not ideal from a performance
        // standpoint, but the number of groups is relatively small and each iteration is quick,
        // so the improved readability of three discrete passes seems worth it.

        // Calculate groups to add.
        groupsToAdd = Sets.difference(newByKey.keySet(), existingByKey.keySet()).stream()
                .map(newByKey::get)
                .collect(Collectors.toList());

        // Calculate groups to modify.
        // (roman, Aug 2 2017): Right now we just pull the ID of the group, and create
        // a new group reusing that ID. In the future, if there are other runtime-properties
        // that change over a group's lifetime, we will want to only modify the spec of the
        // existing group.
        groupUpdates = Sets.intersection(newByKey.keySet(), existingByKey.keySet()).stream()
                .collect(Collectors.toMap(
                        key -> idGetter().apply(existingByKey.get(key)),
                        key -> transitAttribution().apply(existingByKey.get(key), newByKey.get(key))));

        // Calculate groups to remove.
        // All entities that are NOT contained in the new discovered groups, but ARE contained
        // in the existing groups, need to be deleted.
        Sets.difference(existingByKey.keySet(), newByKey.keySet()).stream()
            .map(key -> idGetter().apply(existingByKey.get(key)))
            .forEach(groupsToRemove::add);
    }

    /**
     * @return A function to get the ID of an instance.
     */
    protected abstract Function<InstanceType, Long> idGetter();

    /**
     * @return A function to get the spec of an instance.
     */
    protected abstract Function<InstanceType, SpecType> infoGetter();

    /**
     * @return A function to get the name from a specification.
     */
    protected abstract Function<SpecType, String> infoNameGetter();

    /**
     * This is a workaround for bug OM-22036 - VM and PM groups have the same name.
     * @return an identifier of the spec for the purpose of mapping group to OID
     */
    protected abstract Function<SpecType, String> infoIdGetter();

    /**
     * @return A function to get the entity type of a specification.
     */
    protected abstract Function<SpecType, Integer> memberTypeGetter();

    protected abstract InstanceType createInstance(final long id, final SpecType spec);

    /**
     * @return A function to transit attributions from an instance to a specification.
     */
    protected abstract BiFunction<InstanceType, SpecType, SpecType> transitAttribution();

    @FunctionalInterface
    public interface StoreInstance<InstanceType> {
        void storeInstance(InstanceType instance) throws DatabaseException;
    }

    @FunctionalInterface
    public interface RemoveInstance {
        void removeInstance(long instanceId) throws DatabaseException;
    }

    /**
     * Apply the update, calling the provided functions to store and delete groups.
     *
     * @param storeFn The function to use to save an instance to the DB
     * @param deleteFn The function to use to delete an instance from the DB
     * @return a mapping between instance name and OID, after all additions/deletions/updates
     */
    BiMap<String, Long> apply(@Nonnull final StoreInstance<InstanceType> storeFn,
               @Nonnull final RemoveInstance deleteFn) {
        groupsToAdd.forEach(groupInfo -> {
            try {
                long oid = identityProvider.next();
                storeFn.storeInstance(createInstance(oid, groupInfo));
                oidMap.put(infoIdGetter().apply(groupInfo), oid);
            } catch (DatabaseException e) {
                logger.warn("Encountered exception trying to save group {}. Message: {}",
                        infoNameGetter().apply(groupInfo), e.getLocalizedMessage());
                e.printStackTrace();
            }
        });

        groupUpdates.forEach((groupId, newGroupInfo) -> {
            try {
                InstanceType newInstance = createInstance(groupId, newGroupInfo);
                storeFn.storeInstance(newInstance);
                oidMap.forcePut(infoIdGetter().apply(newGroupInfo), idGetter().apply(newInstance));
            } catch (DatabaseException e) {
                // Ignore the exception - let the process continue.
                logger.warn("Encountered exception trying to update group {}. Message: {}",
                        infoNameGetter().apply(newGroupInfo), e.getLocalizedMessage());
            }
        });

        groupsToRemove.forEach(groupId -> {
            try {
                deleteFn.removeInstance(groupId);
                oidMap.inverse().remove(groupId);
            } catch (DatabaseException e) {
                // Ignore the exception - let the process continue.
                logger.warn("Encountered exception trying to delete group {}. Message: {}",
                    groupId, e.getLocalizedMessage());
            }
        });

        return oidMap;
    }

    /**
     * An implementation of {@link TargetCollectionUpdate} to update {@link Group}s.
     */
    static class TargetGroupUpdate extends TargetCollectionUpdate<Group, GroupInfo> {

        TargetGroupUpdate(final long targetId,
                          @Nonnull final IdentityProvider identityProvider,
                          @Nonnull final List<GroupInfo> newDiscoveredGroups,
                          @Nonnull final Collection<Group> existingGroups) {
            super(targetId, identityProvider, newDiscoveredGroups, existingGroups);
        }

        @Override
        protected Function<Group, Long> idGetter() {
            return Group::getId;
        }

        @Override
        protected Function<Group, GroupInfo> infoGetter() {
            return Group::getInfo;
        }

        @Override
        protected Function<GroupInfo, String> infoNameGetter() {
            return GroupInfo::getName;
        }

        @Override
        protected Function<GroupInfo, String> infoIdGetter() {
            return groupInfo ->
                groupInfo.getName()
                + "-"
                + EntityType.forNumber(groupInfo.getEntityType());
        }

        @Override
        protected Function<GroupInfo, Integer> memberTypeGetter() {
            return GroupInfo::getEntityType;
        }

        @Override
        protected Group createInstance(final long id, final GroupInfo info) {
            return Group.newBuilder()
                    .setId(id)
                    .setTargetId(targetId)
                    .setOrigin(Origin.DISCOVERED)
                    .setInfo(info)
                    .build();
        }

        @Override
        protected BiFunction<Group, GroupInfo, GroupInfo> transitAttribution() {
            return (existingGroup, newGroupInfo) -> newGroupInfo;
        }
    }

    /**
     * An implementation of {@link TargetCollectionUpdate} to update {@link Cluster}s.
     */
    static class TargetClusterUpdate extends TargetCollectionUpdate<Cluster, ClusterInfo> {
        TargetClusterUpdate(final long targetId,
                            @Nonnull final IdentityProvider identityProvider,
                            @Nonnull final List<ClusterInfo> newDiscoveredGroups,
                            @Nonnull final Collection<Cluster> existingClusters) {
            super(targetId, identityProvider, newDiscoveredGroups, existingClusters);
        }

        @Override
        protected Function<Cluster, Long> idGetter() {
            return Cluster::getId;
        }

        @Override
        protected Function<Cluster, ClusterInfo> infoGetter() {
            return Cluster::getInfo;
        }

        @Override
        protected Function<ClusterInfo, String> infoNameGetter() {
            return ClusterInfo::getName;
        }

        @Override
        protected Function<ClusterInfo, String> infoIdGetter() {
            return clusterInfo ->
            clusterInfo.getName()
                + "-"
                + (clusterInfo.getClusterType() == ClusterInfo.Type.COMPUTE
                    ? EntityType.PHYSICAL_MACHINE : EntityType.STORAGE);
        }

        @Override
        protected Function<ClusterInfo, Integer> memberTypeGetter() {
            // Technically this is not the the type of the entity associated with the cluster,
            // but it works to distinguish clusters that have different member types.
            return info -> info.getClusterType().getNumber();
        }

        @Override
        protected Cluster createInstance(final long id, final ClusterInfo info) {
            return Cluster.newBuilder()
                    .setId(id)
                    .setTargetId(targetId)
                    .setInfo(info)
                    .build();
        }

        @Override
        protected BiFunction<Cluster, ClusterInfo, ClusterInfo> transitAttribution() {
            return (existingCluster, newCluster) -> newCluster;
        }
    }

    /**
     * An implementation of {@link TargetCollectionUpdate} to update instances of {@link Policy}.
     *
     * Notice that here we already created a list of input policies from the discovered specs.
     */
    static class TargetPolicyUpdate extends TargetCollectionUpdate<InputPolicy, InputPolicy> {
        TargetPolicyUpdate(final long targetId,
                            @Nonnull final IdentityProvider identityProvider,
                            @Nonnull final List<InputPolicy> newlyDiscoveredPolicies,
                            @Nonnull final Collection<InputPolicy> existingPolicies) {
            super(targetId, identityProvider, newlyDiscoveredPolicies, existingPolicies);
        }

        @Override
        protected Function<InputPolicy, Long> idGetter() {
            return InputPolicy::getId;
        }

        @Override
        protected Function<InputPolicy, InputPolicy> infoGetter() {
            return Function.identity();
        }

        @Override
        protected Function<InputPolicy, String> infoNameGetter() {
            return InputPolicy::getName;
        }

        @Override
        protected Function<InputPolicy, String> infoIdGetter() {
            return InputPolicy::getName;
        }

        @Override
        protected Function<InputPolicy, Integer> memberTypeGetter() {
            return policy -> policy.getPolicyDetailCase().ordinal();
        }

        @Override
        protected InputPolicy createInstance(final long id, final InputPolicy spec) {
            return InputPolicy.newBuilder(spec)
                            .setId(id)
                            .setTargetId(targetId)
                            .build();
        }

        /**
         * We can not reset the discovered policy's enabled attribution when uploading discovered policy,
         * because it could be disabled by users. And for following discoveries, enabled attribution
         * should be as same.
         * @return
         */
        @Override
        protected BiFunction<InputPolicy, InputPolicy, InputPolicy> transitAttribution() {
            // Since enabled value default is true, we do not need to check if it exists or not.
            return (existingPolicy, newPolicy) -> (
                InputPolicy.newBuilder(newPolicy)
                    .setEnabled(existingPolicy.getEnabled())
                    .build()
            );
        }
    }

    /**
     * Used to compare for equality between specs. If a new discovered group
     * has a spec with the same key as an existing group, This is also intended to
     * determine when a new spec is meant to update an existing group.
     */
    private static class SpecKey {
        private final String name;
        private final int memberType;

        SpecKey(@Nonnull final String name,
                final int memberType) {
            this.name = name;
            this.memberType = memberType;
        }

        @Override
        public boolean equals(Object other) {
            if (other instanceof TargetCollectionUpdate.SpecKey) {
                final SpecKey otherKey = (SpecKey)other;
                return otherKey.name.equals(name) && otherKey.memberType == memberType;
            } else {
                return false;
            }
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, memberType);
        }
    }

}
