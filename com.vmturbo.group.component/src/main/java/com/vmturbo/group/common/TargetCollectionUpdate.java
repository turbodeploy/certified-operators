package com.vmturbo.group.common;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.exception.DataAccessException;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Sets;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.TextFormat;

import com.vmturbo.common.protobuf.GroupProtoUtil;
import com.vmturbo.common.protobuf.group.GroupDTO.ClusterInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.group.GroupDTO.Group.Origin;
import com.vmturbo.common.protobuf.group.GroupDTO.Group.Type;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupInfo;
import com.vmturbo.common.protobuf.group.PolicyDTO.Policy;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyInfo;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicyInfo;
import com.vmturbo.group.identity.IdentityProvider;

/**
 * The {@link TargetCollectionUpdate} represents the specification for an update
 * to the discovered collections (i.e. clusters and groups) for a specific target.
 * Note - the "Collection" concept isn't something that really exists in the system.
 * This abstract class exists because there are lots of commonalities in the way we
 * process clusters and groups.
 *
 * <p>The purpose of this class is to separate the logic for how to apply the update
 * from the persistence logic, since the update logic is
 * independent of the storage logic.
 *
 * @param <InstanceType> Instances of the collection have this type, e.g. {@link Group}.
 * @param <SpecType> Specifications for instances of the collection have this type,
 *                  e.g. {@link GroupInfo}.
 */
public abstract class TargetCollectionUpdate<InstanceType extends MessageOrBuilder,
    SpecType extends MessageOrBuilder> {

    private static final Logger logger = LogManager.getLogger();

    private final List<SpecType> groupsToAdd;

    private final Map<Long, SpecType> groupUpdates;

    private final Set<Long>  groupsToRemove;

    private final IdentityProvider identityProvider;

    protected final long targetId;

    // A mapping from CollectionName to CollectionId
    protected BiMap<String, Long> oidMap = HashBiMap.create();

    /**
     * Create a new "update" given the existing collections and a list of new specs representing
     * the new discovered collections for a particular target. The update needs to be applied
     * (via {@link TargetCollectionUpdate#apply(StoreInstance, UpdateInstance, RemoveInstance)}) in order
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
                    return new SpecKey(infoNameGetter().apply(spec),
                            memberTypeGetter().apply(spec), targetId);
                },
                Function.identity(),
                // In case of conflicts, print a warning and just pick one.
                // This should never happen, because we shouldn't have saved colliding collections.
                (one, other) -> {
                    logger.error("Retrieved two existing colliding collections for the " +
                            "same target. Updating the one with the greater ID.: {} and {}",
                        TextFormat.shortDebugString(one), TextFormat.shortDebugString(other));
                    // We want to make an effort to clean up if there are existing duplicates.
                    // If there are two duplicate instances, keep the one with the higher key.
                    final long oneId = idGetter().apply(one);
                    final long otherId = idGetter().apply(other);
                    groupsToRemove.add(oneId > otherId ? otherId : oneId);
                    return oneId > otherId ? one : other;
                }));

        AtomicInteger dupeKeys = new AtomicInteger(0);
        final Map<SpecKey, SpecType> newByKey = newDiscoveredCollections.stream()
                .collect(Collectors.toMap(
                    this::getSpecKey,
                    Function.identity(),
                    (one, other) -> handleDuplicateSpecKey(one, other, dupeKeys)));
        if (dupeKeys.get() > 0) {
            logger.warn("Discovered {} pairs instances with collding keys");
        }

        existingByKey.entrySet().stream().forEach(e ->
            oidMap.put(e.getKey().toString(), idGetter().apply(e.getValue())));

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

    private SpecKey getSpecKey(SpecType instance) {
        return new SpecKey(infoNameGetter().apply(instance),
            memberTypeGetter().apply(instance), targetId);
    }

    private SpecType handleDuplicateSpecKey(SpecType one, SpecType other, AtomicInteger dupeKeys) {
        if (logger.isDebugEnabled()) {
            logger.debug("Discovered two colliding collections for the same target: {} and {}",
                TextFormat.shortDebugString(one), TextFormat.shortDebugString((other)));
            dupeKeys.incrementAndGet();
        }
        return one;
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
    protected abstract BiFunction<SpecType, Long, String> infoIdGetter();

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
        void storeInstance(InstanceType instance) throws InvalidItemException, DataAccessException, DuplicateNameException;
    }

    @FunctionalInterface
    public interface UpdateInstance<InstanceType> {
        void updateInstance(InstanceType newInstance) throws InvalidItemException, ItemNotFoundException, DuplicateNameException, ImmutableUpdateException;
    }

    @FunctionalInterface
    public interface RemoveInstance {
        void removeInstance(long instanceId) throws ItemNotFoundException, ImmutableUpdateException;
    }

    /**
     * Indicates whether updating an instances requires deletion of the old version of the instance.
     * If true, an update action will first issue a delete using the ID.
     *
     * @return True if updating an instance requires deletion of the prior version of that instance.
     */
    protected boolean updateRequiresDeletion() {
        return false;
    }

    /**
     * Apply the update, calling the provided functions to store and delete groups.
     *
     * @param storeFn The function to use to save an instance to the DB.
     * @param updateFn The function to use to update an instance in the DB.
     * @param deleteFn The function to use to delete an instance from the DB.
     * @return a mapping between instance name and OID, after all additions/deletions/updates.
     */
    public BiMap<String, Long> apply(@Nonnull final StoreInstance<InstanceType> storeFn,
               @Nonnull final UpdateInstance<InstanceType> updateFn,
               @Nonnull final RemoveInstance deleteFn) {
        groupsToAdd.forEach(groupInfo -> {
            try {
                long oid = identityProvider.next();
                storeFn.storeInstance(createInstance(oid, groupInfo));
                oidMap.put(infoIdGetter().apply(groupInfo, targetId), oid);
            } catch (InvalidItemException | DataAccessException | DuplicateNameException e) {
                logger.warn("Encountered exception trying to save group {}. Message: {}",
                        infoNameGetter().apply(groupInfo), e.getLocalizedMessage());
                // Ignore the exception - let the process continue.
            }
        });

        groupUpdates.forEach((groupId, newGroupInfo) -> {
            try {
                final InstanceType newInstance = createInstance(groupId, newGroupInfo);
                updateFn.updateInstance(newInstance);
                oidMap.forcePut(infoIdGetter().apply(newGroupInfo, targetId),
                        idGetter().apply(newInstance));
            } catch (InvalidItemException | ImmutableUpdateException | ItemNotFoundException | DuplicateNameException e) {
                // Ignore the exception - let the process continue.
                logger.warn("Encountered exception trying to update group {}. Message: {}",
                        infoNameGetter().apply(newGroupInfo), e.getLocalizedMessage());
            }
        });

        groupsToRemove.forEach(groupId -> {
            try {
                deleteFn.removeInstance(groupId);
                oidMap.inverse().remove(groupId);
            } catch (ImmutableUpdateException | ItemNotFoundException e) {
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
    public static class TargetGroupUpdate extends TargetCollectionUpdate<Group, GroupInfo> {

        public TargetGroupUpdate(final long targetId,
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
            return Group::getGroup;
        }

        @Override
        protected Function<GroupInfo, String> infoNameGetter() {
            return GroupInfo::getName;
        }

        @Override
        protected BiFunction<GroupInfo, Long, String> infoIdGetter() {
            return (info, id) -> GroupProtoUtil.discoveredIdFromName(info, id);
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
                    .setType(Type.GROUP)
                    .setOrigin(Origin.DISCOVERED)
                    .setGroup(info)
                    .build();
        }

        @Override
        protected BiFunction<Group, GroupInfo, GroupInfo> transitAttribution() {
            return (existingGroup, newGroupInfo) -> newGroupInfo;
        }
    }

    /**
     * An implementation of {@link TargetCollectionUpdate} to update {@link Group}s with
     * Type == CLUSTER.
     */
    public static class TargetClusterUpdate extends TargetCollectionUpdate<Group, ClusterInfo> {
        public TargetClusterUpdate(final long targetId,
                            @Nonnull final IdentityProvider identityProvider,
                            @Nonnull final List<ClusterInfo> newDiscoveredGroups,
                            @Nonnull final Collection<Group> existingClusters) {
            super(targetId, identityProvider, newDiscoveredGroups, existingClusters);
        }

        @Override
        protected Function<Group, Long> idGetter() {
            return Group::getId;
        }

        @Override
        protected Function<Group, ClusterInfo> infoGetter() {
            return Group::getCluster;
        }

        @Override
        protected Function<ClusterInfo, String> infoNameGetter() {
            return ClusterInfo::getName;
        }

        @Override
        protected BiFunction<ClusterInfo, Long, String> infoIdGetter() {
            return (info, id) -> GroupProtoUtil.discoveredIdFromName(info, id);
        }

        @Override
        protected Function<ClusterInfo, Integer> memberTypeGetter() {
            // Technically this is not the the type of the entity associated with the cluster,
            // but it works to distinguish clusters that have different member types.
            return info -> info.getClusterType().getNumber();
        }

        @Override
        protected Group createInstance(final long id, final ClusterInfo info) {
            return Group.newBuilder()
                    .setId(id)
                    .setTargetId(targetId)
                    .setType(Type.CLUSTER)
                    .setOrigin(Origin.DISCOVERED)
                    .setCluster(info)
                    .build();
        }

        @Override
        protected BiFunction<Group, ClusterInfo, ClusterInfo> transitAttribution() {
            return (existingCluster, newCluster) -> {
                // If the cluster has a specified headroom template, make sure to keep it.
                if (existingCluster.getCluster().hasClusterHeadroomTemplateId()) {
                    return newCluster.toBuilder()
                        .setClusterHeadroomTemplateId(existingCluster.getCluster().getClusterHeadroomTemplateId())
                        .build();
                } else {
                    return newCluster;
                }
            };
        }
    }

    /**
     * An implementation of {@link TargetCollectionUpdate} to update instances of {@link Policy}.
     *
     * Notice that here we already created a list of input policies from the discovered specs.
     */
    public static class TargetPolicyUpdate extends TargetCollectionUpdate<Policy, PolicyInfo> {
        public TargetPolicyUpdate(final long targetId,
                            @Nonnull final IdentityProvider identityProvider,
                            @Nonnull final List<PolicyInfo> newlyDiscoveredPolicies,
                            @Nonnull final Collection<Policy> existingPolicies) {
            super(targetId, identityProvider, newlyDiscoveredPolicies, existingPolicies);
        }

        @Override
        protected Function<Policy, Long> idGetter() {
            return Policy::getId;
        }

        @Override
        protected Function<Policy, PolicyInfo> infoGetter() {
            return Policy::getPolicyInfo;
        }

        @Override
        protected Function<PolicyInfo, String> infoNameGetter() {
            return PolicyInfo::getName;
        }

        @Override
        protected BiFunction<PolicyInfo, Long, String> infoIdGetter() {
            return (info, targetId) -> String.join(GroupProtoUtil.GROUP_KEY_SEP,
                    info.getName(), String.valueOf(targetId));
        }

        @Override
        protected Function<PolicyInfo, Integer> memberTypeGetter() {
            return policy -> policy.getPolicyDetailCase().getNumber();
        }

        @Override
        protected Policy createInstance(final long id, final PolicyInfo spec) {
            return Policy.newBuilder()
                .setId(id)
                .setTargetId(targetId)
                .setPolicyInfo(spec)
                .build();
        }

        /**
         * We can not reset the discovered policy's enabled attribution when uploading discovered policy,
         * because it could be disabled by users. And for following discoveries, enabled attribution
         * should be as same.
         * @return
         */
        @Override
        protected BiFunction<Policy, PolicyInfo, PolicyInfo> transitAttribution() {
            return (existingPolicy, newPolicyInfo) -> newPolicyInfo.toBuilder()
                    .setEnabled(existingPolicy.getPolicyInfo().getEnabled())
                    .build();
        }
    }

    /**
     * An implementation of {@link TargetCollectionUpdate} to update instances of {@link SettingPolicy}.
     */
    public static class TargetSettingPolicyUpdate extends TargetCollectionUpdate<SettingPolicy, SettingPolicyInfo> {
        public TargetSettingPolicyUpdate (final long targetId,
                                   @Nonnull final IdentityProvider identityProvider,
                                   @Nonnull final List<SettingPolicyInfo> newlyDiscoveredSettingPolicies,
                                   @Nonnull final Collection<SettingPolicy> existingSettingPolicies) {
            super(targetId, identityProvider, newlyDiscoveredSettingPolicies, existingSettingPolicies);
        }

        @Override
        protected Function<SettingPolicy, Long> idGetter() {
            return SettingPolicy::getId;
        }

        @Override
        protected Function<SettingPolicy, SettingPolicyInfo> infoGetter() {
            return SettingPolicy::getInfo;
        }

        @Override
        protected Function<SettingPolicyInfo, String> infoNameGetter() {
            return SettingPolicyInfo::getName;
        }

        @Override
        protected BiFunction<SettingPolicyInfo, Long, String> infoIdGetter() {
            return (info, id) -> String.join(GroupProtoUtil.GROUP_KEY_SEP, info.getName(),
                    String.valueOf(info.getEntityType()), String.valueOf(id));
        }

        @Override
        protected Function<SettingPolicyInfo, Integer> memberTypeGetter() {
            return SettingPolicyInfo::getEntityType;
        }

        @Override
        protected SettingPolicy createInstance(final long id, final SettingPolicyInfo settingPolicyInfo) {
            return SettingPolicy.newBuilder()
                .setId(id)
                .setInfo(settingPolicyInfo)
                .setSettingPolicyType(SettingPolicy.Type.DISCOVERED)
                .build();
        }

        @Override
        protected BiFunction<SettingPolicy, SettingPolicyInfo, SettingPolicyInfo> transitAttribution() {
            return (existingSettingPolicy, newSettingPolicyInfo) -> newSettingPolicyInfo;
        }

        /**
         * Because this update interface doesn't actually support an update function, we need to delete
         * the prior instance before inserting the new instance. This is not terribly efficient, so if
         * it becomes a performance issue we can look into actually supporting an update method.
         *
         * @return true because update does require deletion.
         */
        @Override
        protected boolean updateRequiresDeletion() {
            return true;
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
        private final long targetId;


        SpecKey(@Nonnull final String name,
                final int memberType,
                final long discovererId) {
            this.name = name;
            this.memberType = memberType;
            this.targetId = discovererId;
        }

        @Override
        public boolean equals(Object other) {
            if (other instanceof TargetCollectionUpdate.SpecKey) {
                final SpecKey otherKey = (SpecKey)other;
                return (otherKey.name.equals(name)
                        && otherKey.memberType == memberType
                        && otherKey.targetId == targetId);
            } else {
                return false;
            }
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, memberType, targetId);
        }

        @Override
        public String toString() {
            return String.join(GroupProtoUtil.GROUP_KEY_SEP, name, String.valueOf(memberType),
                    String.valueOf(targetId));
        }
    }

}
