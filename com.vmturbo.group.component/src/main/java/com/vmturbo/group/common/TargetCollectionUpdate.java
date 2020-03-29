package com.vmturbo.group.common;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Sets;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.TextFormat;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.exception.DataAccessException;

import com.vmturbo.common.protobuf.GroupProtoUtil;
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
 * @param <InstanceType> Instances of the collection have this type, e.g. {@link Policy}.
 * @param <SpecType> Specifications for instances of the collection have this type,
 *                  e.g. {@link PolicyInfo}.
 */
public abstract class TargetCollectionUpdate<InstanceType extends MessageOrBuilder,
    SpecType extends MessageOrBuilder> {

    private static final Logger logger = LogManager.getLogger();

    private final List<SpecType> groupsToAdd;

    private final Map<Long, SpecType> groupUpdates;

    private final Set<InstanceType>  groupsToRemove;

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
                    SpecType spec = getInfo(instance);
                    return new SpecKey(getNameFromInfo(spec),
                            getMemberType(spec), targetId);
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
                    final long oneId = getId(one);
                    final long otherId = getId(other);
                    groupsToRemove.add(oneId > otherId ? other : one);
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
            oidMap.put(e.getKey().toString(), getId(e.getValue())));

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
                        key -> getId(existingByKey.get(key)),
                        key -> transitAttribution(existingByKey.get(key), newByKey.get(key))));

        // Calculate groups to remove.
        // All entities that are NOT contained in the new discovered groups, but ARE contained
        // in the existing groups, need to be deleted.
        Sets.difference(existingByKey.keySet(), newByKey.keySet()).stream()
            .map(existingByKey::get)
            .forEach(groupsToRemove::add);
    }

    private SpecKey getSpecKey(SpecType instance) {
        return new SpecKey(getNameFromInfo(instance),
            getMemberType(instance), targetId);
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
     * Returns id from an instance.
     *
     * @param src instance to get id from
     * @return ID of an instance.
     */
    protected abstract long getId(@Nonnull InstanceType src);

    /**
     * Returns specs of an instance.
     *
     * @param src to get spec from
     * @return the spec of an instance.
     */
    protected abstract SpecType getInfo(@Nonnull InstanceType src);

    /**
     * Returns name from a specification.
     *
     * @param src spec to get name from
     * @return the name from a specification.
     */
    @Nonnull
    protected abstract String getNameFromInfo(@Nonnull SpecType src);

    /**
     * This is a workaround for bug OM-22036 - VM and PM groups have the same name.
     *
     * @param src spec
     * @param id id from an instance
     * @return an identifier of the spec for the purpose of mapping group to OID
     */
    @Nonnull
    protected abstract String getInfoForId(@Nonnull SpecType src, long id);

    /**
     * Returns the entity type of a specification.
     *
     * @param src spec to get member type from
     * @return the entity type of a specification.
     */
    protected abstract int getMemberType(SpecType src);

    protected abstract InstanceType createInstance(final long id, final SpecType spec);

    /**
     * Performes attributes transition from an instance to a specification.
     *
     * @param instance instance to transmit attributes from
     * @param src spect to transmit attributes from
     * @return A function to transit attributions from an instance to a specification.
     */
    protected abstract SpecType transitAttribution(InstanceType instance, SpecType src);

    @FunctionalInterface
    public interface StoreInstance<InstanceType> {
        void storeInstance(InstanceType instance) throws InvalidItemException, DataAccessException, DuplicateNameException;
    }

    @FunctionalInterface
    public interface UpdateInstance<InstanceType> {
        void updateInstance(InstanceType newInstance) throws InvalidItemException, ItemNotFoundException, DuplicateNameException, ImmutableUpdateException;
    }

    @FunctionalInterface
    public interface RemoveInstance<InstanceType> {
        void removeInstance(InstanceType instance) throws ItemNotFoundException, ImmutableUpdateException;
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
               @Nonnull final RemoveInstance<InstanceType> deleteFn) {
        groupsToAdd.forEach(groupInfo -> {
            try {
                long oid = identityProvider.next();
                storeFn.storeInstance(createInstance(oid, groupInfo));
                oidMap.put(getInfoForId(groupInfo, targetId), oid);
            } catch (InvalidItemException | DataAccessException | DuplicateNameException e) {
                logger.warn("Encountered exception trying to save group {}. Message: {}",
                        getNameFromInfo(groupInfo), e.getLocalizedMessage());
                // Ignore the exception - let the process continue.
            }
        });

        groupUpdates.forEach((groupId, newGroupInfo) -> {
            try {
                final InstanceType newInstance = createInstance(groupId, newGroupInfo);
                updateFn.updateInstance(newInstance);
                oidMap.forcePut(getInfoForId(newGroupInfo, targetId),
                        getId(newInstance));
            } catch (InvalidItemException | ImmutableUpdateException | ItemNotFoundException | DuplicateNameException e) {
                // Ignore the exception - let the process continue.
                logger.warn("Encountered exception trying to update group {}. Message: {}",
                        getNameFromInfo(newGroupInfo), e.getLocalizedMessage());
            }
        });

        groupsToRemove.forEach(group -> {
            try {
                deleteFn.removeInstance(group);
                oidMap.inverse().remove(getId(group));
            } catch (ImmutableUpdateException | ItemNotFoundException e) {
                // Ignore the exception - let the process continue.
                logger.warn("Encountered exception trying to delete group {}. Message: {}",
                    getId(group), e.getLocalizedMessage());
            }
        });

        return oidMap;
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
        protected long getId(@Nonnull Policy src) {
            return src.getId();
        }

        @Override
        protected PolicyInfo getInfo(@Nonnull Policy src) {
            return src.getPolicyInfo();
        }

        @Nonnull
        @Override
        protected String getNameFromInfo(@Nonnull PolicyInfo src) {
            return src.getName();
        }

        @Nonnull
        @Override
        protected String getInfoForId(@Nonnull PolicyInfo info, long id) {
            return String.join(GroupProtoUtil.GROUP_KEY_SEP,
                    info.getName(), String.valueOf(targetId));
        }

        @Override
        protected int getMemberType(PolicyInfo policy) {
            return policy.getPolicyDetailCase().getNumber();
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
        protected PolicyInfo transitAttribution(Policy existingPolicy, PolicyInfo newPolicyInfo) {
            return newPolicyInfo.toBuilder()
                    .setEnabled(existingPolicy.getPolicyInfo().getEnabled())
                    .build();
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
