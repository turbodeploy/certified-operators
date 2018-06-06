package com.vmturbo.group.group;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.commons.lang3.StringUtils;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.group.GroupDTO.Group.Origin;
import com.vmturbo.common.protobuf.group.GroupDTO.Group.Type;
import com.vmturbo.common.protobuf.group.GroupDTO.TempGroupInfo;
import com.vmturbo.group.identity.IdentityProvider;

/**
 * A cache for temporary groups that don't get saved to the database and expire after
 * a certain period of time.
 *
 * It's separated from {@link GroupStore} because temporary groups do not get "stored" per se.
 * We don't want to over-complicate the {@link GroupStore} implementation with persistend AND
 * transient information. Also, we intentionally want to separate temporary groups and make them
 * harder to find "by accident" so that they can't be used to create policies/setting policies.
 * This is a simple way to do that - validation code that queries the {@link GroupStore}
 * to make sure groups exist will fail when given a temporary group ID.
 */
@ThreadSafe
public class TemporaryGroupCache {

    private final IdentityProvider identityProvider;

    private final Cache<Long, Group> tempGroupCache;

    public TemporaryGroupCache(@Nonnull final IdentityProvider identityProvider,
                               final long expirationTime,
                               final TimeUnit expirationUnit) {
        this.identityProvider = Objects.requireNonNull(identityProvider);
        tempGroupCache = CacheBuilder.newBuilder()
            .expireAfterAccess(expirationTime, expirationUnit)
            .build();
    }

    /**
     * Create a new temporary group. This call will create a new group with a new ID every time,
     * regardless of which temporary groups currently exist.
     *
     * @param groupInfo The info object describing the group to create.
     * @return The newly created {@link Group}.
     * @throws InvalidTempGroupException If the info object describing the group is illegal.
     */
    @Nonnull
    public Group create(final TempGroupInfo groupInfo) throws InvalidTempGroupException {
        validateGroupInfo(groupInfo);
        final long oid = identityProvider.next();
        final Group group = Group.newBuilder()
                .setId(oid)
                .setOrigin(Origin.USER)
                .setType(Type.TEMP_GROUP)
                .setTempGroup(groupInfo)
                .build();
        tempGroupCache.put(oid, group);
        return group;
    }

    /**
     * Deletes a temporary group with a particular ID. No effect if the group does not exist.
     *
     * @param id The id of the group to delete.
     * @return An optional that contains the deleted {@link Group}, or an empty optional if the group does not exist.
     */
    public Optional<Group> delete(final long id) {
        final Optional<Group> existingGroup = get(id);
        tempGroupCache.invalidate(id);
        return existingGroup;
    }

    /**
     * Get a temporary group by ID.
     *
     * @param id The target ID.
     * @return An {@link Optional} containing the group, if it exists.
     */
    @Nonnull
    public Optional<Group> get(final long id) {
        return Optional.ofNullable(tempGroupCache.getIfPresent(id));
    }

    /**
     * Get all existing temporary groups.
     *
     * @return An unmodifiable, weakly consistent collection of currently existing groups. If
     *  new groups are added while the returned collection is being iterated, the new group may
     *  or may not appear in the iteration.
     */
    @Nonnull
    public Collection<Group> getAll() {
        // Iterators from the group cache's internal map are safe for concurrent use.
        return Collections.unmodifiableCollection(tempGroupCache.asMap().values());
    }

    private void validateGroupInfo(@Nonnull final TempGroupInfo groupInfo)
            throws InvalidTempGroupException {
        final List<String> errors = new ArrayList<>();
        if (!groupInfo.hasEntityType()) {
            errors.add("Temporary group must have an entity type!");
        }
        if (!groupInfo.hasName()) {
            errors.add("Temporary group must have a name!");
        }

        if (!errors.isEmpty()) {
            throw new InvalidTempGroupException(errors);
        }
    }

    /**
     * An exception thrown when the {@link TempGroupInfo} describing a group is illegal.
     */
    public static class InvalidTempGroupException extends Exception {

        public InvalidTempGroupException(final List<String> errors) {
            super("Errors in temporary group's info object: " + StringUtils.join(errors, "\n"));
        }
    }
}
