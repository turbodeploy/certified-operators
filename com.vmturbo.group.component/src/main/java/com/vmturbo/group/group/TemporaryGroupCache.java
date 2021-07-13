package com.vmturbo.group.group;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import com.vmturbo.common.protobuf.action.ActionDTO.Severity;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.group.GroupDTO;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupDTO.MemberType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.EntityWithOnlyEnvironmentTypeAndTargets;
import com.vmturbo.group.identity.IdentityProvider;

/**
 * A cache for temporary groups that don't get saved to the database and expire after
 * a certain period of time.
 *
 * It's separated from {@link IGroupStore} because temporary groups do not get "stored" per se.
 * We don't want to over-complicate the {@link IGroupStore} implementation with persistend AND
 * transient information. Also, we intentionally want to separate temporary groups and make them
 * harder to find "by accident" so that they can't be used to create policies/setting policies.
 * This is a simple way to do that - validation code that queries the {@link IGroupStore}
 * to make sure groups exist will fail when given a temporary group ID.
 */
@ThreadSafe
public class TemporaryGroupCache {

    private final IdentityProvider identityProvider;

    private final GroupEnvironmentTypeResolver groupEnvironmentTypeResolver;

    private final GroupSeverityCalculator groupSeverityCalculator;

    private final Cache<Long, Grouping> tempGroupingCache;

    public TemporaryGroupCache(@Nonnull final IdentityProvider identityProvider,
                               @Nonnull GroupEnvironmentTypeResolver groupEnvironmentTypeResolver,
                               @Nonnull GroupSeverityCalculator groupSeverityCalculator,
                               final long expirationTime,
                               final TimeUnit expirationUnit) {
        this.identityProvider = Objects.requireNonNull(identityProvider);
        this.groupEnvironmentTypeResolver = Objects.requireNonNull(groupEnvironmentTypeResolver);
        this.groupSeverityCalculator = Objects.requireNonNull(groupSeverityCalculator);

        tempGroupingCache = CacheBuilder.newBuilder()
                        .expireAfterAccess(expirationTime, expirationUnit)
                        .build();
    }

    /**
     * Create a new temporary group. This call will create a new group with a new ID every time,
     * regardless of which temporary groups currently exist.
     *
     * @param groupDefinition The info object describing the group to create.
     * @param origin The info object describing properties of the origin of the group.
     * @param expectedTypes The list of expected entity and group types in this group.
     * @param entitiesWithEnvironment A list with the group's entities and info about their
     *                                environment, to be used in calculation of the group's
     *                                severity, environment & cloud type.
     * @return The newly created {@link Grouping}.
     */
    @Nonnull
    public Grouping create(@Nonnull final GroupDefinition groupDefinition,
            @Nonnull final GroupDTO.Origin origin,
            Collection<MemberType> expectedTypes,
            @Nonnull final List<PartialEntity> entitiesWithEnvironment) {
        final long oid = identityProvider.next();
        final GroupEnvironment calculatedGroupEnvironment;
        final Severity groupSeverity;

        // Calculate information derived from entities
            // calculate environment type based on members environment type
        calculatedGroupEnvironment =
                groupEnvironmentTypeResolver.calculateEnvironmentTypeFromEntities(oid,
                        entitiesWithEnvironment.stream()
                                .map(PartialEntity::getWithOnlyEnvironmentTypeAndTargets)
                                .collect(Collectors.toSet()));
        // calculate severity based on members severity
        groupSeverity = groupSeverityCalculator.calculateSeverity(
                entitiesWithEnvironment
                        .stream()
                        .map(PartialEntity::getWithOnlyEnvironmentTypeAndTargets)
                        .map(EntityWithOnlyEnvironmentTypeAndTargets::getOid)
                        .collect(Collectors.toSet()));
        // If environment type is explicitly specified (e.g. by the ui due to specific
        // conventions), use this instead of the calculated one.
        final EnvironmentType environmentType;
        if (groupDefinition.hasOptimizationMetadata()
                && groupDefinition.getOptimizationMetadata().hasEnvironmentType()) {
            environmentType = groupDefinition.getOptimizationMetadata().getEnvironmentType();
        } else {
            environmentType = calculatedGroupEnvironment.getEnvironmentType();
        }

        final Grouping group = Grouping.newBuilder()
                        .setId(oid)
                        .setOrigin(origin)
                        .setDefinition(groupDefinition)
                        .addAllExpectedTypes(expectedTypes)
                        .setSupportsMemberReverseLookup(false)
                        .setEnvironmentType(environmentType)
                        .setCloudType(calculatedGroupEnvironment.getCloudType())
                        .setSeverity(groupSeverity)
                        .build();

        tempGroupingCache.put(oid, group);
        return group;
    }

    /**
     * Deletes a temporary group with a particular ID. No effect if the group does not exist.
     *
     * @param id The id of the group to delete.
     * @return An optional that contains the deleted {@link Grouping}, or an empty optional if the
     *         group does not exist.
     */
    @Nonnull
    public Optional<Grouping> deleteGrouping(final long id) {
        final Optional<Grouping> existingGroup = getGrouping(id);
        tempGroupingCache.invalidate(id);
        return existingGroup;
    }

    /**
     * Get a temporary group by ID.
     *
     * @param id The target ID.
     * @return An {@link Optional} containing the group, if it exists.
     */
    @Nonnull
    public Optional<Grouping> getGrouping(long id) {
        return Optional.ofNullable(tempGroupingCache.getIfPresent(id));
    }
}
