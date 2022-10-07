package com.vmturbo.topology.graph;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

/**
 * A graph that contains information about which entities own which entities.
 *
 * @param <T> The type of entity in the graph.
 */
@Immutable
public class OwnershipGraph<T> {

    private final Map<Long, Set<Long>> ownerToOwned;

    private final Map<Long, Long> ownedToOwner;

    private final Map<Long, T> membersById;

    private OwnershipGraph(@Nonnull final Map<Long, Set<Long>> ownerToOwned,
                           @Nonnull final Map<Long, Long> ownedToOwner,
                           @Nonnull final Map<Long, T> membersById) {
        this.ownerToOwned = Collections.unmodifiableMap(Objects.requireNonNull(ownerToOwned));
        this.ownedToOwner = Collections.unmodifiableMap(Objects.requireNonNull(ownedToOwner));
        this.membersById = Collections.unmodifiableMap(Objects.requireNonNull(membersById));
    }

    /**
     *  Get the size of the graph. Mainly for logging and debugging purposes.
     *
     * @return The number of ownership connections in the graph.
     */
    public int size() {
        return ownedToOwner.size();
    }

    /**
     * Return the owners of an entity.
     *
     * @param entity The entity to get the owner for.
     * @return An ordered list. The immediate owner of the entity will be returned first,
     *         followed by the owner's owner, and so on.
     */
    @Nonnull
    public List<T> getOwners(long entity) {
        final List<T> owners = new ArrayList<>();
        Long nextOwnerId = ownedToOwner.get(entity);
        while (nextOwnerId != null) {
            final T nextOwner = membersById.get(nextOwnerId);
            if (nextOwner != null) {
                owners.add(nextOwner);
            }
            nextOwnerId = ownedToOwner.get(nextOwnerId);
        }

        return owners;
    }

    /**
     * Get the entities owned by an entity.
     *
     * @param entity The target entity.
     * @param recursive If the returned stream should also include recursive entities.
     * @return The stream of entities owned by the owner. If recursive is true, also returns the
     *         stream of entities
     */
    @Nonnull
    public Set<Long> getOwned(final long entity, final boolean recursive) {
        if (recursive) {
            final Set<Long> allOwned = new HashSet<>();
            Set<Long> nextLevelOwned = ownerToOwned.getOrDefault(entity, Collections.emptySet());
            while (!nextLevelOwned.isEmpty()) {
                allOwned.addAll(nextLevelOwned);
                nextLevelOwned = nextLevelOwned.stream()
                    .flatMap(id -> ownerToOwned.getOrDefault(id, Collections.emptySet()).stream())
                    .collect(Collectors.toSet());
            }
            return allOwned;
        } else {
            return Collections.unmodifiableSet(ownerToOwned.getOrDefault(entity, Collections.emptySet()));
        }
    }

    /**
     * Extracts the OID from an entity in the {@link OwnershipGraph}.
     *
     * @param <T> The type of entity.
     */
    @FunctionalInterface
    public interface IdExtractor<T> {

        /**
         * Get the OID of the entity.
         *
         * @param graphEntity The entity.
         * @return The OID.
         */
        long getId(T graphEntity);

    }

    /**
     * Create a new graph builder. Add entities to it, and then use {@link Builder#build()}.
     *
     * @param idExtractor Method to extract the OID from the entity type.
     * @param <T> Type of entity in the graph.
     * @return {@link Builder} for the graph.
     */
    public static <T> Builder<T> newBuilder(@Nonnull final IdExtractor<T> idExtractor) {
        return new Builder<>(idExtractor);
    }

    /**
     * Create an empty {@link OwnershipGraph}.
     *
     * @param <T> Type of entity in the graph (irrelevant for empty case).
     * @return The {@link OwnershipGraph}.
     */
    public static <T> OwnershipGraph<T> empty() {
        return new OwnershipGraph<>(Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap());
    }

    /**
     * Builder for an {@link OwnershipGraph}.
     *
     * @param <T> Type of entity in the graph.
     */
    public static class Builder<T> {

        private final Map<Long, Set<Long>> ownerToOwned = new HashMap<>();

        private final Map<Long, Long> ownedToOwner = new HashMap<>();

        private final Map<Long, T> entitiesById = new HashMap<>();

        private final IdExtractor<T> idExtractor;

        private Builder(final IdExtractor<T> idExtractor) {
            this.idExtractor = idExtractor;
        }

        /**
         * Add an ownership relation to the graph, if it doesn't conflict with any ownership
         * relations already in the graph.
         *
         * @param owner The owner entity.
         * @param ownedId The ID of the owned entity.
         * @return True if the relationship does not conflict with any existing ones. False otherwise.
         */
        public boolean addOwner(final T owner, final long ownedId) {
            final long ownerId = idExtractor.getId(owner);
            // Check if there is a path from the owned entity to the owner - in which case
            // adding this relationship would introduce a cycle.
            if (checkPath(ownedId, ownerId)) {
                return false;
            }

            // Check if the owned entity already has an owner.
            // An entity should have only one owner.
            Long curOwnerId = ownedToOwner.get(ownedId);
            if (curOwnerId != null && curOwnerId != ownerId) {
                return false;
            }

            ownerToOwned.computeIfAbsent(ownerId, k -> new HashSet<>()).add(ownedId);
            ownedToOwner.put(ownedId, ownerId);
            entitiesById.put(ownerId, owner);
            return true;
        }

        /**
         * Check for an existing path in the graph.
         *
         * @param from The id of the "from" vertex.
         * @param to   The id of the "to" vertex.
         * @return true if there is a path from "from" vertex to "to".
         */
        private boolean checkPath(final long from, final long to) {
            Set<Long> nextHop = Collections.singleton(from);
            while (!nextHop.isEmpty()) {
                if (nextHop.contains(to)) {
                    return true;
                }
                nextHop = nextHop.stream()
                    .flatMap(id -> ownerToOwned.getOrDefault(id, Collections.emptySet()).stream())
                    .collect(Collectors.toSet());
            }
            return false;
        }

        /**
         * Build and return the graph.
         *
         * @return The {@link OwnershipGraph}.
         */
        public OwnershipGraph<T> build() {
            return new OwnershipGraph<>(ownerToOwned, ownedToOwner, entitiesById);
        }
    }
}
