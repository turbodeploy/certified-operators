package com.vmturbo.topology.processor.topology;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;

/**
 * A node in the {@link TopologyGraph}. Wraps a {@link TopologyEntityDTO.Builder}. Properties
 * of the entity such as commodity values may be updated, but relationships to other entities
 * in the topology cannot be changed.
 *
 * {@link TopologyEntity}s are equivalent when their OIDs are equal. It is an error to create two different
 * {@link TopologyEntity}s with the same OIDs and behavior is undefined when this is done.
 *
 * The TopologyEntityDTO.Builder within a TopologyEntity may be edited but the TopologyEntity is immutable otherwise.
 */
public class TopologyEntity {

    /**
     * A builder for the entity in the topology corresponding to this TopologyEntity.
     */
    private TopologyEntityDTO.Builder entityBuilder;

    /**
     * The set of all entities in the topology that consume commodities from this TopologyEntity.
     * {@see TopologyEntity#getConsumers}
     */
    private final List<TopologyEntity> consumers;

    /**
     * The set of all entities in the topology that provide commodities to this TopologyEntity.
     * {@see TopologyEntity#getProviders}
     */
    private final List<TopologyEntity> providers;

    /**
     * The time that the data for this entity was last updated.
     * {@see TopologyEntity#getLastUpdatedTime} for additional details.
     */
    private final long lastUpdatedTime;

    /**
     * A value for a time indicating the entity was never updated via a target discovery. When an entity has a
     * lastUpdatedTime matching this value, it is usually a product of something like a clone or replace-by-template
     * as part of a plan.
     */
    public static final long NEVER_UPDATED_TIME = -1;

    private TopologyEntity(@Nonnull final TopologyEntityDTO.Builder entity,
                           @Nonnull final List<TopologyEntity> consumers,
                           @Nonnull final List<TopologyEntity> providers,
                           final long lastUpdatedTime) {
        this.entityBuilder = Objects.requireNonNull(entity);
        this.consumers = consumers;
        this.providers = providers;
        this.lastUpdatedTime = lastUpdatedTime;
    }

    /**
     * Get the OID for this TopologyEntity.
     *
     * @return the OID for this TopologyEntity.
     */
    public long getOid() {
        return entityBuilder.getOid();
    }

    /**
     * Get the entityType. This field corresponds to {@link TopologyEntityDTO#getEntityType()}
     *
     * @return The entityType for the entityBuilder corresponding to this node.
     */
    public int getEntityType() {
        return entityBuilder.getEntityType();
    }

    /**
     * Get the display name for this entity.
     *
     * @return The display name for this entity.
     */
    public String getDisplayName() {
        return entityBuilder.getDisplayName();
    }

    /**
     * Get a builder for the entity associated with this TopologyEntity.
     * The builder may be mutated to modify the properties of the entity.
     *
     * DO NOT modify the providers or consumers of the entity or the graph will be invalidated. This
     * means that commodities bought and sold by entities should not be modified in a way that they change
     * which entities are buying or selling commodities to each other. Properties of commodities such as
     * used or capacity values that don't change which entities are involved in the buying or selling may
     * be modified without issue.
     *
     * @return The property information for the entity associated with this TopologyEntity.
     */
    @Nonnull
    public TopologyEntityDTO.Builder getTopologyEntityDtoBuilder() {
        return entityBuilder;
    }

    /**
     * Get the set of all entities in the topology that consume commodities from this TopologyEntity.
     *
     * Note that although the consumers are held in a list (for memory consumption reasons), entries
     * in the list are unique.
     *
     * If a {@link TopologyEntity} is in this list, it indicates that entity buys one or more commodities from
     * this {@link TopologyEntity}. A consumes relationship is one-to-one with a provides relation, so if an
     * {@link TopologyEntity} appears in this entity's consumers list, this {@link TopologyEntity} will appear
     * in that entity's providers list.
     *
     * The consumers list cannot be modified.
     *
     * @return  All {@link TopologyEntity}s that consume commodities from this {@link TopologyEntity}.
     */
    @Nonnull
    public List<TopologyEntity> getConsumers() {
        return Collections.unmodifiableList(consumers);
    }

    /**
     * Get the set of all entities in the topology that provide commodities to this TopologyEntity.
     *
     * Note that although the providers are held in a list (for memory consumption reasons), entries
     * in the list are unique.
     *
     * If a {@link TopologyEntity} is in this list, it indicates this entity sells one or more commodities to
     * that {@link TopologyEntity}. A provides relationship is one-to-one with a consumes relation, so if an
     * {@link TopologyEntity} appears in this entity's providers list, this {@link TopologyEntity} will appear
     * in that entity's consumers list.
     *
     * The providers list cannot be modified.
     *
     * @return  All {@link TopologyEntity}s that provide commodities to this {@link TopologyEntity}.
     */
    @Nonnull
    public List<TopologyEntity> getProviders() {
        return Collections.unmodifiableList(providers);
    }

    /**
     * Get the time that the data for this entity was last updated.
     * If the entity was discovered by multiple targets, this time is the time at which the most recent update
     * across all those targets provided new information for this entity.
     *
     * Important note: This is the time that TopologyProcessor received this data from the probe, not the actual
     * time that the probe retrieved the information from the target.
     *
     * This field may be used as a heuristic for the recency of the data in the absence of better information.
     * The time is in "computer time" and not necessarily UTC, however, times on {@link TopologyEntity}s
     * are comparable. See {@link System#currentTimeMillis()} for further details.
     *
     * @return The time that the data for this entity was last updated.
     */
    public long getLastUpdatedTime() {
        return lastUpdatedTime;
    }

    @Override
    public String toString() {
        return "(oid: " + getOid() + ", entityType: " + getEntityType() + ")";
    }

    /**
     * Create a new builder for constructing a {@link TopologyEntity}.
     *
     * @param entityBuilder The builder for the {@link TopologyEntityDTO.Builder} that the
     *                      {@link TopologyEntity} will wrap.
     * @param lastUpdatedTime The last time at which this entity was updated.
     *                        {@see TopologyEntity#getlastUpdatedTime()}. A time < 0 indicates that the entity was
     *                        never discovered or updated by a target (ie it is the product of a clone in a plan).
     * @return a new builder for constructing a {@link TopologyEntity}.
     */
    public static Builder newBuilder(@Nonnull final TopologyEntityDTO.Builder entityBuilder,
                                     final long lastUpdatedTime) {
        return new Builder(entityBuilder, lastUpdatedTime);
    }

    /**
     * A builder for constructing a {@link TopologyEntity}.
     * See {@link TopologyEntity} for further details.
     */
    public static class Builder {
        /**
         * Consumers and providers are ArrayLists so that {@link ArrayList#trimToSize()} can be called
         * upon building the {@link TopologyEntity}.
         */
        private final ArrayList<TopologyEntity> consumers;
        private final ArrayList<TopologyEntity> providers;

        private final TopologyEntity associatedTopologyEntity;

        private Builder(@Nonnull final TopologyEntityDTO.Builder entityBuilder,
                        final long lastUpdatedTime) {
            this.consumers = new ArrayList<>();
            this.providers = new ArrayList<>();
            this.associatedTopologyEntity = new TopologyEntity(entityBuilder,
                this.consumers, this.providers, lastUpdatedTime);
        }

        public long getLastUpdatedTime() {
            return associatedTopologyEntity.getLastUpdatedTime();
        }

        public void addConsumer(@Nonnull final TopologyEntity.Builder consumer) {
            consumers.add(consumer.associatedTopologyEntity);
        }

        public void addProvider(@Nonnull final TopologyEntity.Builder provider) {
            providers.add(provider.associatedTopologyEntity);
        }

        public long getOid() {
            return associatedTopologyEntity.getOid();
        }

        public int getEntityType() {
            return associatedTopologyEntity.getEntityType();
        }

        public String getDisplayName() {
            return associatedTopologyEntity.getDisplayName();
        }

        public TopologyEntityDTO.Builder getEntityBuilder() {
            return associatedTopologyEntity.getTopologyEntityDtoBuilder();
        }

        public TopologyEntity build() {
            // Trim the arrays to their capacity to reduce memory consumption since
            // the consumers and providers will not be modified after the entity has been built.
            consumers.trimToSize();
            providers.trimToSize();

            return associatedTopologyEntity;
        }
    }
}