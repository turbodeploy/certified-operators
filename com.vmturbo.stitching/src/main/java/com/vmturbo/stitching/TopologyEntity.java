package com.vmturbo.stitching;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;

/**
 * A wrapper around {@link TopologyEntityDTO.Builder}.Properties of the entity such as commodity values
 * may be updated, but relationships to other entities in the topology cannot be changed.
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
     * Discovery information about this entity. {@see DiscoveryInformation}
     * While this field is not marked as final, it is only set during the build of the entity.
     */
    private Optional<DiscoveryInformation> discoveryInformation;

    private TopologyEntity(@Nonnull final TopologyEntityDTO.Builder entity,
                           @Nonnull final List<TopologyEntity> consumers,
                           @Nonnull final List<TopologyEntity> providers) {
        this.entityBuilder = Objects.requireNonNull(entity);
        this.consumers = consumers;
        this.providers = providers;
        this.discoveryInformation = Optional.empty();
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
     * Get information describing when the entity was last updated and by which target(s)
     * this entity was discovered.
     *
     * @return {@link DiscoveryInformation} describing when and by which target(s) this entity
     *         was discovered.
     */
    public Optional<DiscoveryInformation> getDiscoveryInformation() {
        return discoveryInformation;
    }

    @Override
    public String toString() {
        return "(oid: " + getOid() + ", entityType: " + getEntityType() + ")";
    }

    /**
     * Set the discovery information for this entity.
     *
     * @param discoveryInformation Information describing when and by which targets this entity was discovered.
     */
    private void setDiscoveryInformation(@Nonnull final DiscoveryInformation discoveryInformation) {
        this.discoveryInformation = Optional.of(discoveryInformation);
    }

    /**
     * Create a new builder for constructing a {@link TopologyEntity}.
     *
     * @param entityBuilder The builder for the {@link TopologyEntityDTO.Builder} that the
     *                      {@link TopologyEntity} will wrap.
     * @return a new builder for constructing a {@link TopologyEntity}.
     */
    public static Builder newBuilder(@Nonnull final TopologyEntityDTO.Builder entityBuilder) {
        return new Builder(entityBuilder);
    }

    /**
     * Information describing when the entity was last updated and by which target(s) this entity was discovered.
     */
    public static class DiscoveryInformation {
        private final long targetId;
        private final long lastUpdatedTime;

        private DiscoveryInformation(final long targetId,
                                     final long lastUpdatedTime) {
            this.targetId = targetId;
            this.lastUpdatedTime = lastUpdatedTime;
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

        /**
         * Get the id of the target that discovered this entity. If the entity was never discovered (ie it is the
         * product of a clone or replace-by-template operation to add demand for a plan), it will return
         * {@link Optional#empty()}.
         *
         * TODO: Support for multiple targets discovering the entity.
         *
         * @return The ID of the target that discovered this entity.
         */
        public long getTargetId() {
            return targetId;
        }
    }

    /**
     * A builder for {@link com.vmturbo.stitching.TopologyEntity.DiscoveryInformation} that contains only
     * the targetId indicating which target discovered an entity.
     */
    public static class DiscoveredByInformationBuilder {
        private final long targetId;

        private DiscoveredByInformationBuilder(final long targetId) {
            this.targetId = targetId;
        }

        public DiscoveryInformation lastUpdatedAt(final long lastUpdateTime) {
            return new DiscoveryInformation(targetId, lastUpdateTime);
        }
    }

    public static DiscoveredByInformationBuilder discoveredBy(final long targetId) {
        return new DiscoveredByInformationBuilder(targetId);
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

        private Builder(@Nonnull final TopologyEntityDTO.Builder entityBuilder) {
            this.consumers = new ArrayList<>();
            this.providers = new ArrayList<>();
            this.associatedTopologyEntity = new TopologyEntity(entityBuilder,
                this.consumers, this.providers);
        }

        public TopologyEntity.Builder discoveryInformation(@Nonnull final DiscoveryInformation discoveryInformation) {
            associatedTopologyEntity.setDiscoveryInformation(discoveryInformation);
            return this;
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