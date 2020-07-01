package com.vmturbo.topology.processor.entity;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.ThreadSafe;

import com.google.common.annotations.VisibleForTesting;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.platform.common.builders.SDKConstants;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;

/**
 * Validates {@link TopologyEntity}s during topology pipeline.
 *
 */
@ThreadSafe
public class EntityValidator {
    private static final Logger logger = LogManager.getLogger();
    private final boolean oldValuesCacheEnabled;
    /**
     * All non-access commodity capacities seen during the last live pipeline processing.
     * Whenever the sold commodity has capacity missing, the previous value may be assumed.
     * Instead of setting secret magic number into it.
     * Map node size is 80b so for large topologies this may take significant amount of mem.
     * TODO consider also caching usages - although they have smaller processing impact
     */
    private Map<SoldCommodityReference, Double> capacities = new HashMap<>();

    /**
     * Construct the entity validator.
     *
     * @param oldValuesCacheEnabled whether to keep previous live capacities to
     * assume values instead of incorrect ones in the current broadcast
     */
    public EntityValidator(boolean oldValuesCacheEnabled) {
        this.oldValuesCacheEnabled = oldValuesCacheEnabled;
    }

    private void logCommoditySoldInvalid(final long entityId, @Nonnull final String entityName,
                                     final int entityType,
                                     @Nonnull final CommoditySoldDTO.Builder original,
                                     @Nonnull final String property,
                                     final double illegalAmount) {
        // This has to be at warning level so that the root causes of missing capacities can be investigated.
        logger.warn("Entity {} with name {} of type {} is selling {} commodity {} with illegal {} {}",
                entityId,
                entityName,
                EntityType.forNumber(entityType),
                original.getActive() ? "active" : "non-active",
                CommodityType.forNumber(original.getCommodityType().getType()),
                property,
                illegalAmount);
    }

    private void logCommodityBoughtReplacement(final long entityId, @Nonnull final String entityName,
                                               final int entityType,
                                               @Nonnull final CommodityBoughtDTO.Builder original,
                                               @Nonnull final String property,
                                               final double illegalAmount, final long providerId,
                                               final int providerType) {
        // TODO changed from warn to trace to reduce logging load - does it need more visibility?
        logger.trace("Entity {} with name {} of type {} is buying {} commodity {} with illegal " +
                "{} {} from entity {} of type {}", entityId, entityName,
            EntityType.forNumber(entityType),
            original.getActive() ? "active" : "non-active",
            CommodityType.forNumber(original.getCommodityType().getType()),
            property, illegalAmount, providerId,
            EntityType.forNumber(providerType)
        );
    }

    /**
     * React to illegal values in each commodity to various hard-coded overrides.
     * We cannot send random capacity values because the market will produce invalid actions.
     * We can zero out the invalid usage values with less impact.
     *
     * Note: this method modifies in place commodities bought and sold by a topology entity DTO
     * builder passed in as a parameter.
     *
     * @param entity the topology entity DTO builder to modify
     * @param clonedFrom the source entity for the plan clone
     * @param entityGraph entity graph
     */
    @VisibleForTesting
    void processIllegalCommodityValues(@Nonnull final TopologyEntityDTO.Builder entity,
                                       @Nonnull final Optional<TopologyEntityDTO.Builder> clonedFrom,
                                       @Nonnull final TopologyGraph<TopologyEntity> entityGraph) {
        final long id = entity.getOid();
        final String name = entity.getDisplayName();
        final int type = entity.getEntityType();
        entity.getCommoditiesBoughtFromProvidersBuilderList()
            .forEach(fromProvider ->
                fromProvider.getCommodityBoughtBuilderList()
                    .forEach(commodityBought -> {
                        final int providerType = fromProvider.getProviderEntityType();
                        final long providerId = fromProvider.getProviderId();
                        if (commodityBought.getPeak() < 0 || Double.isNaN(commodityBought.getPeak())) {
                            // Negative peak values are illegal, but we sometimes get them.
                            // unset them - 0 peak with positive usage makes no sense, but absent peak is legitimate
                            logCommodityBoughtReplacement(id, name, type, commodityBought, "peak",
                                commodityBought.getPeak(), providerId, providerType);
                            commodityBought.clearPeak();
                        }
                        if (commodityBought.getUsed() < 0.0 || Double.isNaN(commodityBought.getUsed())) {
                            // Negative used values are illegal, but we sometimes get them.
                            // Replace them with 0.
                            logCommodityBoughtReplacement(id, name, type, commodityBought, "used",
                                commodityBought.getUsed(), providerId, providerType);
                            commodityBought.setUsed(0);
                        }
                })
        );

        entity.getCommoditySoldListBuilderList().forEach(commoditySold -> {
            double used = commoditySold.getUsed();
            double capacity = commoditySold.getCapacity();
            // never replace to secret magic numbers, this leads to incorrect decision making
            // - attempt to get previous value, if enabled and available
            // - if not, mark entity not controllable (regardless of commodity type)
            //   and clearly indicate lack of capacity for the components further down the data flow
            if (!commoditySold.hasCapacity() || Double.isNaN(capacity) || capacity <= 0) {
                Double oldCapacity = null;
                if (oldValuesCacheEnabled) {
                    synchronized (this) {
                        oldCapacity = capacities.get(new SoldCommodityReference(
                                        clonedFrom.isPresent() ? clonedFrom.get() : entity,
                                        commoditySold));
                    }
                }
                if (oldCapacity == null) {
                    logCommoditySoldInvalid(id, name, type, commoditySold, "capacity", 0);
                    commoditySold.clearCapacity();
                    entity.getAnalysisSettingsBuilder().setControllable(false);
                    StringBuilder controllableFalseEntities = new StringBuilder();
                    entityGraph.getConsumers(entity.getOid()).forEach(consumer -> {
                        boolean shouldMarkConsumerControllableFalse  =
                            consumer.getTopologyEntityDtoBuilder()
                                .getCommoditiesBoughtFromProvidersList()
                                .stream()
                                .filter(grouping -> grouping.getProviderId() == entity.getOid())
                                .filter(grouping -> grouping.getCommodityBoughtList().stream()
                                    .map(CommodityBoughtDTO::getCommodityType)
                                    .anyMatch(boughtCommType -> boughtCommType.equals(commoditySold.getCommodityType()))
                                )
                                .findFirst().isPresent();
                        if (shouldMarkConsumerControllableFalse) {
                            consumer.getTopologyEntityDtoBuilder().getAnalysisSettingsBuilder().setControllable(false);
                            if (logger.isTraceEnabled()) {
                                controllableFalseEntities.append(consumer.toString()).append(", ");
                            }
                        }
                    });
                    controllableFalseEntities.setLength(Math.max(controllableFalseEntities.length() - 2, 0));
                    logger.warn("Setting controllable false on {}|{} and its consumers which buy {}|{} from it because of "
                        + "illegal capacity on the comm sold.", id, name,
                        CommodityType.forNumber(commoditySold.getCommodityType().getType()),
                        commoditySold.getCommodityType().getKey());
                    logger.trace("The consumers of {}|{} for which controllable was set to false are - {}.",
                        id, name, controllableFalseEntities.toString());
                } else {
                    logger.warn("Setting capacity value for oid " + id
                                    + ", commodity "
                                    + commoditySold.getCommodityType()
                                    + " to previous value "
                                    + oldCapacity);
                    commoditySold.setCapacity(oldCapacity);
                }
            }
            if (Double.isNaN(used)) {
                commoditySold.setUsed(0);
                logger.warn("Setting used value for " + EntityType.forNumber(type)
                                + CommodityType.forNumber(commoditySold.getCommodityType().getType())
                                + " from NaN to 0");
            }
            if (commoditySold.getPeak() < 0) {
                // Negative peak values are illegal, but we sometimes get them.
                // unset them - 0 peak with positive usage makes no sense, but absent peak is legitimate
                logCommoditySoldInvalid(id, name, type, commoditySold, "peak",
                    commoditySold.getPeak());
                commoditySold.clearPeak();
            }
            if (commoditySold.getUsed() < 0.0) {
                // Negative used values are illegal, but we sometimes get them.
                // Replace them with 0.
                logCommoditySoldInvalid(id, name, type, commoditySold, "used",
                    commoditySold.getUsed());
                commoditySold.setUsed(0);
            }
        });
    }

    /**
     * Check that the properties of an entity are valid.
     *
     * @param entity the entity to validate
     * @param validateCapacity whether to check the capacity
     * @return error messages when errors exist, {@link Optional#empty} otherwise.
     */
    @VisibleForTesting
    Optional<EntityValidationFailure> validateSingleEntity(
                                                @Nonnull final TopologyEntityDTO.Builder entity,
                                                boolean validateCapacity) {
        final List<String> validationErrors =
            entity.getCommoditiesBoughtFromProvidersList().stream()
                .map(commodityBought -> {
                    final StringBuilder errorStringBuilder = new StringBuilder();
                    final List<String> errors = commodityBought.getCommodityBoughtList().stream()
                            .flatMap(commodityDTO -> validateCommodityBought(commodityDTO).stream())
                            .collect(Collectors.toList());
                    if (!errors.isEmpty()) {
                        errorStringBuilder
                                .append("Errors with commodity bought from provider ")
                                .append(commodityBought.getProviderId())
                                .append(":");
                        errors.forEach(errorStr -> errorStringBuilder.append("\n    ")
                                            .append(errorStr));
                    }
                    return errorStringBuilder.toString();
                })
                .filter(errorStr -> !errorStr.isEmpty())
                .collect(Collectors.toList());

        final List<String> commoditiesSoldErrors = entity.getCommoditySoldListList().stream()
                .flatMap(commoditySold -> {
                    final List<String> errors = validateCommoditySold(commoditySold, validateCapacity).stream()
                        .map(errorStr -> "Error with commodity sold: " + errorStr)
                        .collect(Collectors.toList());
                    return errors.stream();
                })
                .collect(Collectors.toList());

        validationErrors.addAll(commoditiesSoldErrors);

        if (!validationErrors.isEmpty()) {
            final StringBuilder errorBuilder = new StringBuilder();
            errorBuilder.append(validationErrors.size()).append(" validation errors.\n");
            validationErrors.forEach(errorStr -> errorBuilder.append("\n").append(errorStr));
            return Optional.of(new EntityValidationFailure(entity.getOid(), entity.getDisplayName(),
                errorBuilder.toString()));
        }
        return Optional.empty();
    }

    @Nonnull
    private List<String> validateCommoditySold(@Nonnull final CommoditySoldDTO commoditySold,
                    boolean validateCapacity) {
        final List<String> errors = new ArrayList<>();

        if (commoditySold.hasUsed() && commoditySold.getUsed() < 0) {
            errors.add("Used " + commoditySold.getCommodityType() + " has a negative value: " +
                commoditySold.getUsed());
        }

        if (commoditySold.hasPeak() && commoditySold.getPeak() < 0) {
            errors.add("Peak " + commoditySold.getCommodityType() + " has a negative value: " +
                commoditySold.getPeak());
        }

        if (validateCapacity) {
            if (!commoditySold.hasCapacity() || commoditySold.getCapacity() == 0) {
                errors.add("Capacity " + commoditySold.getCommodityType() + " has a zero value: " +
                    commoditySold.getCapacity());
            }
            if (commoditySold.hasCapacity() && commoditySold.getCapacity() < 0) {
                errors.add("Capacity " + commoditySold.getCommodityType() + " has a negative value: " +
                    commoditySold.getCapacity());
            }
        }

        return errors;
    }

    @Nonnull
    private List<String> validateCommodityBought(@Nonnull final CommodityBoughtDTO commodityBought) {
        final List<String> errors = new ArrayList<>();

        if (commodityBought.hasUsed() && commodityBought.getUsed() < 0) {
            errors.add("Used " + commodityBought.getCommodityType() + " has a negative value: " +
                commodityBought.getUsed());
        }

        if (commodityBought.hasPeak() && commodityBought.getPeak() < 0) {
            errors.add("Peak " + commodityBought.getCommodityType() + " has a negative value: " +
                commodityBought.getPeak());
        }
        return errors;
    }

    /**
     * Information about a validation failure for a specific entity.
     */
    @Immutable
    static class EntityValidationFailure {
        final long entityId;
        final String name;
        final String errorMessage;

        EntityValidationFailure(final long entityId,
                                @Nonnull final String name,
                                @Nonnull final String errorMessage) {
            this.entityId = entityId;
            this.name = Objects.requireNonNull(name);
            this.errorMessage = Objects.requireNonNull(errorMessage);
        }

        @Override
        public String toString() {
            return "Entity " + entityId + " (" + name + ") encountered validation errors:\n" +
                errorMessage;
        }

    }

    /**
     * Validates a stream of entities from a topology graph.
     *
     * @param entityGraph the graph of entities to validate
     * @param isPlan whether validation happens for live or plan topology
     * @throws EntitiesValidationException if an entity is invalid and illegal values cannot be replaced
     */
    public void validateTopologyEntities(@Nonnull final TopologyGraph<TopologyEntity> entityGraph, boolean isPlan)
                                                            throws EntitiesValidationException {
        final List<EntityValidationFailure> validationFailures = new ArrayList<>();
        Map<SoldCommodityReference, Double> newCapacities = isPlan || !oldValuesCacheEnabled ? null : new HashMap<>();
        entityGraph.entities().forEach(entity -> {
            final Optional<EntityValidationFailure> error =
                validateSingleEntity(entity.getTopologyEntityDtoBuilder(), true);
            if (error.isPresent()) {
                processIllegalCommodityValues(entity.getTopologyEntityDtoBuilder(),
                    entity.getClonedFromEntity(), entityGraph);

                final Optional<EntityValidationFailure> errorAfterReplacement =
                    validateSingleEntity(entity.getTopologyEntityDtoBuilder(), false);
                if (errorAfterReplacement.isPresent()) {
                    logger.error("Errors validating entity {}:\n{}", entity.getOid(),
                        error.get().errorMessage);
                    validationFailures.add(error.get());
                }
            }

            if (newCapacities != null) {
                // after the validation which can change things
                // remember all valid capacities of non-access commodities
                entity.getTopologyEntityDtoBuilder().getCommoditySoldListBuilderList().forEach(commoditySold -> {
                    double capacity = commoditySold.getCapacity();
                    if (commoditySold.hasCapacity() && !Double.isNaN(capacity) && capacity > 0
                                    && Math.abs(capacity - SDKConstants.ACCESS_COMMODITY_CAPACITY) > 0.0001) {
                        newCapacities.put(new SoldCommodityReference(
                                        entity.getTopologyEntityDtoBuilder(),
                                        commoditySold), capacity);
                    }
                });
            }
        });

        if (newCapacities != null) {
            synchronized (this) {
                capacities = newCapacities;
            }
        }

        if (!validationFailures.isEmpty()) {
            throw new EntitiesValidationException(validationFailures);
        }
    }

    /**
     * Minimal identity for a sold commodity.
     * It purposefully does not contain the sold commodity key.
     * At the moment of writing there is only one non-access commodity sold by 1 provider
     * with multiple keys - VStorage.
     */
    @Immutable
    private static class SoldCommodityReference {
        private final long oid;
        private final int type;

        /**
         * Construct the sold commodity identity.
         *
         * @param entity topology entity builder
         * @param commSold sold commodity builder
         */
        SoldCommodityReference(TopologyEntityDTO.Builder entity, CommoditySoldDTO.Builder commSold) {
            this.oid = entity.getOid();
            this.type = commSold.getCommodityType().getType();
        }

        @Override
        public int hashCode() {
            return Objects.hash(oid, type);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof SoldCommodityReference) {
                final SoldCommodityReference other = (SoldCommodityReference)obj;
                return oid == other.oid && type == other.type;
            } else {
                return false;
            }
        }
    }
}
