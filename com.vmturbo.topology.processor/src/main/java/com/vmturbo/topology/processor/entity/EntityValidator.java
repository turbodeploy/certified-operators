package com.vmturbo.topology.processor.entity;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.annotations.VisibleForTesting;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;

/**
 * Validates {@link TopologyEntity}s during topology pipeline.
 *
 */
public class EntityValidator {

    /**
     * MAD HAX! A very large, but noticeable number that should stand out
     * in debugging. Use this number instead of something like MAX_DOUBLE
     * so that it makes sense to do calculations with it in the market.
     */
    private static final double HACKED_INFINITE_CAPACITY = 999999999999.999;
    private static final double HACKED_CAPACITY = 101013370101.7;

    private static final Logger logger = LogManager.getLogger();

    private void logCommoditySoldReplacement(final long entityId, @Nonnull final String entityName,
                                     final int entityType,
                                     @Nonnull final CommoditySoldDTO.Builder original,
                                     @Nonnull final String property,
                                     final double illegalAmount) {
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
        logger.warn("Entity {} with name {} of type {} is buying {} commodity {} with illegal " +
                "{} {} from entity {} of type {}", entityId, entityName,
            EntityType.forNumber(entityType),
            original.getActive() ? "active" : "non-active",
            CommodityType.forNumber(original.getCommodityType().getType()),
            property, illegalAmount, providerId,
            EntityType.forNumber(providerType)
        );
    }

    /**
     * Replace illegal values in each commodity to various hard-coded overrides. We can't send out
     * illegal values because the market will crash, but we still want to have some resilience
     * against bugs in the probe's discovery results.
     *
     * Note: this method modifies in place commodities bought and sold by a topology entity DTO
     * builder passed in as a parameter.
     *
     * @param entity the topology entity DTO builder to modify
     */
    @VisibleForTesting
    void replaceIllegalCommodityValues(@Nonnull final TopologyEntityDTO.Builder entity) {
        final long id = entity.getOid();
        final String name = entity.getDisplayName();
        final int type = entity.getEntityType();
        entity.getCommoditiesBoughtFromProvidersBuilderList()
            .forEach(fromProvider ->
                fromProvider.getCommodityBoughtBuilderList()
                    .forEach(commodityBought -> {
                        final int providerType = fromProvider.getProviderEntityType();
                        final long providerId = fromProvider.getProviderId();
                        if (commodityBought.getPeak() < 0) {
                            // Negative peak values are illegal, but we sometimes get them.
                            // Replace them with 0.
                            logCommodityBoughtReplacement(id, name, type, commodityBought, "peak",
                                commodityBought.getPeak(), providerId, providerType);
                            commodityBought.setPeak(0);
                        }
                        if (commodityBought.getUsed() < 0.0) {
                            // Negative used values are illegal, but we sometimes get them.
                            // Replace them with 0.
                            logCommodityBoughtReplacement(id, name, type, commodityBought, "used",
                                commodityBought.getUsed(), providerId, providerType);
                            commodityBought.setUsed(0);
                        }
                })
        );

        entity.getCommoditySoldListBuilderList().forEach(commoditySold -> {
            if (!commoditySold.hasCapacity() || commoditySold.getCapacity() == 0) {
                // capacity of 0 is illegal - replace it with a usable value
                logCommoditySoldReplacement(id, name, type, commoditySold, "capacity", 0);
                commoditySold.setCapacity(HACKED_CAPACITY);
            } else if (commoditySold.getCapacity() < 0) {
                // Negative capacity sold can, in some cases, indicate
                // infinite capacity. While this is not officially supported
                // in the SDK, we do get these values (e.g. from the VC probe)
                // at the time of this writing - Sept 9, 2016 - and need to
                // deal with them unless we want to fail the discovery.
                logCommoditySoldReplacement(id, name, type, commoditySold, "capacity",
                    commoditySold.getCapacity());
                commoditySold.setCapacity(HACKED_INFINITE_CAPACITY);
            }
            if (commoditySold.getPeak() < 0) {
                // Negative peak values are illegal, but we sometimes get them.
                // Replace them with 0.
                logCommoditySoldReplacement(id, name, type, commoditySold, "peak",
                    commoditySold.getPeak());
                commoditySold.setPeak(0);
            }
            if (commoditySold.getUsed() < 0.0) {
                // Negative used values are illegal, but we sometimes get them.
                // Replace them with 0.
                logCommoditySoldReplacement(id, name, type, commoditySold, "used",
                    commoditySold.getUsed());
                commoditySold.setUsed(0);
            }
        });
    }

    /**
     * Check that the properties of an entity are valid.
     *
     * @param entity the entity to validate
     * @return error messages when errors exist, {@link Optional#empty} otherwise.
     */
    @VisibleForTesting
    Optional<EntityValidationFailure> validateSingleEntity(
                                                @Nonnull final TopologyEntityDTO.Builder entity) {
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
                    final List<String> errors = validateCommoditySold(commoditySold).stream()
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
    private List<String> validateCommoditySold(@Nonnull final CommoditySoldDTO commoditySold) {
        final List<String> errors = new ArrayList<>();

        if (commoditySold.hasUsed() && commoditySold.getUsed() < 0) {
            errors.add("Used " + commoditySold.getCommodityType() + " has a negative value: " +
                commoditySold.getUsed());
        }

        if (commoditySold.hasPeak() && commoditySold.getPeak() < 0) {
            errors.add("Peak " + commoditySold.getCommodityType() + " has a negative value: " +
                commoditySold.getPeak());
        }

        if (!commoditySold.hasCapacity() || commoditySold.getCapacity() == 0) {
            errors.add("Capacity " + commoditySold.getCommodityType() + " has a zero value: " +
                commoditySold.getCapacity());
        }
        if (commoditySold.hasCapacity() && commoditySold.getCapacity() < 0) {
            errors.add("Capacity " + commoditySold.getCommodityType() + " has a negative value: " +
                commoditySold.getCapacity());
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
     * @param entities the stream of entities to validate
     * @throws EntitiesValidationException if an entity is invalid and illegal values cannot be replaced
     */
    public void validateTopologyEntities(@Nonnull final Stream<TopologyEntity> entities)
                                                            throws EntitiesValidationException {
        final List<EntityValidationFailure> validationFailures = new ArrayList<>();
        entities.forEach(entity -> {
            final Optional<EntityValidationFailure> error =
                validateSingleEntity(entity.getTopologyEntityDtoBuilder());
            if (error.isPresent()) {
                replaceIllegalCommodityValues(entity.getTopologyEntityDtoBuilder());

                final Optional<EntityValidationFailure> errorAfterReplacement =
                    validateSingleEntity(entity.getTopologyEntityDtoBuilder());
                if (errorAfterReplacement.isPresent()) {
                    logger.error("Errors validating entity {}:\n{}", entity.getOid(),
                        error.get().errorMessage);
                    validationFailures.add(error.get());
                }
            }
        });

        if (!validationFailures.isEmpty()) {
            throw new EntitiesValidationException(validationFailures);
        }
    }
}
