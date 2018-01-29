package com.vmturbo.topology.processor.entity;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.Discovery.ErrorDTO;
import com.vmturbo.platform.common.dto.Discovery.ErrorDTO.ErrorSeverity;

/**
 * Validates {@link EntityDTO}s that come in from probes.
 *
 * <p>It's an object instead of just utility methods for dependency injection/testing.
 */
public class EntityValidator {

    /**
     * MAD HAX! A very large, but noticeable number that should stand out
     * in debugging. Use this number instead of something like MAX_DOUBLE
     * so that it makes sense to do calculations with it in the market.
     */
    private static final double HACKED_INFINITE_CAPACITY = 999999999999.999;

    private static final Logger logger = LogManager.getLogger();

    private void logReplacementError(@Nonnull final EntityDTO ownerEntity,
                                     @Nonnull final CommodityDTO original,
                                     final boolean sold,
                                     final String type,
                                     final double illegalAmount) {
        logger.warn("Entity {} with name {} of type {} is {} {} commodity {} with illegal {} {}",
                ownerEntity.getId(),
                ownerEntity.getDisplayName(),
                ownerEntity.getEntityType(),
                sold ? "selling" : "buying",
                original.getActive() ? "active" : "non-active",
                original.getCommodityType(),
                type,
                illegalAmount);
    }

    /**
     * Replace illegal values in a commodity to
     * various hard-coded overrides. We can't send out
     * illegal values because the market will crash, but we
     * still want to have some resilience against bugs in
     * the probe's discovery results.
     *
     * @param ownerEntity The entity that is buying or selling this commodity.
     * @param original The commodity.
     * @param sold If true, the commodity is being sold by the owner. If false, it's being bought.
     * @return A copy of the original commodity with illegal values replaced.
     */
    @Nonnull
    public CommodityDTO replaceIllegalCommodityValues(@Nonnull final EntityDTO ownerEntity,
                                                      @Nonnull final CommodityDTO original,
                                                      final boolean sold) {
        final CommodityDTO.Builder modifiedBuilder = CommodityDTO.newBuilder(original);

        // We only need to deal with capacity for
        // commodities sold, since capacity has
        // no meaning in bought commodities.
        if (sold) {
            if (!original.hasCapacity() || original.getCapacity() == 0.0) {
                logReplacementError(ownerEntity, original, sold, "capacity", 0.0);
                double hackedCapacity = 0;
                // TODO (roman, Sep 15 2016): Some commodities need to be
                // filled in from user settings. Since that doesn't exist
                // right now, just set a reasonable default based on the
                // commodity type.
                switch (original.getCommodityType()) {
                    case STORAGE_ACCESS:
                        hackedCapacity = 5000;
                        break;
                    default:
                        // MAD HAX! Fill in missing capacity to some intentionally absurd number.
                        hackedCapacity = 101013370101.7;
                        break;
                }
                modifiedBuilder.setCapacity(hackedCapacity);
            } else if (original.getCapacity() < 0.0) {
                // Negative capacity sold can, in some cases, indicate
                // infinite capacity. While this is not officially supported
                // in the SDK, we do get these values (e.g. from the VC probe)
                // at the time of this writing - Sept 9, 2016 - and need to
                // deal with them unless we want to fail the discovery.
                logReplacementError(ownerEntity, original, sold, "capacity", original.getCapacity());
                modifiedBuilder.setCapacity(HACKED_INFINITE_CAPACITY);
            }

            if (original.getLimit() < 0.0) {
                logReplacementError(ownerEntity, original, sold, "limit", original.getLimit());
                modifiedBuilder.setLimit(HACKED_INFINITE_CAPACITY);
            }
        } else {
            if (original.getReservation() < 0.0) {
                logReplacementError(ownerEntity, original, sold, "reservation", original.getReservation());
                modifiedBuilder.setReservation(0.0);
            }
        }

        if (original.getUsed() < 0.0) {
            // Negative used values are illegal, but we sometimes get them.
            // Replace them with 0.
            logReplacementError(ownerEntity, original, sold, "used", original.getUsed());
            modifiedBuilder.setUsed(0);
        }

        if (original.getPeak() < 0.0) {
            logReplacementError(ownerEntity, original, sold, "peak", original.getPeak());
            modifiedBuilder.setPeak(0);
        }

        return modifiedBuilder.build();
    }

    public Optional<EntityValidationFailure> validateEntityDTO(final long entityId,
                    @Nonnull final EntityDTO entityDTO) {
        return validateEntityDTO(entityId, entityDTO, false);
    }

    /**
     * Check that the properties of an entity are valid.
     *
     * @param entityId ID of the entity to validate
     * @param entityDTO the entity to validate
     * @param last set to true in the post-massage re-validation in order to skip tests
     * that may still fail at that stage, e.g. the check for provisioned commodities.
     * @return error messages when errors exist, {@link Optional#empty} otherwise.
     */
    public Optional<EntityValidationFailure> validateEntityDTO(final long entityId,
                                                               @Nonnull final EntityDTO entityDTO, boolean last) {
        final List<String> validationErrors = entityDTO.getCommoditiesBoughtList().stream()
                .map(commodityBought -> {
                    final StringBuilder errorStringBuilder = new StringBuilder();
                    final List<String> errors = commodityBought.getBoughtList().stream()
                            .flatMap(commodityDTO -> validateCommodityDTO(commodityDTO, true, last).stream())
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

        final List<String> commoditiesSoldErrors = entityDTO.getCommoditiesSoldList().stream()
                .flatMap(commoditySold -> {
                    final List<String> errors = validateCommodityDTO(commoditySold, false, last).stream()
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
            return Optional.of(new EntityValidationFailure(entityId, entityDTO, errorBuilder.toString()));
        }
        return Optional.empty();
    }

    @Nonnull
    private List<String> validateCommodityDTO(@Nonnull final CommodityDTO commodityDTO,
                                      final boolean buyer, boolean last) {
        final List<String> errors = new ArrayList<>();

        if (commodityDTO.hasUsed() && commodityDTO.getUsed() < 0) {
            errors.add("Used " + commodityDTO.getCommodityType() + " has a negative value: " + commodityDTO.getUsed());
        }

        if (commodityDTO.hasPeak() && commodityDTO.getPeak() < 0) {
            errors.add("Peak " + commodityDTO.getCommodityType() + " has a negative value: " + commodityDTO.getPeak());
        }

        if (buyer) {
            if (commodityDTO.hasReservation() && commodityDTO.getReservation() < 0) {
                errors.add("Reservation " + commodityDTO.getCommodityType() + " has a negative value: " + commodityDTO.getReservation());
            }
        } else {
            if (!commodityDTO.hasCapacity() || commodityDTO.getCapacity() == 0) {
                errors.add("Capacity " + commodityDTO.getCommodityType() + " has a zero value: " + commodityDTO.getCapacity());
            }
            if (commodityDTO.hasCapacity() && commodityDTO.getCapacity() < 0) {
                errors.add("Capacity " + commodityDTO.getCommodityType() + " has a negative value: " + commodityDTO.getCapacity());
            }
            if (commodityDTO.hasLimit() && commodityDTO.getLimit() < 0) {
                errors.add("Limit " + commodityDTO.getCommodityType() + " has a negative value: " + commodityDTO.getLimit());
            }
        }
        return errors;
    }

    /**
     * Information about a validation failure for a specific entity.
     */
    @Immutable
    static class EntityValidationFailure {
        final long entityId;
        final EntityDTO entityDto;
        final String errorMessage;

        EntityValidationFailure(final long entityId,
                                @Nonnull final EntityDTO entityDto,
                                @Nonnull final String errorMessage) {
            this.entityId = entityId;
            this.entityDto = Objects.requireNonNull(entityDto);
            this.errorMessage = Objects.requireNonNull(errorMessage);
        }

        @Nonnull
        ErrorDTO errorDto() {
            return ErrorDTO.newBuilder()
                    .setSeverity(ErrorSeverity.CRITICAL)
                    .setDescription("Entity " + entityId +
                            " (name: " + entityDto.getDisplayName() + ")" +
                            " encountered validation errors:\n" + errorMessage)
                    .setEntityUuid(entityDto.getId())
                    .build();
        }
    }
}
