package com.vmturbo.components.common.setting;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Utility with static methods to access the percentile-related settings associated with a given
 * entity type.
 *
 * <p/>Used because we need to know which percentile-related {@link EntitySettingSpecs} are
 * associated with which entity type in multiple components (currently topology processor and
 * extractor).
 */
public class PercentileSettingSpecs {


    private static final Map<EntityType, EntityTypePercentileSettings> PERCENTILE_SETTINGS =
        ImmutableMap.<EntityType, EntityTypePercentileSettings>builder()
            .put(EntityType.BUSINESS_USER, new EntityTypePercentileSettings(
                EntitySettingSpecs.MaxObservationPeriodBusinessUser,
                EntitySettingSpecs.PercentileAggressivenessBusinessUser, null))
            .put(EntityType.CONTAINER_SPEC, new EntityTypePercentileSettings(
                EntitySettingSpecs.MaxObservationPeriodContainerSpec,
                EntitySettingSpecs.PercentileAggressivenessContainerSpec, EntitySettingSpecs.MinObservationPeriodContainerSpec))
            .put(EntityType.VIRTUAL_MACHINE, new EntityTypePercentileSettings(
                EntitySettingSpecs.MaxObservationPeriodVirtualMachine,
                EntitySettingSpecs.PercentileAggressivenessVirtualMachine,
                EntitySettingSpecs.MinObservationPeriodVirtualMachine))
            .put(EntityType.VIRTUAL_VOLUME, new EntityTypePercentileSettings(
                EntitySettingSpecs.MaxObservationPeriodVirtualVolume,
                EntitySettingSpecs.PercentileAggressivenessVirtualVolume,
                EntitySettingSpecs.MinObservationPeriodVirtualVolume))
            .put(EntityType.DATABASE, new EntityTypePercentileSettings(
                EntitySettingSpecs.MaxObservationPeriodDatabase,
                EntitySettingSpecs.PercentileAggressivenessDatabase,
                null))
            .put(EntityType.DATABASE_SERVER, new EntityTypePercentileSettings(
                EntitySettingSpecs.MaxObservationPeriodDatabaseServer,
                EntitySettingSpecs.PercentileAggressivenessDatabaseServer,
                EntitySettingSpecs.MinObservationPeriodDatabaseServer
            ))
            .build();

    private PercentileSettingSpecs() {}

    /**
     * Get the percentile settings associated with the input entity type.
     *
     * @param entityType The entity type.
     * @return The associated {@link EntityTypePercentileSettings}, or an empty optional if this
     *         entity type has no percentile settings.
     */
    public static Optional<EntityTypePercentileSettings> getPercentileSettings(@Nullable final ApiEntityType entityType) {
        return Optional.ofNullable(entityType)
            .map(ApiEntityType::sdkType)
            .map(PERCENTILE_SETTINGS::get);
    }

    /**
     * Get the percentile settings associated with the input entity type.
     *
     * @param entityType The entity type.
     * @return The associated {@link EntityTypePercentileSettings}, or an empty optional if this
     *         entity type has no percentile settings.
     */
    public static Optional<EntityTypePercentileSettings> getPercentileSettings(@Nullable final EntityType entityType) {
        return Optional.ofNullable(entityType)
                .map(PERCENTILE_SETTINGS::get);
    }

    /**
     * Get the names of all percentile-related settings.
     *
     * @return The set of setting spec names containing all setting specs related to percentile
     *         calculations.
     */
    public static Set<String> settingNames() {
        return PERCENTILE_SETTINGS.values().stream()
            .flatMap(e -> Stream.of(e.getAggressiveness(), e.getMinObservationPeriod(), e.getObservationPeriod()))
            .filter(Objects::nonNull)
            .map(EntitySettingSpecs::getSettingName)
            .collect(Collectors.toSet());
    }

    /**
     * Contains the percentile settings associated with a specific entity type.
     */
    public static class EntityTypePercentileSettings {
        private final EntitySettingSpecs observationPeriod;
        private final EntitySettingSpecs aggresiveness;
        private final EntitySettingSpecs minObservationPeriod;

        private EntityTypePercentileSettings(@Nonnull final EntitySettingSpecs observationPeriod,
                @Nonnull final EntitySettingSpecs aggresiveness,
                @Nullable final EntitySettingSpecs minObservationPeriod) {
            this.observationPeriod = observationPeriod;
            this.aggresiveness = aggresiveness;
            this.minObservationPeriod = minObservationPeriod;
        }

        @Nonnull
        public EntitySettingSpecs getObservationPeriod() {
            return observationPeriod;
        }

        @Nonnull
        public EntitySettingSpecs getAggressiveness() {
            return aggresiveness;
        }

        @Nullable
        public EntitySettingSpecs getMinObservationPeriod() {
            return minObservationPeriod;
        }
    }

}
