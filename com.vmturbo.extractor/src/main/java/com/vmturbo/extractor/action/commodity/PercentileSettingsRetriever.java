package com.vmturbo.extractor.action.commodity;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import javax.annotation.Nonnull;

import io.grpc.StatusRuntimeException;

import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectMaps;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc.SettingPolicyServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingFilter;
import com.vmturbo.common.protobuf.setting.SettingProto.GetEntitySettingsRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.components.common.setting.PercentileSettingSpecs;

/**
 * Responsible for retrieving percentile-related settings for entities from the group component.
 *
 * <p/>We move this logic to a separate class because it's a pain to test gRPC/external dependencies,
 * and its much simpler to do it in an isolated fashion.
 */
public class PercentileSettingsRetriever {

    private static final Logger logger = LogManager.getLogger();

    private final SettingPolicyServiceBlockingStub settingPolicyServiceStub;

    PercentileSettingsRetriever(@Nonnull final SettingPolicyServiceBlockingStub settingPolicyServiceStub) {
        this.settingPolicyServiceStub = settingPolicyServiceStub;
    }

    /**
     * Data object containing percentile-related settings for a collection of entities.
     */
    public static class PercentileSettings {
        private final Map<EntitySettingSpecs, Long2ObjectMap<Integer>> settingsBySpecAndEntity;

        PercentileSettings(Map<EntitySettingSpecs, Long2ObjectMap<Integer>> settingsBySpecAndEntity) {
            this.settingsBySpecAndEntity = settingsBySpecAndEntity;
        }

        /**
         * Get the percentile-related settings for a particular entity.
         *
         * @param entityId The id of the entity.
         * @param entityType The type of the entity.
         * @return An optional containing the {@link PercentileSettings}. Empty optional if the
         *         entity has no percentile settings in the group component.
         */
        public Optional<PercentileSetting> getEntitySettings(final long entityId, @Nonnull final ApiEntityType entityType) {
            return PercentileSettingSpecs.getPercentileSettings(entityType)
                    .map(typeSettings -> {
                        final Integer observationPeriod =
                            settingsBySpecAndEntity.getOrDefault(typeSettings.getObservationPeriod(), Long2ObjectMaps.emptyMap()).get(entityId);
                        final Integer aggressiveness =
                            settingsBySpecAndEntity.getOrDefault(typeSettings.getAggressiveness(), Long2ObjectMaps.emptyMap()).get(entityId);
                        if (observationPeriod != null && aggressiveness != null) {
                            return new PercentileSetting(observationPeriod, aggressiveness);
                        } else {
                            return null;
                        }
                    });
        }

        /**
         * Data object containing the percentile-related settings of an entity.
         */
        public static class PercentileSetting {
            // Using Integer types to avoid autoboxing later down the chain, when we put these
            // values into a JSON object.
            private final Integer observationPeriod;
            private final Integer aggresiveness;

            private PercentileSetting(@Nonnull final Integer observationPeriod,
                    @Nonnull final Integer aggresiveness) {
                this.observationPeriod = observationPeriod;
                this.aggresiveness = aggresiveness;
            }

            @Nonnull
            public Integer getAggresiveness() {
                return aggresiveness;
            }

            @Nonnull
            public Integer getObservationPeriod() {
                return observationPeriod;
            }
        }
    }

    /**
     * Get the percentile-related settings data associated with a set of entities.
     *
     * @param entityIds The ids of the entities.
     * @return The {@link PercentileSettings}.
     */
    @Nonnull
    public PercentileSettings getPercentileSettingsData(LongSet entityIds) {
        final Map<EntitySettingSpecs, Long2ObjectMap<Integer>> values = new HashMap<>();
        try {
            settingPolicyServiceStub.getEntitySettings(GetEntitySettingsRequest.newBuilder()
                    .setIncludeSettingPolicies(false)
                    .setSettingFilter(EntitySettingFilter.newBuilder()
                            .addAllEntities(entityIds)
                            .addAllSettingName(PercentileSettingSpecs.settingNames()))
                    .build()).forEachRemaining(resp -> {
                resp.getSettingGroupList().forEach(settingGroup -> {
                    Setting setting = settingGroup.getSetting();
                    EntitySettingSpecs.getSettingByName(
                            settingGroup.getSetting().getSettingSpecName()).ifPresent(settingType -> {
                        final int value = Math.round(setting.getNumericSettingValue().getValue());
                        Long2ObjectMap<Integer> m = values.computeIfAbsent(settingType, k -> new Long2ObjectOpenHashMap<>());
                        settingGroup.getEntityOidsList().forEach(oid -> m.put(oid.longValue(), Integer.valueOf(value)));
                    });
                });
            });
        } catch (StatusRuntimeException e) {
            logger.error("Failed to retrieve percentile settings from group component.", e);
        }
        return new PercentileSettings(values);
    }
}
