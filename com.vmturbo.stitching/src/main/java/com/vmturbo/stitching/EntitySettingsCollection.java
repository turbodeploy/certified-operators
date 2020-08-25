package com.vmturbo.stitching;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettings;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettings.SettingToPolicyId;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy;
import com.vmturbo.components.common.setting.EntitySettingSpecs;

/**
 * Provides services for looking up a specific setting belonging to a specific entity.
 *
 * If there are n entities in the topology with a maximum of m settings applying to any individual entity,
 * the time to look up an individual setting for an individual entity is O(m).
 *
 * The assumption is that we will not have to look up very many settings for an entity and that there will usually
 * be a small number of settings available for an entity. If this is the case, the time difference between
 * an O(m) lookup and an O(1) lookup should be small, so we trade the small amount of extra time to reduce
 * the amount of space we would need to further accelerate lookups. If any of these assumptions prove
 * to be untrue, we can reevaluate how the information is kept in memory and retrieved.
 */
public class EntitySettingsCollection {
    private final Map<Long, SettingPolicy> defaultSettingPolicies;
    private final Map<Long, EntitySettings> settingsByEntity;

    /**
     * Create a new {@link EntitySettingsCollection}.
     *
     * @param defaultSettingPolicies A map of default {@link SettingPolicy}s keyed by their ID.
     * @param settingsByEntity A map of {@link EntitySettings} keyed by entity OID.
     */
    public EntitySettingsCollection(@Nonnull final Map<Long, SettingPolicy> defaultSettingPolicies,
                                    @Nonnull final Map<Long, EntitySettings> settingsByEntity) {
        this.defaultSettingPolicies = Objects.requireNonNull(defaultSettingPolicies);
        this.settingsByEntity = Objects.requireNonNull(settingsByEntity);
    }

    /**
     * Get a setting for an entity.
     *
     * @param oid The object ID of the entity.
     * @param settingName The name of the setting to look up.
     * @return The setting with the given name for the entity with the given OID.
     *         Returns {@link Optional#empty()} if no such entity/setting pair exists.
     */
    public Optional<Setting> getEntitySetting(final long oid, @Nonnull final String settingName) {
        final EntitySettings settingsForEntity = settingsByEntity.get(oid);
        if (settingsForEntity == null) {
            return Optional.empty();
        }

        // Return the user setting if it exists, and if not, look up the default setting if it exists.
        return settingsForEntity.getUserSettingsList().stream()
            .map(SettingToPolicyId::getSetting)
            .filter(setting -> setting.getSettingSpecName().equals(settingName))
            .findFirst()
            .map(Optional::of)
            .orElseGet(() -> associatedDefaultSetting(settingsForEntity, settingName));
    }

    /**
     * Get a setting for an entity.
     *
     * @param oid The object ID of the entity.
     * @param settingName The {@link EntitySettingSpecs} describing the name of the setting to look up.
     * @return The setting with the given name for the entity with the given OID.
     *         Returns {@link Optional#empty()} if no such entity/setting pair exists.
     */
    public Optional<Setting> getEntitySetting(final long oid, @Nonnull final EntitySettingSpecs settingName) {
        return getEntitySetting(oid, settingName.getSettingName());
    }

    /**
     * Get a setting for an entity.
     *
     * @param topologyEntity The {@link TopologyEntity} whose setting should be looked up.
     * @param settingName The {@link EntitySettingSpecs} describing the name of the setting to look up.
     * @return The setting with the given name for the entity with the given OID.
     *         Returns {@link Optional#empty()} if no such entity/setting pair exists.
     */
    public Optional<Setting> getEntitySetting(@Nonnull final TopologyEntity topologyEntity,
                                              @Nonnull final EntitySettingSpecs settingName) {
        return getEntitySetting(topologyEntity.getOid(), settingName);
    }

    /**
     * Extract the entity setting value by passed entity and spec.
     *
     * @param <T> setting value type
     * @param oid entity identifier
     * @param settingSpec setting specification
     * @param cls setting value class
     * @return setting value, null if not present
     */
    @Nullable
    public <T> T getEntitySettingValue(long oid,
                                       @Nonnull EntitySettingSpecs settingSpec,
                                       @Nonnull Class<T> cls) {
        Optional<Setting> setting = getEntitySetting(oid, settingSpec);
        if (!setting.isPresent()) {
            return null;
        }
        return settingSpec.getValue(setting.get(), cls);
    }

    private Optional<Setting> associatedDefaultSetting(@Nonnull final EntitySettings settingsForEntity,
                                                       @Nonnull final String settingName) {
        if (!settingsForEntity.hasDefaultSettingPolicyId()) {
            return Optional.empty();
        }

        final SettingPolicy defaultSettingPolicy =
            defaultSettingPolicies.get(settingsForEntity.getDefaultSettingPolicyId());

        return defaultSettingPolicy == null
            ? Optional.empty()
            : defaultSettingPolicy.getInfo().getSettingsList().stream()
                .filter(setting -> setting.getSettingSpecName().equals(settingName))
                .findFirst();
    }

    /**
     * Get a user setting for an entity.
     *
     * @param oid The object ID of the entity.
     * @param settingName The {@link EntitySettingSpecs} describing the name of the setting to look up.
     * @return The user setting with the given name, if there is one, for the entity with the given OID.
     */
    public Optional<Setting> getEntityUserSetting(final long oid, @Nonnull final String settingName) {
        final EntitySettings settingsForEntity = settingsByEntity.get(oid);
        if (settingsForEntity == null) {
            return Optional.empty();
        }

        return settingsForEntity.getUserSettingsList().stream()
            .map(SettingToPolicyId::getSetting)
            .filter(setting -> setting.getSettingSpecName().equals(settingName))
            .findFirst();
    }

    /**
     * Get a user setting for an entity.
     *
     * @param entity The {@link TopologyEntity} whose user setting should be looked up.
     * @param setting The {@link EntitySettingSpecs} describing the name of the setting to look up.
     * @return The user setting of the given type, if there is one, for the given entity with the given OID.
     */
    public Optional<Setting> getEntityUserSetting(@Nonnull final TopologyEntity entity,
                                                  @Nonnull final EntitySettingSpecs setting) {
        return getEntityUserSetting(entity.getOid(), setting.getSettingName());
    }

    /**
     * Indicate weather an entity with a given entityOid has a user policy defined.
     *
     * @param entityOid of the input entity
     * @return true if the entity has a user policy, false otherwise.
     */
    public boolean hasUserPolicySettings(final long entityOid) {
        return settingsByEntity.containsKey(entityOid) &&
            !settingsByEntity.get(entityOid).getUserSettingsList().isEmpty();
    }
}
