package com.vmturbo.group.api;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.setting.SettingProto.SettingSpec;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Data structure for SettingPolicy settings. It represents both the restrictions on potential
 * values and default values assigned to the setting.
 *
 * @param <T> type of the data to hold
 */
public interface SettingDataStructure<T> {

    /**
     * Returns default value of the setting for the specified entity type.
     *
     * @param entityType entity type to retrieve default for
     * @return default value of the setting
     * @throws NullPointerException if {@code entityType} is null
     */
    @Nonnull
    T getDefault(@Nonnull EntityType entityType);

    /**
     * /**
     * Adds Protobuf representation into the protobuf setting specification builder.
     *
     * @param builder builder to append setting data structure to.
     */
    void build(@Nonnull SettingSpec.Builder builder);
}
