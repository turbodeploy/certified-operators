package com.vmturbo.group.persistent;

import java.util.Collection;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.setting.SettingProto.SettingSpec;

/**
 * Settings specs store to provide us with settings specifications.
 */
public interface SettingSpecStore {
    /**
     * Returns setting specification (if any) identified by the specified name).
     *
     * @param name name of the setting to retrieve
     * @return setting specification or empty optional, if none found
     */
    @Nonnull
    Optional<SettingSpec> getSettingSpec(@Nonnull final String name);

    /**
     * Returns all the existing spettings specifications avaiable.
     *
     * @return settings specifications collection
     */
    @Nonnull
    Collection<SettingSpec> getAllSettingSpecs();

    /**
     * Returns all the existing global setting specifications.
     *
     * @return global settings specifications collection
     */
    @Nonnull
    Collection<SettingSpec> getAllGlobalSettingSpecs();
}
