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
     * Returns setting specification by name.
     *
     * @param name setting name to look up for.
     * @return setting specification or empty optional.
     */
    @Nonnull
    Optional<SettingSpec> getSettingSpec(@Nonnull final String name);

    /**
     * Returns all the existing setting specifications.
     *
     * @return settings specification collection
     */
    @Nonnull
    Collection<SettingSpec> getAllSettingSpec();
}
