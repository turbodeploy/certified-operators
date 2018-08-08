package com.vmturbo.group.setting;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

import com.vmturbo.common.protobuf.setting.SettingProto.SettingSpec;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.components.common.setting.GlobalSettingSpecs;

/**
 * Setting spec store, based on enum ({@link EntitySettingSpecs}) and
 * ({@link GlobalSettingSpecs}).
 */
@Immutable
public class EnumBasedSettingSpecStore implements SettingSpecStore {

    private final Map<String, SettingSpec> settingSpecMap;

    public EnumBasedSettingSpecStore() {

        Map<String, SettingSpec> specs = new HashMap<>();
        for (EntitySettingSpecs setting : EntitySettingSpecs.values()) {
            specs.put(setting.getSettingName(), setting.getSettingSpec());
        }
        for (GlobalSettingSpecs setting : GlobalSettingSpecs.values()) {
            specs.put(setting.getSettingName(), setting.createSettingSpec());
        }

        settingSpecMap = Collections.unmodifiableMap(specs);
    }

    @Nonnull
    @Override
    public Optional<SettingSpec> getSettingSpec(@Nonnull String name) {
        Objects.requireNonNull(name);
        return Optional.ofNullable(settingSpecMap.get(name));
    }

    @Nonnull
    @Override
    public Collection<SettingSpec> getAllSettingSpecs() {
        return settingSpecMap.values();
    }

    @Nonnull
    @Override
    public Collection<SettingSpec> getAllGlobalSettingSpecs() {
        return settingSpecMap
                .values()
                .stream()
                .filter(SettingSpec::hasGlobalSettingSpec)
                .collect(Collectors.toList());
    }
}
