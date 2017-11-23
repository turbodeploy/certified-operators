package com.vmturbo.group.persistent;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

import com.vmturbo.common.protobuf.setting.SettingProto.SettingSpec;
import com.vmturbo.group.api.SettingPolicySetting;

/**
 * Setting spec store, based on enum ({@link SettingPolicySetting}).
 */
@Immutable
public class EnumBasedSettingSpecStore implements SettingSpecStore {

    private final Map<String, SettingSpec> settingSpecMap;

    public EnumBasedSettingSpecStore() {
        settingSpecMap = Collections.unmodifiableMap(Stream.of(SettingPolicySetting.values())
                .collect(Collectors.toMap(SettingPolicySetting::getSettingName,
                        SettingPolicySetting::createSettingSpec)));
    }

    @Nonnull
    @Override
    public Optional<SettingSpec> getSettingSpec(@Nonnull String name) {
        Objects.requireNonNull(name);
        return Optional.ofNullable(settingSpecMap.get(name));
    }

    @Nonnull
    @Override
    public Collection<SettingSpec> getAllSettingSpec() {
        return settingSpecMap.values();
    }
}
