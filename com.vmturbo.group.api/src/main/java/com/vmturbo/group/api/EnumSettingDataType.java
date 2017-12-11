package com.vmturbo.group.api;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

import com.vmturbo.common.protobuf.setting.SettingProto.EnumSettingValueType;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingSpec.Builder;

/**
 * Data structure description for enumeration settings.
 *
 * @param <T> enum type to represent
 */
@Immutable
public class EnumSettingDataType<T extends Enum<T>> extends AbstractSettingDataType<T> {

    private T maxValue;

    /**
     * Constructs enum data type holding specified default value.
     *
     * @param defaultValue default value
     */
    public EnumSettingDataType(@Nonnull T defaultValue) {
        super(defaultValue);
    }

    public EnumSettingDataType(@Nonnull T defaultValue, @Nonnull T maxValue) {
        super(defaultValue);
        this.maxValue = maxValue;
    }

    @Override
    public void build(@Nonnull Builder builder) {
        final List<String> values = Stream.of(getDefault().getDeclaringClass().getEnumConstants())
                .filter(t -> ((this.maxValue == null) || (t.ordinal() <= this.maxValue.ordinal())))
                .map(Enum::name)
                .collect(Collectors.toList());
        final Map<Integer, String> entityDefaults = getEntityDefaults().entrySet()
                .stream()
                .collect(Collectors.toMap(Entry::getKey, entry -> entry.getValue().name()));
        builder.setEnumSettingValueType(EnumSettingValueType.newBuilder()
                .addAllEnumValues(values)
                .setDefault(getDefault().name())
                .putAllEntityDefaults(entityDefaults)
                .build());
    }
}

