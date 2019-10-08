package com.vmturbo.components.common.setting;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.setting.SettingProto.EnumSettingValueType;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingSpec.Builder;

/**
 * Data structure description for enumeration settings.
 *
 * @param <T> enum type to represent
 */
@Immutable
public class EnumSettingDataType<T extends Enum<T>> extends AbstractSettingDataType<T> {
    private static final Logger logger = LogManager.getLogger();

    private final T maxValue;
    private final Class<T> enumClass;

    /**
     * Constructs enum data type holding specified default value.
     *
     * @param defaultValue default value
     * @param enumClass class of an enum
     */
    public EnumSettingDataType(@Nonnull T defaultValue, @Nonnull Class<T> enumClass) {
        this(defaultValue, null, enumClass);
    }

    /**
     * Constructs enum data type holding specified default value.
     *
     * @param defaultValue default value
     * @param maxValue maximum value
     * @param enumClass class of an enum
     */
    public EnumSettingDataType(@Nonnull T defaultValue, @Nullable T maxValue,
                    @Nonnull Class<T> enumClass) {
        super(defaultValue);
        this.maxValue = maxValue;
        this.enumClass = enumClass;
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

    @Override
    @Nullable
    public T getValue(@Nullable Setting setting) {
        if (setting == null || !setting.hasEnumSettingValue()
                        || !setting.getEnumSettingValue().hasValue()) {
            return null;
        }
        String value = setting.getEnumSettingValue().getValue();
        if (value == null) {
            logger.error("Unset enum setting value in " + setting);
            return null;
        }
        try {
            return Enum.valueOf(enumClass, value);
        } catch (IllegalArgumentException e) {
            logger.error("Invalid enum setting value " + value + " in " + setting, e);
            return null;
        }
    }
}

