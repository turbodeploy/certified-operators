package com.vmturbo.components.common.setting;

import java.util.Collections;
import java.util.Map;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import com.vmturbo.common.protobuf.setting.SettingProto.NumericSettingValueType;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingSpec.Builder;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Data structure for numeric data structures.
 */
@Immutable
public class NumericSettingDataType extends AbstractSettingDataType<Float> {
    private final float min;
    private final float max;

    /**
     * Constructs numeric data type with entity type specific default values.
     *
     * @param min minimal allowed value
     * @param max maximum allowed value
     * @param defaultValue global default value
     * @param entityDefaults entity type specific default values
     */
    public NumericSettingDataType(float min, float max, float defaultValue,
            Map<EntityType, Float> entityDefaults) {
        super(defaultValue, entityDefaults);
        this.min = min;
        this.max = max;
    }

    /**
     * Constructs numeric data type.
     *
     * @param min minimal allowed value
     * @param max maximum allowed value
     * @param defaultValue global default value
     */
    public NumericSettingDataType(float min, float max, float defaultValue) {
        this(min, max, defaultValue, Collections.emptyMap());
    }

    /**
     * Return minimal allowed value.
     *
     * @return minimal allowed value
     */
    public float getMin() {
        return min;
    }

    /**
     * Returns maximum allowed value.
     *
     * @return maximum allowed value
     */
    public float getMax() {
        return max;
    }

    @Override
    public void build(@Nonnull Builder builder) {
        final NumericSettingValueType.Builder settingBuilder = NumericSettingValueType.newBuilder().setMin(min)
                .setMax(max).putAllEntityDefaults(getEntityDefaults());
        final Float defaultValue = getDefault();
        if (defaultValue != null) {
            settingBuilder.setDefault(defaultValue);
        }
        builder.setNumericSettingValueType(settingBuilder.build());
    }

    @Override
    @Nullable
    public Float getValue(@Nullable Setting setting) {
        return setting == null || !setting.hasNumericSettingValue()
                        || !setting.getNumericSettingValue().hasValue() ? null
                                        : setting.getNumericSettingValue().getValue();
    }
}
