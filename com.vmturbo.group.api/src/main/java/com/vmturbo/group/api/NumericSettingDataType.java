package com.vmturbo.group.api;

import java.util.Collections;
import java.util.Map;

import javax.annotation.Nonnull;

import jdk.nashorn.internal.ir.annotations.Immutable;

import com.vmturbo.common.protobuf.setting.SettingProto.NumericSettingValueType;
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
        builder.setNumericSettingValueType(NumericSettingValueType.newBuilder()
                .setMin(min)
                .setMax(max)
                .setDefault(getDefault())
                .putAllEntityDefaults(getEntityDefaultsNum())
                .build());
    }
}
