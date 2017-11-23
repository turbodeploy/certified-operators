package com.vmturbo.group.api;

import javax.annotation.Nonnull;

import jdk.nashorn.internal.ir.annotations.Immutable;

import com.vmturbo.common.protobuf.setting.SettingProto.BooleanSettingValueType;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingSpec.Builder;

/**
 * Data structure description for boolean settings.
 */
@Immutable
public class BooleanSettingDataType extends AbstractSettingDataType<Boolean> {
    /**
     * Cpmstructs setting data type holding the specified default value.
     *
     * @param defaultValue default value to use.
     */
    public BooleanSettingDataType(boolean defaultValue) {
        super(defaultValue);
    }

    @Override
    public void build(@Nonnull Builder builder) {
        builder.setBooleanSettingValueType(BooleanSettingValueType.newBuilder()
                .setDefault(getDefault())
                .putAllEntityDefaults(getEntityDefaultsNum())
                .build());
    }
}
