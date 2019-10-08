package com.vmturbo.components.common.setting;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import com.vmturbo.common.protobuf.setting.SettingProto.BooleanSettingValueType;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
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
                .putAllEntityDefaults(getEntityDefaults())
                .build());
    }

    @Override
    @Nullable
    public Boolean getValue(@Nullable Setting setting) {
        return setting == null || !setting.hasBooleanSettingValue()
               || !setting.getBooleanSettingValue().hasValue() ? null
                               : setting.getBooleanSettingValue().getValue();
    }
}
