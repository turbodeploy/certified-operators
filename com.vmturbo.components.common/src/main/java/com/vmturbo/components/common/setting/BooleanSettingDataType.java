package com.vmturbo.components.common.setting;

import java.util.Map;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import com.vmturbo.common.protobuf.setting.SettingProto.BooleanSettingValueType;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingSpec.Builder;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Data structure description for boolean settings.
 */
@Immutable
public class BooleanSettingDataType extends AbstractSettingDataType<Boolean> {
    /**
     * Constructs setting data type holding the specified default value.
     *
     * @param defaultValue default value to use.
     */
    public BooleanSettingDataType(boolean defaultValue) {
        super(defaultValue);
    }

    /**
     * Constructs setting data type holding the specified default value.
     *
     * @param defaultValue default value to use.
     * @param entityDefaults map of default values for different entity types
     */
    public BooleanSettingDataType(boolean defaultValue, @Nonnull Map<EntityType, Boolean> entityDefaults) {
        super(defaultValue, entityDefaults);
    }

    @Override
    public void build(@Nonnull Builder builder) {
        final BooleanSettingValueType.Builder settingBuilder =
                BooleanSettingValueType.newBuilder().putAllEntityDefaults(getEntityDefaults());
        final Boolean defaultValue = getDefault();
        if (defaultValue != null) {
            settingBuilder.setDefault(defaultValue);
        }
        builder.setBooleanSettingValueType(settingBuilder.build());
    }

    @Override
    @Nullable
    public Boolean getValue(@Nullable Setting setting) {
        return setting == null || !setting.hasBooleanSettingValue()
               || !setting.getBooleanSettingValue().hasValue() ? null
                               : setting.getBooleanSettingValue().getValue();
    }
}
