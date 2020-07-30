package com.vmturbo.components.common.setting;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import org.apache.commons.lang3.Validate;

import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingSpec.Builder;
import com.vmturbo.common.protobuf.setting.SettingProto.StringSettingValueType;

/**
 * Data structure description for string settings.
 */
@Immutable
public class StringSettingDataType extends AbstractSettingDataType<String> {
    private final String regexp;

    /**
     * COnstructs string data type.
     *
     * @param defaultValue default value
     * @param regexp regular expression for values to match.
     */
    public StringSettingDataType(@Nonnull String defaultValue, @Nonnull String regexp) {
        super(defaultValue);
        this.regexp = Validate.notBlank(regexp);
    }

    /**
     * Return regular expression this value must match.
     *
     * @return matching reg exp.
     */
    @Nonnull
    public String getRegexp() {
        return regexp;
    }

    @Override
    public void build(@Nonnull Builder builder) {
        builder.setStringSettingValueType(StringSettingValueType.newBuilder()
                .setDefault(getDefault())
                .setValidationRegex(regexp)
                .putAllEntityDefaults(getEntityDefaults())
                .build());
    }

    @Override
    @Nullable
    public String getValue(@Nullable Setting setting) {
        return setting == null || !setting.hasStringSettingValue()
                        || !setting.getStringSettingValue().hasValue() ? null
                                        : setting.getStringSettingValue().getValue();
    }
}
