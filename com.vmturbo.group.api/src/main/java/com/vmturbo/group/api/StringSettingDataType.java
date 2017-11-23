package com.vmturbo.group.api;

import javax.annotation.Nonnull;

import org.apache.commons.lang3.Validate;

import jdk.nashorn.internal.ir.annotations.Immutable;

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
        super(Validate.notBlank(defaultValue));
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
                .putAllEntityDefaults(getEntityDefaultsNum())
                .build());
    }
}
