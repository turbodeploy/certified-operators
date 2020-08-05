package com.vmturbo.components.common.setting;

import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableSet;

import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingSpec.Builder;
import com.vmturbo.common.protobuf.setting.SettingProto.SortedSetOfOidSettingValueType;
import com.vmturbo.common.protobuf.setting.SettingProto.SortedSetOfOidSettingValueType.Type;

/**
 * Data structure description for list of oids settings.
 */
public class SortedSetOfOidSettingDataType extends AbstractSettingDataType<Set<Long>> {

    private final Type type;

    /**
     * Construct list data type.
     *
     * @param type the type of the oids in the list
     * @param defaultValue default value
     */
    SortedSetOfOidSettingDataType(@Nonnull final Type type, @Nullable final Set<Long> defaultValue) {
        super(defaultValue);
        this.type = type;
    }

    /**
     * Adds Protobuf representation into the protobuf setting specification builder.
     *
     * @param builder builder to append setting data structure to
     */
    @Override
    public void build(@Nonnull final Builder builder) {
        final SortedSetOfOidSettingValueType.Builder settingBuilder =
                SortedSetOfOidSettingValueType.newBuilder().setType(type);
        final Set<Long> defaultValue = getDefault();
        if (defaultValue != null) {
            settingBuilder.addAllDefault(defaultValue);
        }
        builder.setSortedSetOfOidSettingValueType(settingBuilder.build());
    }

    /**
     * Extract the value out of a setting object.
     *
     * @param setting setting
     * @return value, null if absent
     */
    @Nullable
    @Override
    public Set<Long> getValue(@Nullable final Setting setting) {
        return setting == null || !setting.hasSortedSetOfOidSettingValue() ? null
                : ImmutableSet.copyOf(setting.getSortedSetOfOidSettingValue().getOidsList());
    }
}
