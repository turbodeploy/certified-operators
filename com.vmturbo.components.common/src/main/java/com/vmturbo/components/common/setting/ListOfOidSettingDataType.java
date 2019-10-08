package com.vmturbo.components.common.setting;

import java.util.List;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.common.protobuf.setting.SettingProto.ListOfOidSettingValueType;
import com.vmturbo.common.protobuf.setting.SettingProto.ListOfOidSettingValueType.Type;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingSpec.Builder;

/**
 * Data structure description for list of oids settings.
 */
public class ListOfOidSettingDataType extends AbstractSettingDataType<List<Long>> {

    private final Type type;

    /**
     * Construct list data type.
     *
     * @param type the type of the oids in the list
     * @param defaultValue default value
     */
    ListOfOidSettingDataType(@Nonnull final Type type, @Nonnull final List<Long> defaultValue) {
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
        builder.setListOfOidSettingValueType(
            ListOfOidSettingValueType.newBuilder()
                .setType(type)
                .addAllDefault(getDefault()));
    }

    @Override
    @Nullable
    public List<Long> getValue(@Nullable Setting setting) {
        return setting == null || !setting.hasListOfOidSettingValue() ? null
                        : setting.getListOfOidSettingValue().getOidsList();
    }
}
