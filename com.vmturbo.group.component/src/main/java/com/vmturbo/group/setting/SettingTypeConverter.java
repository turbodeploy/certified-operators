package com.vmturbo.group.setting;

import org.jooq.Converter;

import com.vmturbo.common.protobuf.setting.SettingProto.Setting.ValueCase;

/**
 * Converter for setting types.
 *
 * <p>WARNING!
 *
 * <p>We cannot trust the default Jooq's enum converter as it is relying on {@link Enum#ordinal()}.
 * If any value in the enum becomes deprecated, ordinal value of Enum changes!!!
 * Instead we are relying on protobuf's internal field numbers, which are preserved if any of
 * fields are removed from the DTO object.
 */
public class SettingTypeConverter implements Converter<Short, ValueCase> {
    @Override
    public ValueCase from(Short databaseObject) {
        return ValueCase.forNumber(databaseObject);
    }

    @Override
    public Short to(ValueCase userObject) {
        return (short)userObject.getNumber();
    }

    @Override
    public Class<Short> fromType() {
        return Short.class;
    }

    @Override
    public Class<ValueCase> toType() {
        return ValueCase.class;
    }
}
