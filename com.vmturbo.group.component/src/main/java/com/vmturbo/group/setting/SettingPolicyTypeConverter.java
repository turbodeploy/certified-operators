package com.vmturbo.group.setting;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy.Type;
import com.vmturbo.group.db.enums.SettingPolicyPolicyType;


/**
 * A converter between {@link Type} and {@link SettingPolicyPolicyType}.
 * <p>
 * Using this instead of a JOOQ converter because attempting to create a converter
 * for {@link Type} and bind the policy_type column to it meant that
 * {@link SettingPolicyPolicyType} never got generated, so we would have to go from
 * {@link Type} to string, which is not ideal.
 */
class SettingPolicyTypeConverter {

    @Nonnull
    static SettingPolicyPolicyType typeToDb(@Nonnull final Type type) {
        switch (type) {
            case DEFAULT:
                return SettingPolicyPolicyType.default_;
            case USER:
                return SettingPolicyPolicyType.user;
            case DISCOVERED:
                return SettingPolicyPolicyType.discovered;
            default:
                throw new IllegalArgumentException("Unexpected type: " + type);
        }
    }

    @Nonnull
    static Type typeFromDb(@Nonnull final SettingPolicyPolicyType dbType) {
        switch (dbType) {
            case user:
                return Type.USER;
            case default_:
                return Type.DEFAULT;
            case discovered:
                return Type.DISCOVERED;
            default:
                throw new IllegalArgumentException("Unexpected database type: " + dbType);
        }
    }
}
