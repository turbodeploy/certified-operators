package com.vmturbo.group.persistent;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy.Type;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicyInfo;

/**
 * An interface to abstract away the validation of setting policies in order
 * to make unit testing easier.
 */
@FunctionalInterface
public interface SettingPolicyValidator {
    /**
     * Verify that a {@link SettingPolicyInfo} meets all the requirements - e.g. that all
     * setting names are valid, that the setting values match the expected types, that all
     * required fields are set, etc.
     *
     * @param settingPolicyInfo The {@link SettingPolicyInfo} to validate.
     * @param type The {@link Type} of the policy. Default and scope
     * policies have slightly different validation rules.
     * @throws InvalidSettingPolicyException If the policy is invalid.
     */
    void validateSettingPolicy(@Nonnull final SettingPolicyInfo settingPolicyInfo,
            @Nonnull final Type type) throws InvalidSettingPolicyException;
}
