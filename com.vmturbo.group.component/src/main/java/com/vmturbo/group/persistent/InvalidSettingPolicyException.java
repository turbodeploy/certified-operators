package com.vmturbo.group.persistent;

import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicyInfo;

/**
 * Indicates an invalid {@link SettingPolicyInfo} input to the {@link SettingStore}.
 */
public class InvalidSettingPolicyException extends Exception {
    public InvalidSettingPolicyException(final String message) {
        super(message);
    }
}
