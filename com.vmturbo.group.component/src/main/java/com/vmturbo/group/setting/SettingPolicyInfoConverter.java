package com.vmturbo.group.setting;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Converter;

import com.google.protobuf.InvalidProtocolBufferException;

import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicyInfo;

/**
 * Convert a ScenarioSpec to/from a byte blob for serialization of the protobuf to the database.
 */
public class SettingPolicyInfoConverter implements Converter<byte[], SettingPolicyInfo> {
    private static final Logger logger = LogManager.getLogger();

    @Override
    public SettingPolicyInfo from(byte[] specBlob) {
        try {
            return SettingPolicyInfo.parseFrom(specBlob);
        } catch (InvalidProtocolBufferException e) {
            logger.error("Failed to convert specBlob to SettingPolicyInfo: ", e);
            return null;
        }
    }

    @Override
    public byte[] to(SettingPolicyInfo settingPolicyInfo) {
        return settingPolicyInfo.toByteArray();
    }

    @Override
    public Class<byte[]> fromType() {
        return byte[].class;
    }

    @Override
    public Class<SettingPolicyInfo> toType() {
        return SettingPolicyInfo.class;
    }
}
