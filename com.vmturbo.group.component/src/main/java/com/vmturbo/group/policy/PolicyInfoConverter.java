package com.vmturbo.group.policy;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Converter;

import com.google.protobuf.InvalidProtocolBufferException;

import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyInfo;

/**
 * Convert a ScenarioSpec to/from a byte blob for serialization of the protobuf to the database.
 */
public class PolicyInfoConverter implements Converter<byte[], PolicyInfo> {
    private static final Logger logger = LogManager.getLogger();

    @Override
    public PolicyInfo from(byte[] specBlob) {
        try {
            return PolicyInfo.parseFrom(specBlob);
        } catch (InvalidProtocolBufferException e) {
            logger.error("Failed to convert specBlob to PolicyInfo: ", e);
            return null;
        }
    }

    @Override
    public byte[] to(PolicyInfo policyInfo) {
        return policyInfo.toByteArray();
    }

    @Override
    public Class<byte[]> fromType() {
        return byte[].class;
    }

    @Override
    public Class<PolicyInfo> toType() {
        return PolicyInfo.class;
    }
}
