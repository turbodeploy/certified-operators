package com.vmturbo.plan.orchestrator.deployment.profile;

import org.jooq.Converter;
import org.jooq.exception.DataAccessException;

import com.google.protobuf.InvalidProtocolBufferException;

import com.vmturbo.common.protobuf.plan.DeploymentProfileDTO.DeploymentProfileInfo;

/**
 * Convert a Deployment profile to/from a byte blob for serialization of the protobuf to the database.
 */
public class DeploymentProfileInfoConverter implements Converter<byte[], DeploymentProfileInfo> {

    @Override
    public DeploymentProfileInfo from(byte[] instanceBlob) {
        try {
            return DeploymentProfileInfo.parseFrom(instanceBlob);
        } catch (InvalidProtocolBufferException e) {
            throw new DataAccessException("Failed to convert instanceBlob to Deployment profile:", e);
        }
    }

    @Override
    public byte[] to(DeploymentProfileInfo instance) {
        return instance.toByteArray();
    }

    @Override
    public Class<byte[]> fromType() {
        return byte[].class;
    }

    @Override
    public Class<DeploymentProfileInfo> toType() {
        return DeploymentProfileInfo.class;
    }
}

