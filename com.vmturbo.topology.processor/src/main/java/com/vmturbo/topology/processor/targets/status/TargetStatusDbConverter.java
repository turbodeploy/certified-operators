package com.vmturbo.topology.processor.targets.status;

import com.google.protobuf.InvalidProtocolBufferException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Converter;

import com.vmturbo.topology.processor.targets.TargetStatusOuterClass.TargetStatus;

/**
 * Convert a {@link TargetStatus}.ProbeStageDetails to/from a byte blob for serialization to the database.
 */
public class TargetStatusDbConverter implements Converter<byte[], TargetStatus> {

    private static final long serialVersionUID = 1L;
    private static final Logger logger = LogManager.getLogger();

    @Override
    public  TargetStatus from(byte[] specBlob) {
        try {
            if (specBlob != null) {
                return TargetStatus.parseFrom(specBlob);
            }
        } catch (InvalidProtocolBufferException e) {
            logger.error("Failed to convert specBlob to DiscoveryREST.ProbeStageDetails: ", e);
        }
        return TargetStatus.getDefaultInstance();
    }

    @Override
    public byte[] to(TargetStatus stageDetails) {
        return stageDetails.toByteArray();
    }

    @Override
    public Class<byte[]> fromType() {
        return byte[].class;
    }

    @Override
    public Class<TargetStatus> toType() {
        return TargetStatus.class;
    }

}
