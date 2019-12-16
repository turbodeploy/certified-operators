package com.vmturbo.plan.orchestrator.project;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Converter;

import com.google.protobuf.InvalidProtocolBufferException;

import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProjectInfo;

/**
 * Convert a PlanProjectInfo to/from a byte blob for serialization of the protobuf to the database.
 */
public class PlanProjectInfoConverter implements Converter<byte[], PlanProjectInfo> {
    private static final Logger logger = LogManager.getLogger();

    @Override
    public PlanProjectInfo from(byte[] infoBlob) {
        try {
            return PlanProjectInfo.parseFrom(infoBlob);
        } catch (InvalidProtocolBufferException e) {
            logger.error("Failed to convert infoBlob to PlanProjectInfo: ", e);
            return null;
        }
    }

    @Override
    public byte[] to(PlanProjectInfo planProjectInfo) {
        return planProjectInfo.toByteArray();
    }

    @Override
    public Class<byte[]> fromType() {
        return byte[].class;
    }

    @Override
    public Class<PlanProjectInfo> toType() {
        return PlanProjectInfo.class;
    }
}
