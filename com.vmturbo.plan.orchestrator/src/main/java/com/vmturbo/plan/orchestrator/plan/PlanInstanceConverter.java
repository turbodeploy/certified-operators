package com.vmturbo.plan.orchestrator.plan;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Converter;

import com.google.protobuf.InvalidProtocolBufferException;

import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;

/**
 * Jooq converter for plan instance field.
 */
public class PlanInstanceConverter implements Converter<byte[], PlanInstance> {

    private final Logger logger = LogManager.getLogger();

    @Override
    public byte[] to(PlanInstance planInstance) {
        return planInstance.toByteArray();
    }

    @Override
    public PlanInstance from(byte[] bytes) {
        try {
            return PlanInstance.parseFrom(bytes);
        } catch (InvalidProtocolBufferException e) {
            logger.error("Failed to convert plan instance BLOB to PlanInstance", e);
            return null;
        }
    }

    @Override
    public Class<byte[]> fromType() {
        return byte[].class;
    }

    @Override
    public Class<PlanInstance> toType() {
        return PlanInstance.class;
    }
}
