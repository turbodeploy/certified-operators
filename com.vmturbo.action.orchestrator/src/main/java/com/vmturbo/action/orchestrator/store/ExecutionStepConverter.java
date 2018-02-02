package com.vmturbo.action.orchestrator.store;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Converter;

import com.google.protobuf.InvalidProtocolBufferException;

import com.vmturbo.common.protobuf.action.ActionDTO;

/**
 * Convert a ActionDTO.ExecutionStep to/from a byte blob for serialization of the protobuf to the database.
 */
public class ExecutionStepConverter implements Converter<byte[], ActionDTO.ExecutionStep> {
    private static final Logger logger = LogManager.getLogger();

    @Override
    public ActionDTO.ExecutionStep from(byte[] specBlob) {
        try {
            if (specBlob != null) {
                return ActionDTO.ExecutionStep.parseFrom(specBlob);
            }
        } catch (InvalidProtocolBufferException e) {
            logger.error("Failed to convert specBlob to ActionDTO.ExecutionStep: ", e);
            return null;
        }
        return ActionDTO.ExecutionStep.getDefaultInstance();
    }

    @Override
    public byte[] to(ActionDTO.ExecutionStep executionStep) {
        return executionStep.toByteArray();
    }

    @Override
    public Class<byte[]> fromType() {
        return byte[].class;
    }

    @Override
    public Class<ActionDTO.ExecutionStep> toType() {
        return ActionDTO.ExecutionStep.class;
    }
}
