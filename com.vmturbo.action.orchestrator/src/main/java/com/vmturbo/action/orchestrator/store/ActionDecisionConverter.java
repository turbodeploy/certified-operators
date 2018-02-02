package com.vmturbo.action.orchestrator.store;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Converter;

import com.google.protobuf.InvalidProtocolBufferException;

import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionDecision;

/**
 * Convert a ActionDTO.ActionDecision to/from a byte blob for serialization of the protobuf to the database.
 */
public class ActionDecisionConverter implements Converter<byte[], ActionDTO.ActionDecision> {
    private static final Logger logger = LogManager.getLogger();

    @Override
    public ActionDecision from(byte[] specBlob) {
        try {
            if (specBlob != null) {
                return ActionDTO.ActionDecision.parseFrom(specBlob);
            }
        } catch (InvalidProtocolBufferException e) {
            logger.error("Failed to convert specBlob to ActionDTO.ActionDecision: ", e);
            return null;
        }
        return ActionDecision.getDefaultInstance();
    }

    @Override
    public byte[] to(ActionDecision actionDecision) {
        return actionDecision.toByteArray();
    }

    @Override
    public Class<byte[]> fromType() {
        return byte[].class;
    }

    @Override
    public Class<ActionDecision> toType() {
        return ActionDecision.class;
    }
}
