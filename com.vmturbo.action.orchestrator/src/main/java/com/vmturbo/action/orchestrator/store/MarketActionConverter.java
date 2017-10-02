package com.vmturbo.action.orchestrator.store;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Converter;

import com.google.protobuf.InvalidProtocolBufferException;

import com.vmturbo.common.protobuf.action.ActionDTO;

/**
 * Convert a ActionDTO.Action to/from a byte blob for serialization of the protobuf to the database.
 */
public class MarketActionConverter implements Converter<byte[], ActionDTO.Action> {
    private static final Logger logger = LogManager.getLogger();

    @Override
    public ActionDTO.Action from(byte[] specBlob) {
        try {
            return ActionDTO.Action.parseFrom(specBlob);
        } catch (InvalidProtocolBufferException e) {
            logger.error("Failed to convert specBlob to ActionDTO.Action: ", e);
            return null;
        }
    }

    @Override
    public byte[] to(ActionDTO.Action actionDTO) {
        return actionDTO.toByteArray();
    }

    @Override
    public Class<byte[]> fromType() {
        return byte[].class;
    }

    @Override
    public Class<ActionDTO.Action> toType() {
        return ActionDTO.Action.class;
    }
}
