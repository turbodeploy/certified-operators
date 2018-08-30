package com.vmturbo.action.orchestrator.workflow.store;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Converter;

import com.google.protobuf.InvalidProtocolBufferException;

import com.vmturbo.common.protobuf.workflow.WorkflowDTO;

/**
 * Convert a WorkflowDTO.WorkflowInfo to/from a byte blob for serialization of the protobuf to the database.
 */
public class WorkflowInfoConverter implements Converter<byte[], WorkflowDTO.WorkflowInfo> {
    private static final Logger logger = LogManager.getLogger();

    @Override
    public  WorkflowDTO.WorkflowInfo from(byte[] specBlob) {
        try {
            if (specBlob != null) {
                return  WorkflowDTO.WorkflowInfo.parseFrom(specBlob);
            }
        } catch (InvalidProtocolBufferException e) {
            logger.error("Failed to convert specBlob to WorkflowDTO.WorkflowInfo: ", e);
            return null;
        }
        return  WorkflowDTO.WorkflowInfo.getDefaultInstance();
    }

    @Override
    public byte[] to( WorkflowDTO.WorkflowInfo workflowInfo) {
        return workflowInfo.toByteArray();
    }

    @Override
    public Class<byte[]> fromType() {
        return byte[].class;
    }

    @Override
    public Class<WorkflowDTO.WorkflowInfo> toType() {
        return WorkflowDTO.WorkflowInfo.class;
    }
}

