package com.vmturbo.plan.orchestrator.plan.export;

import com.google.protobuf.InvalidProtocolBufferException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Converter;

import com.vmturbo.common.protobuf.plan.PlanExportDTO;

/**
 * Convert a PlanExportDTO.PlanDestination to/from a byte blob for serialization of the protobuf to the database.
 */
public class PlanDestinationConverter implements Converter<byte[], PlanExportDTO.PlanDestination> {
    private static final Logger logger = LogManager.getLogger();

    @Override
    public PlanExportDTO.PlanDestination from(byte[] blob) {
        try {
            return PlanExportDTO.PlanDestination.parseFrom(blob);
        } catch (InvalidProtocolBufferException e) {
            logger.error("Failed to convert blob to PlanDestination: ", e);
            return null;
        }
    }

    @Override
    public byte[] to(PlanExportDTO.PlanDestination planDestination) {
        return planDestination.toByteArray();
    }

    @Override
    public Class<byte[]> fromType() {
        return byte[].class;
    }

    @Override
    public Class<PlanExportDTO.PlanDestination> toType() {
        return PlanExportDTO.PlanDestination.class;
    }
}
