package com.vmturbo.plan.orchestrator.reservation;

import org.jooq.Converter;
import org.jooq.exception.DataAccessException;

import com.google.protobuf.InvalidProtocolBufferException;

import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationTemplateCollection;

/**
 * Convert a ReservationTemplateCollection to/from a byte blob for serialization of the protobuf to
 * the database.
 */
public class ReservationTemplateCollectionConverter implements Converter<byte[], ReservationTemplateCollection> {

    @Override
    public ReservationTemplateCollection from(byte[] instanceBlob) {
        try {
            return ReservationTemplateCollection.parseFrom(instanceBlob);
        } catch (InvalidProtocolBufferException e) {
            throw new DataAccessException("Failed to convert instanceBlob to ReservationInstanceCollection:", e);
        }
    }

    @Override
    public byte[] to(ReservationTemplateCollection instance) {
        return instance.toByteArray();
    }

    @Override
    public Class<byte[]> fromType() {
        return byte[].class;
    }

    @Override
    public Class<ReservationTemplateCollection> toType() {
        return ReservationTemplateCollection.class;
    }
}
