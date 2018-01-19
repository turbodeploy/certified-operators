package com.vmturbo.plan.orchestrator.reservation;

import org.jooq.Converter;
import org.jooq.exception.DataAccessException;

import com.google.protobuf.InvalidProtocolBufferException;

import com.vmturbo.common.protobuf.plan.ReservationDTO.ConstraintInfoCollection;

/**
 * Convert a ConstraintInfoCollection to/from a byte blob for serialization of the protobuf to the
 * database.
 */
public class ConstraintInfoCollectionConverter implements Converter<byte[], ConstraintInfoCollection> {

    @Override
    public ConstraintInfoCollection from(byte[] instanceBlob) {
        try {
            return ConstraintInfoCollection.parseFrom(instanceBlob);
        } catch (InvalidProtocolBufferException e) {
            throw new DataAccessException("Failed to convert instanceBlob to ConstraintInfoCollection:", e);
        }
    }

    @Override
    public byte[] to(ConstraintInfoCollection instance) {
        return instance.toByteArray();
    }

    @Override
    public Class<byte[]> fromType() {
        return byte[].class;
    }

    @Override
    public Class<ConstraintInfoCollection> toType() {
        return ConstraintInfoCollection.class;
    }
}
