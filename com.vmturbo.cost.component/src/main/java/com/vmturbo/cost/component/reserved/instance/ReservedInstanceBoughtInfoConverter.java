package com.vmturbo.cost.component.reserved.instance;

import org.jooq.Converter;
import org.jooq.exception.DataAccessException;

import com.google.protobuf.InvalidProtocolBufferException;

import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo;

/**
 * Convert a ReservedInstanceInfo to/from a byte blob for serialization of the protobuf to the database.
 */
public class ReservedInstanceBoughtInfoConverter implements Converter<byte[], ReservedInstanceBoughtInfo> {

    @Override
    public ReservedInstanceBoughtInfo from(byte[] instanceBlob) {
        try {
            return ReservedInstanceBoughtInfo.parseFrom(instanceBlob);
        } catch (InvalidProtocolBufferException e) {
            throw new DataAccessException("Failed to convert instance blob to ReservedInstance: ", e);
        }
    }

    @Override
    public byte[] to(ReservedInstanceBoughtInfo instance) {
        return instance.toByteArray();
    }

    @Override
    public Class<byte[]> fromType() {
        return byte[].class;
    }

    @Override
    public Class<ReservedInstanceBoughtInfo> toType() {
        return ReservedInstanceBoughtInfo.class;
    }
}
