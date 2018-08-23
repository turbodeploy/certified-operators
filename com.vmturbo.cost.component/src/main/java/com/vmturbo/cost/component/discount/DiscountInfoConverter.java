package com.vmturbo.cost.component.discount;

import org.jooq.Converter;
import org.jooq.exception.DataAccessException;

import com.google.protobuf.InvalidProtocolBufferException;

import com.vmturbo.common.protobuf.cost.Cost.DiscountInfo;

/**
 * Convert a DiscountInfo to/from a byte blob for serialization of the protobuf to the database.
 */
public class DiscountInfoConverter implements Converter<byte[], DiscountInfo> {

    @Override
    public DiscountInfo from(byte[] instanceBlob) {
        try {
            return DiscountInfo.parseFrom(instanceBlob);
        } catch (InvalidProtocolBufferException e) {
            throw new DataAccessException("Failed to convert instance blob to Discount: ", e);
        }
    }

    @Override
    public byte[] to(DiscountInfo instance) {
        return instance.toByteArray();
    }

    @Override
    public Class<byte[]> fromType() {
        return byte[].class;
    }

    @Override
    public Class<DiscountInfo> toType() {
        return DiscountInfo.class;
    }
}
