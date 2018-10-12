package com.vmturbo.cost.component.pricing;

import org.jooq.Converter;
import org.jooq.exception.DataAccessException;

import com.google.protobuf.InvalidProtocolBufferException;

import com.vmturbo.common.protobuf.cost.Pricing.PriceTable;

/**
 * Converter to convert between blobs in the SQL database and {@link PriceTable} protobufs.
 */
public class PriceTableConverter implements Converter<byte[], PriceTable> {
    @Override
    public PriceTable from(final byte[] databaseObject) {
        try {
            return PriceTable.parseFrom(databaseObject);
        } catch (InvalidProtocolBufferException e) {
            throw new DataAccessException("Failed to deserialize price table blob", e);
        }
    }

    @Override
    public byte[] to(final PriceTable userObject) {
        return userObject.toByteArray();
    }

    @Override
    public Class<byte[]> fromType() {
        return byte[].class;
    }

    @Override
    public Class<PriceTable> toType() {
        return PriceTable.class;
    }
}
