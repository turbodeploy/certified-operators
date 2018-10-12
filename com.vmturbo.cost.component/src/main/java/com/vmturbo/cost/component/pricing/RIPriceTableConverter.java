package com.vmturbo.cost.component.pricing;

import org.jooq.Converter;
import org.jooq.exception.DataAccessException;

import com.google.protobuf.InvalidProtocolBufferException;

import com.vmturbo.common.protobuf.cost.Pricing.ReservedInstancePriceTable;

/**
 * Converter to convert between blobs in the SQL database and {@link ReservedInstancePriceTable} protobufs.
 */
public class RIPriceTableConverter implements Converter<byte[], ReservedInstancePriceTable> {
    @Override
    public ReservedInstancePriceTable from(final byte[] databaseObject) {
        try {
            return ReservedInstancePriceTable.parseFrom(databaseObject);
        } catch (InvalidProtocolBufferException e) {
            throw new DataAccessException("Failed to deserialize RI price table blob", e);
        }
    }

    @Override
    public byte[] to(final ReservedInstancePriceTable userObject) {
        return userObject.toByteArray();
    }

    @Override
    public Class<byte[]> fromType() {
        return byte[].class;
    }

    @Override
    public Class<ReservedInstancePriceTable> toType() {
        return ReservedInstancePriceTable.class;
    }
}
