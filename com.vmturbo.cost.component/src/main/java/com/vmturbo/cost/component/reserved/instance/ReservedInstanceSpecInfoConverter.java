package com.vmturbo.cost.component.reserved.instance;

import org.jooq.Converter;
import org.jooq.exception.DataAccessException;

import com.google.protobuf.InvalidProtocolBufferException;

import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpecInfo;

public class ReservedInstanceSpecInfoConverter implements Converter<byte[], ReservedInstanceSpecInfo> {

    @Override
    public ReservedInstanceSpecInfo from(byte[] instanceBlob) {
        try {
            return ReservedInstanceSpecInfo.parseFrom(instanceBlob);
        } catch (InvalidProtocolBufferException e) {
            throw new DataAccessException("Failed to convert instance blob to ReservedInstanceSpecInfo: ", e);
        }
    }

    @Override
    public byte[] to(ReservedInstanceSpecInfo instance) {
        return instance.toByteArray();
    }

    @Override
    public Class<byte[]> fromType() {
        return byte[].class;
    }

    @Override
    public Class<ReservedInstanceSpecInfo> toType() {
        return ReservedInstanceSpecInfo.class;
    }
}
