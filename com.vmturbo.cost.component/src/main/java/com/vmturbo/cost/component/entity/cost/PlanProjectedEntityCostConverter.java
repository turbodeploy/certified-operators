package com.vmturbo.cost.component.entity.cost;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Converter;

import com.google.protobuf.InvalidProtocolBufferException;
import com.vmturbo.common.protobuf.cost.Cost;

/**
 * Convert a Cost.EntityCost to/from a byte blob for serialization of the protobuf to the database.
 */
public class PlanProjectedEntityCostConverter implements Converter<byte[], Cost.EntityCost> {

    private static final long serialVersionUID = 1234567L;

    private static final Logger logger = LogManager.getLogger();

    @Override
    public Cost.EntityCost from(byte[] specBlob) {
        try {
            if (specBlob != null) {
                return  Cost.EntityCost.parseFrom(specBlob);
            }
        } catch (InvalidProtocolBufferException e) {
            logger.error("Failed to convert specBlob to Cost.EntityCost: ", e);
            return null;
        }
        return Cost.EntityCost.getDefaultInstance();
    }

    @Override
    public byte[] to(Cost.EntityCost entityCost) {
        return entityCost.toByteArray();
    }

    @Override
    public Class<byte[]> fromType() {
        return byte[].class;
    }

    @Override
    public Class<Cost.EntityCost> toType() {
        return Cost.EntityCost.class;
    }
}
