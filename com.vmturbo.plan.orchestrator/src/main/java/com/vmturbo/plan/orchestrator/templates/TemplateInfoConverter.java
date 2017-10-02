package com.vmturbo.plan.orchestrator.templates;

import org.jooq.Converter;
import org.jooq.exception.DataAccessException;

import com.google.protobuf.InvalidProtocolBufferException;

import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateInfo;

/**
 * Convert a TemplateInstance to/from a byte blob for serialization of the protobuf to the database.
 */
public class TemplateInfoConverter implements Converter<byte[], TemplateInfo> {

    @Override
    public TemplateInfo from(byte[] instanceBlob) {
        try {
            return TemplateInfo.parseFrom(instanceBlob);
        } catch (InvalidProtocolBufferException e) {
            throw new DataAccessException("Failed to convert instanceBlob to TemplateInstance:", e);
        }
    }

    @Override
    public byte[] to(TemplateInfo instance) {
        return instance.toByteArray();
    }

    @Override
    public Class<byte[]> fromType() {
        return byte[].class;
    }

    @Override
    public Class<TemplateInfo> toType() {
        return TemplateInfo.class;
    }
}
