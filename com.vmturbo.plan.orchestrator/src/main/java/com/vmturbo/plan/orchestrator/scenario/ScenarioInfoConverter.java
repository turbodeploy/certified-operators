package com.vmturbo.plan.orchestrator.scenario;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Converter;

import com.google.protobuf.InvalidProtocolBufferException;

import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioInfo;

/**
 * Convert a ScenarioInfo to/from a byte blob for serialization of the protobuf to the database.
 */
public class ScenarioInfoConverter implements Converter<byte[], ScenarioInfo> {
    private static final Logger logger = LogManager.getLogger();

    @Override
    public ScenarioInfo from(byte[] infoBlob) {
        try {
            return ScenarioInfo.parseFrom(infoBlob);
        } catch (InvalidProtocolBufferException e) {
            logger.error("Failed to convert infoBlob to ScenarioInfo: ", e);
            return null;
        }
    }

    @Override
    public byte[] to(ScenarioInfo scenarioInfo) {
        return scenarioInfo.toByteArray();
    }

    @Override
    public Class<byte[]> fromType() {
        return byte[].class;
    }

    @Override
    public Class<ScenarioInfo> toType() {
        return ScenarioInfo.class;
    }
}
