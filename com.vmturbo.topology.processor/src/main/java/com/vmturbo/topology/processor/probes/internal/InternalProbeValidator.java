package com.vmturbo.topology.processor.probes.internal;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;

/**
 * A class is responsible for validation of mandatory fields of an internal probe.
 */
class InternalProbeValidator {

    private static final Logger LOGGER = LogManager.getLogger();

    private InternalProbeValidator() {

    }

    static boolean isProbeValid(@Nonnull ProbeInfo probeInfo) {
        if (probeInfo.getCreationMode() != ProbeInfo.CreationMode.INTERNAL) {
            LOGGER.warn("Incorrect internal probe. Creation mode: {}", probeInfo.getCreationMode());
            return false;
        } else if (probeInfo.getSupplyChainDefinitionSetCount() == 0) {
            LOGGER.warn("Incorrect internal probe. Empty supply chain definition set.");
            return false;
        } else if (probeInfo.getEntityMetadataCount() == 0) {
            LOGGER.warn("Incorrect internal probe. Empty entity metadata.");
            return false;
        }
        return true;
    }
}
