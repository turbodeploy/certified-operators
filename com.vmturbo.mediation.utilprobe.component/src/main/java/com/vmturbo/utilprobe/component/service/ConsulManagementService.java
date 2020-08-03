package com.vmturbo.utilprobe.component.service;

import java.util.Map;
import java.util.Optional;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.stereotype.Service;

import com.vmturbo.kvstore.KeyValueStore;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;

/**
 * Service for connecting to Consul.
 */
@Service
public class ConsulManagementService {

    private static final String PROBE_KV_STORE_PREFIX = "probes/";
    private KeyValueStore keyValueStore;
    private static final Logger logger = LogManager.getLogger(ConsulManagementService.class);

    /**
     * {@link ConsulManagementService} constructor.
     *
     * @param keyValueStore key/value store of Consul
     */
    public ConsulManagementService(KeyValueStore keyValueStore) {
        logger.info("Initializing ConsulManagementService store");
        this.keyValueStore = keyValueStore;
    }

    /**
     * Deleting probe info from Consul.
     *
     * @param probeCategory category of probe
     * @param probeType type of probe
     */
    public void deleteConfig(String probeCategory, String probeType) {
        logger.info("Deleting old probe info started");
        Map<String, String> probes = keyValueStore.getByPrefix(PROBE_KV_STORE_PREFIX);
        Optional<Map.Entry<String, String>> oldProbeInfo = probes.entrySet().stream().filter(entry -> {
                    try {
                        final ProbeInfo.Builder probeInfoBuilder = ProbeInfo.newBuilder();
                        JsonFormat.parser().merge(entry.getValue(), probeInfoBuilder);
                        ProbeInfo info = probeInfoBuilder.build();
                        return probeCategory.equals(info.getProbeCategory()) &&
                                probeType.equals(info.getProbeType());
                    } catch (InvalidProtocolBufferException e) {
                        logger.error("Failed to load probe info from Consul, cannot parse data from {} key",
                                entry.getKey());
                        return false;
                    }
                }).findFirst();
        if (oldProbeInfo.isPresent()) {
            final String key = oldProbeInfo.get().getKey();
            logger.info("Deleting probe info for probe {} with type '{}' and probe '{}'",
                    key, probeType, probeCategory);
            keyValueStore.removeKey(key);
            if (!keyValueStore.containsKey(key)) {
                logger.info("Probe info deleted successfully");
            } else {
                logger.error("Cannot delete probe info with key {}", key);
            }
        } else {
            logger.info("No config found for probe with type '{}' and category '{}'",
                    probeType, probeCategory);
        }
    }
}
