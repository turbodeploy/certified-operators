package com.vmturbo.mediation.udt.explore;

import static com.vmturbo.mediation.udt.UdtProbe.UDT_PROBE_TAG;
import static com.vmturbo.platform.sdk.common.supplychain.SupplyChainConstants.VENDOR;
import static com.vmturbo.platform.sdk.common.util.SDKUtil.VENDOR_ID;
import static org.apache.commons.lang.StringUtils.isNotEmpty;

import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.ParametersAreNonnullByDefault;

import com.google.common.collect.Maps;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.mediation.udt.inventory.UdtEntity;

/**
 * A special task for mapping OIDs to TDD-IDs.
 * If a topology entity was created from TDD it contains properties which are used for
 * mapping its OID to its TDD-ID:
 *      VENDOR:     UDT
 *      VENDOR_ID:  TDD-ID
 * In this case we call UdtEntity.setOid() and the OID value is put to EntityDTO in
 * the discovery response.
 */
class OidToUdtMappingTask {

    private static final Logger LOGGER = LogManager.getLogger();

    /**
     * Utility classes should not have a public or default constructor.
     */
    private OidToUdtMappingTask() {

    }

    @Nonnull
    @ParametersAreNonnullByDefault
    static Set<UdtEntity> execute(Set<UdtEntity> definedEntities, DataProvider dataProvider) {
        final Long targetId = dataProvider.getUdtTargetId();
        if (targetId != null) {
            final Set<TopologyEntityDTO> entitiesDiscoveredByUdt = dataProvider.searchEntitiesByTargetId(targetId);
            LOGGER.info("OID/UDT Mapping: Retrieved {} UDT entities.", entitiesDiscoveredByUdt.size());
            if (!entitiesDiscoveredByUdt.isEmpty()) {
                final Map<Long, Long> oidToDefinitionIdMap = createOidMap(entitiesDiscoveredByUdt);
                definedEntities.stream()
                        .flatMap(udtEntity -> udtEntity.getChildren().stream())
                        .forEach(child -> {
                            final long childOid = child.getOid();
                            if (oidToDefinitionIdMap.containsKey(childOid)) {
                                long udtId = oidToDefinitionIdMap.get(childOid);
                                LOGGER.trace("OID/UDT Mapping: OID entity {} has UDT-ID {}.", childOid, udtId);
                                child.setUdtId(udtId);
                            }
                        });
            }
        }
        return definedEntities;
    }

    /**
     * Creates a mapping of IDs.
     * KEY   = OID
     * VALUE = UDT-ID
     *
     * @param topologyEntities - entities to process
     * @return a map of IDs.
     */
    @Nonnull
    private static Map<Long, Long> createOidMap(@Nonnull Set<TopologyEntityDTO> topologyEntities) {
        final Map<Long, Long> map = Maps.newHashMap();
        for (TopologyEntityDTO entity : topologyEntities) {
            final Long oid = entity.getOid();
            final String vendor = entity.getEntityPropertyMapMap().get(VENDOR);
            final String definitionId = entity.getEntityPropertyMapMap().get(VENDOR_ID);
            if (isNotEmpty(vendor) && isNotEmpty(definitionId) && vendor.equals(UDT_PROBE_TAG)) {
                try {
                    map.put(oid, Long.valueOf(definitionId));
                } catch (NumberFormatException e) {
                    LOGGER.warn("Incorrect UDT ID.", e);
                }
            }
        }
        return map;
    }
}
