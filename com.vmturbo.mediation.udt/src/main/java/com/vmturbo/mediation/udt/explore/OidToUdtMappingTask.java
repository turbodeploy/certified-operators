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
                final Map<String, Long> definitionIdToOidMap = createOidMap(entitiesDiscoveredByUdt);
                definedEntities.forEach(udt -> {
                    if (definitionIdToOidMap .containsKey(udt.getId())) {
                        long oid = definitionIdToOidMap .get(udt.getId());
                        LOGGER.trace("OID/UDT Mapping: UDT entity {} has OID {}.", udt.getId(), oid);
                        udt.setOid(oid);
                    }
                });
            }
        }
        return definedEntities;
    }

    @Nonnull
    private static Map<String, Long> createOidMap(@Nonnull Set<TopologyEntityDTO> topologyEntities) {
        final Map<String, Long> map = Maps.newHashMap();
        for (TopologyEntityDTO entity : topologyEntities) {
            final Long oid = entity.getOid();
            final String vendor = entity.getEntityPropertyMapMap().get(VENDOR);
            final String definitionId = entity.getEntityPropertyMapMap().get(VENDOR_ID);
            if (isNotEmpty(vendor) && isNotEmpty(definitionId) && vendor.equals(UDT_PROBE_TAG)) {
                map.put(definitionId, oid);
            }
        }
        return map;
    }
}
