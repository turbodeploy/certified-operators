package com.vmturbo.mediation.udt.explore;

import static com.vmturbo.mediation.udt.UdtProbe.UDT_PROBE_TAG;
import static com.vmturbo.platform.sdk.common.supplychain.SupplyChainConstants.VENDOR;
import static com.vmturbo.platform.sdk.common.util.SDKUtil.VENDOR_ID;
import static org.apache.commons.lang.StringUtils.isNotEmpty;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.ParametersAreNonnullByDefault;

import com.google.common.collect.Maps;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.mediation.udt.inventory.UdtChildEntity;
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

    /**
     * Utility classes should not have a public or default constructor.
     */
    private OidToUdtMappingTask() {

    }

    @Nonnull
    @ParametersAreNonnullByDefault
    static Set<UdtEntity> execute(Set<UdtEntity> definedEntities, DataProvider dataProvider) {
        final Set<Long> oids = getChildrenOids(definedEntities);
        final Set<TopologyEntityDTO> topologyEntities = dataProvider.getEntitiesByOids(oids);
        final Map<String, Long> udtToOidMap = createOidMap(topologyEntities);
        definedEntities.forEach(udt -> {
            if (udtToOidMap.containsKey(udt.getId())) {
                udt.setOid(udtToOidMap.get(udt.getId()));
            }
        });
        return definedEntities;
    }

    @Nonnull
    private static Set<Long> getChildrenOids(@Nonnull Set<UdtEntity> definedEntities) {
        return definedEntities.stream()
                .flatMap(entity -> entity.getChildren().stream())
                .map(UdtChildEntity::getOid)
                .collect(Collectors.toSet());
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
