package com.vmturbo.api.component.external.api.mapper.aspect;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.api.dto.entityaspect.EntityAspect;
import com.vmturbo.api.dto.entityaspect.RegionAspectApiDTO;
import com.vmturbo.api.enums.AspectName;
import com.vmturbo.common.protobuf.RepositoryDTOUtil;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.GetMultiSupplyChainsRequest;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainScope;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainSeed;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc.SupplyChainServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.GeoDataInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.RegionInfo;

/**
 * RegionAspectMapper to map region to its aspects.
 * At present we only have GeoData attached into region aspects.
 */
public class RegionAspectMapper extends AbstractAspectMapper {

    private SupplyChainServiceBlockingStub supplyChainBlockingStub;

    /**
     * Constructor.
     *
     * @param supplyChainBlockingStub supply chain service api
     */
    public RegionAspectMapper(SupplyChainServiceBlockingStub supplyChainBlockingStub) {
        this.supplyChainBlockingStub = supplyChainBlockingStub;
    }

    /**
     * Map the topology entity dto to RegionAspectApiDTO.
     *
     * @param entity The topology entity that we will be fetching aspects for.
     * @return an instance of RegionAspectApiDTO containing aspects related to this region.
     */
    @Override
    @Nullable
    public EntityAspect mapEntityToAspect(@Nonnull final TopologyEntityDTO entity) {
        Optional<Map<Long, EntityAspect>> result = mapEntityToAspectBatch(Collections.singletonList(entity));
        if (result.isPresent()) {
            result.get().get(entity.getOid());
        }
        return null;
    }

    /**
     * Map a list of TopologyEntityDTO objects (of type Region) to the corresponding RegionAspectApiDTO objects.
     *
     * @param entities a list of TopologyEntityDTO objects of type Region. each identified by unique oid.
     * @return A map of oid -> RegionAspectApiDTO objects.
     */
    @Override
    @Nonnull
    public Optional<Map<Long, EntityAspect>> mapEntityToAspectBatch(@Nonnull final List<TopologyEntityDTO> entities) {
        Map<Long, Integer> workloadCount = getWorkloadCount(entities);
        Map<Long, EntityAspect> response = new HashMap<>();

        entities.stream().forEach(entity -> {
            final TypeSpecificInfo typeSpecificInfo = entity.getTypeSpecificInfo();
            if (typeSpecificInfo != null && typeSpecificInfo.hasRegion()) {
                final RegionInfo regionInfo = typeSpecificInfo.getRegion();
                if (regionInfo != null && regionInfo.hasGeoData()) {
                    final GeoDataInfo geoDataInfo = regionInfo.getGeoData();
                    final RegionAspectApiDTO regionAspectApiDTO = new RegionAspectApiDTO();
                    regionAspectApiDTO.setLatitude(geoDataInfo.getLatitude());
                    regionAspectApiDTO.setLongitude(geoDataInfo.getLongitude());
                    if (workloadCount.containsKey(entity.getOid())) {
                        regionAspectApiDTO.setNumWorkloads(workloadCount.get(entity.getOid()));
                    }
                    response.put(entity.getOid(), regionAspectApiDTO);
                }
            }
        });

        return Optional.of(response);
    }

    private Map<Long, Integer> getWorkloadCount(@Nonnull final List<TopologyEntityDTO> entities) {
        GetMultiSupplyChainsRequest.Builder builder = GetMultiSupplyChainsRequest.newBuilder();
        entities.forEach(entity -> {
            builder.addSeeds(SupplyChainSeed.newBuilder()
                    .setSeedOid(entity.getOid())
                    .setScope(SupplyChainScope.newBuilder()
                            .addStartingEntityOid(entity.getOid())
                            .addEntityTypesToInclude(ApiEntityType.VIRTUAL_MACHINE.typeNumber())
                            .addEntityTypesToInclude(ApiEntityType.DATABASE_SERVER.typeNumber())
                            .addEntityTypesToInclude(ApiEntityType.DATABASE.typeNumber())));
        });

        Map<Long, Integer> result = new HashMap<>();

        supplyChainBlockingStub.getMultiSupplyChains(builder.build()).forEachRemaining(response -> {
            result.put(response.getSeedOid(),
                    response.getSupplyChain().getSupplyChainNodesList().stream()
                            .mapToInt(RepositoryDTOUtil::getMemberCount)
                            .sum());
        });

        return result;
    }

    /**
     * Get Aspect Name for this aspect.
     *
     * @return aspect name
     */
    @Override
    @Nonnull
    public AspectName getAspectName() {
        return AspectName.REGION;
    }
}
