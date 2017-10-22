package com.vmturbo.repository.topology;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.repository.constant.RepoObjectState;
import com.vmturbo.repository.constant.RepoObjectType;
import com.vmturbo.repository.dto.CommoditiesBoughtRepoFromProviderDTO;
import com.vmturbo.repository.dto.CommodityBoughtRepoDTO;
import com.vmturbo.repository.dto.CommoditySoldRepoDTO;
import com.vmturbo.repository.dto.ServiceEntityRepoDTO;

/**
 * Convert topology DTOs to repository DTOs.
 */
public class TopologyConverter {

    public static Set<ServiceEntityRepoDTO> convert(Collection<TopologyEntityDTO> topologyDTOs) {
        return topologyDTOs.stream().map(ServiceEntityMapper::convert).collect(Collectors.toSet());
    }

    static class ServiceEntityMapper {

        private static ServiceEntityRepoDTO convert(TopologyEntityDTO t) {
            String seOid = Long.toString(t.getOid());
            ServiceEntityRepoDTO se = new ServiceEntityRepoDTO();
            se.setOid(seOid);
            se.setDisplayName(t.getDisplayName());
            se.setEntityType(mapEntityType(t.getEntityType()));
            se.setUuid(String.valueOf(t.getOid()));
            se.setState(mapEntityState(t.getEntityState()));

            // Commodities bought list
            List<CommoditiesBoughtRepoFromProviderDTO> commoditiesBoughtRepoFromProviderDTOList = Lists.newArrayList();
            t.getCommoditiesBoughtFromProvidersList().forEach(commoditiesBoughtFromProvider -> {
                commoditiesBoughtRepoFromProviderDTOList.add(
                    CommodityMapper.convert(commoditiesBoughtFromProvider, seOid));
            });


            se.setCommoditiesBoughtRepoFromProviderDTOList(commoditiesBoughtRepoFromProviderDTOList);

            // Only set the valid provider list
            se.setProviders(commoditiesBoughtRepoFromProviderDTOList.stream().filter(
                commoditiesBoughtRepoFromProviderDTO -> commoditiesBoughtRepoFromProviderDTO.getProviderId() != null)
                    .map(grouping -> String.valueOf(grouping.getProviderId()))
                    .collect(Collectors.toList()));

            // Commodities sold list
            se.setCommoditySoldList(t.getCommoditySoldListList().stream().map(comm ->
                    CommodityMapper.convert(seOid, seOid, comm)).collect(Collectors.toList()));

            return se;
        }

        static String mapEntityType(int type) {
            return RepoObjectType.mapEntityType(type);
        }

        /**
         * Maps the entity state from the one used in TopologyDTO to
         * the one expected in UI.
         * TODO: This is a temporary fix, see OM-11305.
         *
         * @param topologyEntityState
         * @return A string of entity state that UI expects
         */
        static String mapEntityState(EntityState topologyEntityState) {
            return RepoObjectState.mapEntityType(topologyEntityState);
        }
    }


    static class CommodityMapper {
        private static CommodityBoughtRepoDTO convert(
                String ownerOid,
                String providerOid,
                CommodityBoughtDTO comm) {
            CommodityBoughtRepoDTO commRepo = new CommodityBoughtRepoDTO();

            commRepo.setUuid(UUID.randomUUID().toString());
            commRepo.setProviderOid(providerOid);
            commRepo.setOwnerOid(ownerOid);
            commRepo.setType(mapCommodityType(comm.getCommodityType().getType()));

            commRepo.setKey(comm.getCommodityType().getKey());
            commRepo.setUsed(comm.getUsed());
            commRepo.setPeak(comm.getPeak());

            return commRepo;
        }

        private static CommoditySoldRepoDTO convert(
                String ownerOid,
                String providerOid,
                CommoditySoldDTO comm) {
            CommoditySoldRepoDTO commRepo = new CommoditySoldRepoDTO();

            commRepo.setUuid(UUID.randomUUID().toString());
            commRepo.setProviderOid(providerOid);
            commRepo.setOwnerOid(ownerOid);
            commRepo.setType(mapCommodityType(comm.getCommodityType().getType()));

            commRepo.setKey(comm.getCommodityType().getKey());
            commRepo.setUsed(comm.getUsed());
            commRepo.setPeak(comm.getPeak());

            commRepo.setCapacity(comm.getCapacity());
            commRepo.setEffectiveCapacityPercentage(comm.getEffectiveCapacityPercentage());
            commRepo.setReservedCapacity(comm.getReservedCapacity());
            commRepo.setResizeable(comm.getIsResizeable());
            commRepo.setThin(comm.getIsThin());
            return commRepo;
        }

        private static String mapCommodityType(int type) {
            return RepoObjectType.mapCommodityType(type);
        }

        private static CommoditiesBoughtRepoFromProviderDTO convert(
            CommoditiesBoughtFromProvider commoditiesBoughtFromProvider, String seOid) {
            final String provId = commoditiesBoughtFromProvider.hasProviderId() ?
                Long.toString(commoditiesBoughtFromProvider.getProviderId())
                : null;
            CommoditiesBoughtRepoFromProviderDTO commoditiesBoughtRepoFromProviderDTO =
                new CommoditiesBoughtRepoFromProviderDTO();
            commoditiesBoughtRepoFromProviderDTO.setCommodityBoughtRepoDTOs(
                commoditiesBoughtFromProvider.getCommodityBoughtList().stream()
                    .map(comm -> CommodityMapper.convert(seOid, provId, comm))
                    .collect(Collectors.toList()));
            commoditiesBoughtRepoFromProviderDTO.setProviderId(commoditiesBoughtFromProvider.hasProviderId() ?
                commoditiesBoughtFromProvider.getProviderId() : null);
            commoditiesBoughtRepoFromProviderDTO.setProviderEntityType(commoditiesBoughtFromProvider.hasProviderEntityType() ?
                commoditiesBoughtFromProvider.getProviderEntityType() : null);
            return commoditiesBoughtRepoFromProviderDTO;
        }
    }
}
