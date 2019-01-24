package com.vmturbo.repository.topology;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.repository.constant.RepoObjectType;
import com.vmturbo.repository.graph.executor.ArangoDBExecutor;
import com.vmturbo.repository.service.SupplyChainRpcService;

/**
 * Used to compute a multimap where the keys are SE types and the values associated with
 * a key are the SE types that are providers of the key SE type.
 * An example of a possible result is:
 * {
 *    PhysicalMachine=[DataCenter],
 *    Storage=[DiskArray],
 *    Application=[VirtualMachine],
 *    VirtualMachine=[PhysicalMachine, Storage]
 *  }
 *
 */
public class IncrementalTopologyRelationshipRecorder {
    private static final Logger logger = LogManager.getLogger();
    /**
     *  The supply chain of SE numerical types.
     */
    final Multimap<Integer, Integer> providerRels = HashMultimap.create();
    /**
     * Map from SE oid to its numerical type.
     */
    final Map<Long, Integer> idTypes = new HashMap<>();
    /**
     * Map from oid of an SE which type is not yet known to the SE numerical
     * types that it is a provider of. Used for handling forward references,
     * when the provider DTO was not processed yet.
     */
    final Multimap<Long, Integer> unknownProvidersMap = HashMultimap.create();

    /**
     * Handle a partial collection of DTOs.
     * @param chunk a collection of DTOs that is only part of the whole topology.
     */
    public void processChunk(Collection<TopologyEntityDTO> chunk) {
        idTypes.putAll(chunk.stream().collect(
                Collectors.toMap(TopologyEntityDTO::getOid, TopologyEntityDTO::getEntityType)));

        for (TopologyEntityDTO dto : chunk) {
            long oid = dto.getOid();
            int seType = dto.getEntityType();
            Collection<Integer> consumersTypes = unknownProvidersMap.get(oid);
            if (!consumersTypes.isEmpty()) {
                // the DTO is a provider but only now we found its type
                for (Integer consumerType : consumersTypes) {
                    providerRels.put(consumerType, seType);
                }
                unknownProvidersMap.removeAll(oid);
            }

            for (CommoditiesBoughtFromProvider provider : dto.getCommoditiesBoughtFromProvidersList()) {
                if (provider.hasProviderId()) {
                    Integer providerType = idTypes.get(provider.getProviderId());
                    if (providerType != null) {
                        providerRels.put(seType, providerType);
                    }
                    else {
                        unknownProvidersMap.put(provider.getProviderId(), seType);
                    }
                }
            }

            // add connectedTo entity types to providers since we want to show a mix of consumes
            // and connected relationship in the global supply chain
            // Note: currently we don't want to show some cloud entity types, so we skip them
            if (!SupplyChainRpcService.IGNORED_ENTITY_TYPES_FOR_GLOBAL_SUPPLY_CHAIN.contains(seType)) {
                dto.getConnectedEntityListList().forEach(connectedEntity -> {
                    Integer providerType = connectedEntity.getConnectedEntityType();
                    if (!SupplyChainRpcService.IGNORED_ENTITY_TYPES_FOR_GLOBAL_SUPPLY_CHAIN.contains(providerType)) {
                        providerRels.put(seType, providerType);
                    }
                });
            }
        }
    }

    /**
     * Generate the the supply chain as a Map<String, String>.
     * @return the supply chain
     */
    public Multimap<String, String> supplyChain() {
        Multimap<String, String> tempGlobalSupplyChainProviderRels = HashMultimap.create();
        providerRels.asMap().forEach((seType, provTypes) -> {
            Set<String> repoProvTypes = provTypes.stream().map(RepoObjectType::mapEntityType)
                            .collect(Collectors.toSet());
            tempGlobalSupplyChainProviderRels.putAll(RepoObjectType.mapEntityType(seType),
                                                 repoProvTypes);
        });
        logger.debug("Supply chain : {}", tempGlobalSupplyChainProviderRels);
        return tempGlobalSupplyChainProviderRels;
    }
}
