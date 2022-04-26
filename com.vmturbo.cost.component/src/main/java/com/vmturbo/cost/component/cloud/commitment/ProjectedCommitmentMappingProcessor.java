package com.vmturbo.cost.component.cloud.commitment;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;

import com.vmturbo.cloud.common.topology.CloudTopology;
import com.vmturbo.cloud.common.topology.TopologyEntityCloudTopologyFactory;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentAmount;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentMapping;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.commons.Pair;
import com.vmturbo.repository.api.RepositoryClient;

/**
 * The processor waits for both projected cloud commitment mappings and projected topology, then call commitment writer to persist.
 */
public class ProjectedCommitmentMappingProcessor {
    HashMap<Long, Long> projectedTopologyIDToContextId;
    HashMap<Long, Pair<TopologyInfo, List<CloudCommitmentMapping>>> projectedMapping;
    TopologyCommitmentCoverageWriter.Factory commitmentCoverageWriterFactory;
    private final RepositoryClient repositoryClient;
    private final TopologyEntityCloudTopologyFactory cloudTopologyFactory;

    ProjectedCommitmentMappingProcessor(HashMap<Long, Long> projectedTopologyIDToContextId,
                                        HashMap<Long, Pair<TopologyInfo, List<CloudCommitmentMapping>>> projectedCloudCommitmentMapping,
                                        TopologyCommitmentCoverageWriter.Factory commitmentCoverageWriterFactory,
                                        final RepositoryClient repositoryClient,
                                        TopologyEntityCloudTopologyFactory cloudTopologyFactory) {
        this.projectedTopologyIDToContextId = projectedTopologyIDToContextId;
        this.projectedMapping = projectedCloudCommitmentMapping;
        this.commitmentCoverageWriterFactory = commitmentCoverageWriterFactory;
        this.repositoryClient = Objects.requireNonNull(repositoryClient);
        this.cloudTopologyFactory = Objects.requireNonNull(cloudTopologyFactory);
    }

    /**
     * Persist commitment when the projected topology is available, otherwise save the cloud commitment mapping list.
     * @param topologyId The topology id.
     * @param topologyId The topology info.
     * @param cloudCommitmentMappingList A list of cloud commitment mapping.
     */
    public void mappingsAvailable(@Nonnull final long topologyId,
                                  @Nonnull final TopologyInfo topologyInfo,
                                  @Nonnull final List<CloudCommitmentMapping> cloudCommitmentMappingList) {
        if (projectedTopologyIDToContextId.containsKey(topologyId)) {
            // call repository to get cloud topology
            final Stream<TopologyEntityDTO> topologyEntities = repositoryClient.retrieveTopologyEntities(
                                                                                        Collections.singletonList(topologyId),
                                                                                        projectedTopologyIDToContextId.get(topologyId));
            CloudTopology<TopologyEntityDTO> cloudTopology = cloudTopologyFactory.newCloudTopology(topologyEntities);

            // call writer to persist commitment
            commitmentCoverageWriterFactory.newWriter(cloudTopology)
                    .persistCommitmentAllocations(topologyInfo, convertCloudCommitmentMappingListToTable(cloudCommitmentMappingList));
        } else {
            projectedMapping.put(topologyId, new Pair<>(topologyInfo, cloudCommitmentMappingList));
        }
    }

    /**
     * Persist commitment when the cloud commitment mapping is available, otherwise save the projected topology.
     * @param projectedTopologyId The topology id.
     * @param topologyContextId The topology context id.
     */
    public void projectedAvailable(@Nonnull final long projectedTopologyId,
                                   @Nonnull final long topologyContextId,
                                   @Nonnull final CloudTopology<TopologyEntityDTO> cloudTopology) {
        if (projectedMapping.containsKey(projectedTopologyId)) {
            TopologyInfo topologyInfo = projectedMapping.get(projectedTopologyId).first;
            List<CloudCommitmentMapping> cloudCommitmentMappingList = projectedMapping.get(projectedTopologyId).second;

            // call writer to persist commitment
            commitmentCoverageWriterFactory.newWriter(cloudTopology)
                    .persistCommitmentAllocations(topologyInfo, convertCloudCommitmentMappingListToTable(cloudCommitmentMappingList));
            projectedMapping.remove(projectedTopologyId);
        } else {
            projectedTopologyIDToContextId.put(projectedTopologyId, topologyContextId);
        }
    }

    private Table<Long, Long, CloudCommitmentAmount> convertCloudCommitmentMappingListToTable(@Nonnull final List<CloudCommitmentMapping> cloudCommitmentMappingList) {
        Table<Long, Long, CloudCommitmentAmount> commitmentAllocations = HashBasedTable.create();
        cloudCommitmentMappingList.stream()
                .map(cloudCommitmentMapping ->
                        commitmentAllocations.put(cloudCommitmentMapping.getEntityOid(),
                                cloudCommitmentMapping.getCloudCommitmentOid(),
                                cloudCommitmentMapping.getCommitmentAmount()))
                .collect(Collectors.toList());
        return commitmentAllocations;
    }
}
