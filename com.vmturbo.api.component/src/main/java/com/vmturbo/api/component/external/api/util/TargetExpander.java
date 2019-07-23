package com.vmturbo.api.component.external.api.util;

import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.common.protobuf.search.SearchProtoUtil;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.topology.processor.api.TargetInfo;
import com.vmturbo.topology.processor.api.TopologyProcessor;
import com.vmturbo.topology.processor.api.TopologyProcessorException;

/**
 * Class to manage returning the entities discovered by a given target ID.
 */
public class TargetExpander {

    private static final Logger logger = LogManager.getLogger();

    private final TopologyProcessor topologyProcessorClient;
    private final RepositoryApi repositoryApi;

    public TargetExpander(@Nonnull final TopologyProcessor topologyProcessorClient,
                          @Nonnull final RepositoryApi repositoryApi) {
        this.topologyProcessorClient = topologyProcessorClient;
        this.repositoryApi = repositoryApi;
    }

    /**
     * Fetch the target info for this targetId.
     *
     * @param targetId the OID for the target to be fetched
     * @return the target info for this targetId, or Optional.empty() if the ID does not represent
     * a target
     * @throws CommunicationException if there is a problem communicating with the TopologyProcessor
     */
    public Optional<TargetInfo> getTarget(@Nonnull String targetId) throws CommunicationException {
        if (StringUtils.isNumeric(targetId)) {
            try {
                return Optional.of(topologyProcessorClient.getTarget(Long.parseLong(targetId)));
            } catch (TopologyProcessorException e) {
                logger.debug("No target {}", targetId);
            }
        }
        return Optional.empty();
    }

    /**
     * Return the OIDs of the ServiceEntities discovered by this target.
     *
     * @param targetInfo the info describing this target - in particular the OID
     * @return a set of OIDs of all ServiceEntities discovered by this target
     */
    public Set<Long> getTargetEntityIds(@Nonnull TargetInfo targetInfo) {
        return repositoryApi.newSearchRequest(
            SearchProtoUtil.makeSearchParameters(
                SearchProtoUtil.discoveredBy(targetInfo.getId()))
                .build())
            .getOids();
    }

}
