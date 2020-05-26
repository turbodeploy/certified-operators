package com.vmturbo.extractor.topology;

import static com.vmturbo.extractor.topology.ITopologyWriter.getFinishPhaseLabel;
import static com.vmturbo.extractor.topology.ITopologyWriter.getIngestionPhaseLabel;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Streams;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;

import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupFilter;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode.MemberList;
import com.vmturbo.common.protobuf.topology.TopologyDTO.Topology.DataSegment;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTOUtil;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.communication.chunking.RemoteIterator;
import com.vmturbo.components.api.client.RemoteIteratorDrain;
import com.vmturbo.components.common.utils.MultiStageTimer;
import com.vmturbo.components.common.utils.MultiStageTimer.AsyncTimer;
import com.vmturbo.components.common.utils.MultiStageTimer.Detail;
import com.vmturbo.extractor.topology.ITopologyWriter.InterruptibleConsumer;
import com.vmturbo.extractor.topology.SupplyChainEntity.Builder;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.graph.TopologyGraphCreator;
import com.vmturbo.topology.graph.supplychain.SupplyChainCalculator;
import com.vmturbo.topology.graph.supplychain.TraversalRulesLibrary;
import com.vmturbo.topology.processor.api.EntitiesListener;

/**
 * Handler for entity information coming in from the Topology Processor. Listens only to the live
 * topology.
 */
public class TopologyEntitiesListener implements EntitiesListener {
    private final Logger logger = LogManager.getLogger();

    private final List<Supplier<? extends ITopologyWriter>> writerFactories;
    private final GroupServiceBlockingStub groupService;
    private final WriterConfig config;
    private final AtomicBoolean busy = new AtomicBoolean(false);
    private static final Set<GroupType> GROUP_TYPE_BLACKLIST = ImmutableSet.of(GroupType.RESOURCE, GroupType.BILLING_FAMILY);

    /**
     * Create a new instance.
     *
     * @param groupService    group service endpoint
     * @param writerFactories factories to create required writers
     * @param writerConfig    common config parameters for writers
     */
    public TopologyEntitiesListener(@Nonnull final GroupServiceBlockingStub groupService,
            @Nonnull final List<Supplier<? extends ITopologyWriter>> writerFactories,
            @Nonnull final WriterConfig writerConfig) {
        this.groupService = groupService;
        this.writerFactories = writerFactories;
        this.config = writerConfig;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onTopologyNotification(@Nonnull final TopologyInfo topologyInfo,
            @Nonnull final RemoteIterator<DataSegment> entityIterator) {
        String label = TopologyDTOUtil.getSourceTopologyLabel(topologyInfo);
        if (busy.compareAndSet(false, true)) {
            logger.info("Received topology {}", TopologyDTOUtil.getSourceTopologyLabel(topologyInfo));
            try {
                new TopologyRunner(topologyInfo, entityIterator).runTopology();
            } catch (SQLException | UnsupportedDialectException e) {
                logger.error("Failed to process topology", e);
            } finally {
                RemoteIteratorDrain.drainIterator(entityIterator, label, false);
                busy.set(false);
            }
        } else {
            RemoteIteratorDrain.drainIterator(
                    entityIterator, TopologyDTOUtil.getSourceTopologyLabel(topologyInfo), true);
        }
    }

    /**
     * Class instantiated for each topology, responsible for processing the topology.
     */
    private class TopologyRunner {

        private final MultiStageTimer timer = new MultiStageTimer(logger);

        private TopologyGraphCreator<Builder, SupplyChainEntity> graphBuilder
                = new TopologyGraphCreator<>();
        private Long2ObjectMap<List<Grouping>> entityToGroupInfo = getGroupMemberships();

        private final TopologyInfo topologyInfo;
        private final RemoteIterator<DataSegment> entityIterator;

        TopologyRunner(final TopologyInfo topologyInfo, final RemoteIterator<DataSegment> entityIterator) {
            this.topologyInfo = topologyInfo;
            this.entityIterator = entityIterator;
        }

        private void runTopology() throws UnsupportedDialectException, SQLException {
            final AsyncTimer elapsedTimer = timer.async("Total Elapsed");
            timer.stop();
            long successCount = 0;
            try {
                successCount = writeTopology();
            } catch (IOException | InterruptedException e) {
                logger.error("Interrupted while writing topology", e);
                Thread.currentThread().interrupt();
            }
            elapsedTimer.close();
            timer.info(String.format("Processed %s entities", successCount), Detail.STAGE_SUMMARY);
        }

        private long writeTopology() throws IOException, UnsupportedDialectException, SQLException, InterruptedException {
            // fetch cluster membership
            List<ITopologyWriter> writers = new ArrayList<>();
            writerFactories.stream()
                    .map(Supplier::get)
                    .forEach(writers::add);

            final long topologyId = topologyInfo.getTopologyId();
            final long topologyContextId = topologyInfo.getTopologyContextId();

            final List<Pair<InterruptibleConsumer<TopologyEntityDTO>, ITopologyWriter>> entityConsumers = new ArrayList<>();
            for (ITopologyWriter writer : writers) {
                InterruptibleConsumer<TopologyEntityDTO> entityConsumer =
                        writer.startTopology(topologyInfo, entityToGroupInfo, config, timer);
                entityConsumers.add(Pair.of(entityConsumer, writer));
            }
            long entityCount = 0L;
            try {
                while (entityIterator.hasNext()) {
                    timer.start("Chunk Retrieval");
                    final Collection<DataSegment> chunk = entityIterator.nextChunk();
                    timer.stop();
                    entityCount += chunk.size();
                    addChunkToGraph(chunk);
                    for (Pair<InterruptibleConsumer<TopologyEntityDTO>, ITopologyWriter> consumer : entityConsumers) {
                        timer.start(getIngestionPhaseLabel(consumer.getRight()));
                        for (final DataSegment dataSegment : chunk) {
                            if (dataSegment.hasEntity()) {
                                consumer.getLeft().accept(dataSegment.getEntity());
                            }
                        }
                    }
                }
                timer.start("Compute Supply Chain");
                final Map<Long, Set<Long>> entityToRelated = computeRleatedentities();
                timer.stop();
                for (final ITopologyWriter writer : writers) {
                    timer.start(getFinishPhaseLabel(writer));
                    writer.finish(entityToRelated);
                    timer.stop();
                }
            } catch (CommunicationException | TimeoutException e) {
                logger.error("Error occurred while receiving topology " + topologyId
                        + " for context " + topologyContextId, e);
            } catch (InterruptedException e) {
                logger.error("Thread interrupted receiving topology " + topologyId
                        + " for context " + topologyContextId, e);
            } catch (RuntimeException e) {
                logger.error("Unexpected error occurred while receiving topology " + topologyId
                        + " for context " + topologyContextId, e);
                throw e; // Re-throw the exception to abort reading the topology.
            }
            return entityCount;
        }

        private Map<Long, Set<Long>> computeRleatedentities() {
            Map<Long, Set<Long>> entityToRelated = new Long2ObjectOpenHashMap<>();
            final TopologyGraph<SupplyChainEntity> graph = graphBuilder.build();
            graphBuilder = null;
            final Map<Long, Set<Long>> syncEntityToRelated = Collections.synchronizedMap(entityToRelated);
            SupplyChainCalculator calc = new SupplyChainCalculator();
            final TraversalRulesLibrary<SupplyChainEntity> ruleChain = new TraversalRulesLibrary<>();
            graph.entities().parallel().forEach(e -> {
                final Map<Integer, SupplyChainNode> related = calc.getSupplyChainNodes(
                        graph, Collections.singletonList(e.getOid()), _e -> true, ruleChain);
                final long[] memberOids = related.values().stream()
                        .map(SupplyChainNode::getMembersByStateMap)
                        .flatMap(map -> map.values().stream())
                        .map(MemberList::getMemberOidsList)
                        .flatMap(Collection::stream)
                        .mapToLong(Long::longValue)
                        .toArray();
                // TODO maybe try to optimize hash set creation
                syncEntityToRelated.put(e.getOid(), new LongOpenHashSet(memberOids));
            });
            return entityToRelated;
        }

        private void addChunkToGraph(final Collection<DataSegment> chunk) {
            chunk.stream()
                    .filter(DataSegment::hasEntity)
                    .map(DataSegment::getEntity)
                    .map(SupplyChainEntity::newBuilder)
                    .forEach(graphBuilder::addEntity);
        }

        /**
         * Load group information from group component.
         *
         * @return map linking each entity id to the groups to which it belongs
         */
        private Long2ObjectMap<List<Grouping>> getGroupMemberships() {
            // TODO Maybe compute groups from our topology rather than asking group component to
            // do it. That way, groups will be in sync with topology, which is otherwise not
            // guaranteed.
            timer.start("Load group memberships");
            Long2ObjectMap<List<Grouping>> entityToGroup = new Long2ObjectOpenHashMap<>();
            Long2ObjectMap<Grouping> groupsById = new Long2ObjectOpenHashMap<>();
            // request all non-temporary, visible group definitions
            final Iterator<Grouping> groups = groupService.getGroups(GetGroupsRequest.newBuilder()
                    .setGroupFilter(GroupFilter.newBuilder()
                            .setIncludeTemporary(false)
                            .setIncludeHidden(false)
                            .build())
                    .build());
            // filter out the group types we don't care about, build map of all groups, as well
            // as a list of group ids.
            LongList groupIds = new LongArrayList(Streams.stream(groups)
                    .filter(g -> !GROUP_TYPE_BLACKLIST.contains(g.getDefinition().getType()))
                    .peek(g -> groupsById.put(g.getId(), g))
                    .mapToLong(Grouping::getId)
                    .toArray());

            // retrieve (flattened) group memberships for all the groups we care about
            if (!groupIds.isEmpty()) {
                final Iterator<GetMembersResponse> memberships = groupService.getMembers(
                        GetMembersRequest.newBuilder()
                                .addAllId(groupIds)
                                .setExpectPresent(true)
                                .setEnforceUserScope(false)
                                .setExpandNestedGroups(true)
                                .build());
                // and finally add each group's GroupInfo to each of its members
                memberships.forEachRemaining(ms -> ms.getMemberIdList().forEach(
                        member -> entityToGroup.computeIfAbsent((long)member, m -> new ArrayList<>())
                                .add(groupsById.get(ms.getGroupId()))));
            }
            timer.stop();
            logger.info("Fetched {} groups totaling {} members from group service",
                    groupIds.size(), entityToGroup.size());
            return entityToGroup;
        }
    }
}
