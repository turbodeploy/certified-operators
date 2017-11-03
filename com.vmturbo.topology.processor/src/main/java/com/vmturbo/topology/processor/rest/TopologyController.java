package com.vmturbo.topology.processor.rest;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiModelProperty;
import io.swagger.annotations.ApiOperation;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.topology.processor.entity.EntityStore;
import com.vmturbo.topology.processor.entity.IdentifiedEntityDTO;
import com.vmturbo.topology.processor.group.GroupResolver;
import com.vmturbo.topology.processor.group.filter.TopologyFilterFactory;
import com.vmturbo.topology.processor.group.policy.PolicyManager;
import com.vmturbo.topology.processor.scheduling.Scheduler;
import com.vmturbo.topology.processor.topology.TopologyGraph;
import com.vmturbo.topology.processor.topology.TopologyGraph.Vertex;
import com.vmturbo.topology.processor.topology.TopologyHandler;
import com.vmturbo.topology.processor.topology.TopologyHandler.TopologyBroadcastInfo;

/**
 * REST controller for topology processor management.
 */
@Api(value = "/topology")
@RequestMapping(value = "/topology")
@RestController
public class TopologyController {

    private final Scheduler scheduler;
    private final TopologyHandler topologyHandler;
    private final EntityStore entityStore;
    private final PolicyManager policyManager;

    public TopologyController(@Nonnull final Scheduler scheduler,
                              @Nonnull final TopologyHandler topologyHandler,
                              @Nonnull final EntityStore entityStore,
                              @Nonnull final PolicyManager policyManager) {
        this.scheduler = Objects.requireNonNull(scheduler);
        this.topologyHandler = Objects.requireNonNull(topologyHandler);
        this.entityStore = Objects.requireNonNull(entityStore);
        this.policyManager = Objects.requireNonNull(policyManager);
    }

    @RequestMapping(value = "/send",
            method = RequestMethod.POST,
            produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
    @ApiOperation(value = "Send topology to subscribed listeners.",
    notes = "Triggers converting the probe DTOs from all "
            + "registered targets to topology DTOs, and "
            + "sending them to subscribed listeners."
    )
    /**
     * Triggers converting the probe DTOs from all registered targets to topology DTOs, and
     * sending them to subscribed listeners.
     */ public ResponseEntity<SendTopologyResponse> send()
            throws CommunicationException, InterruptedException {
        scheduler.resetBroadcastSchedule();
        final TopologyBroadcastInfo broadcastInfo = topologyHandler.broadcastLatestTopology();
        return new ResponseEntity<>(
                new SendTopologyResponse("Sent " + broadcastInfo.getEntityCount() + " entities",
                    broadcastInfo.getEntityCount(),
                    broadcastInfo.getTopologyId(),
                    broadcastInfo.getTopologyContextId()),
                HttpStatus.OK);
    }

    @RequestMapping(method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
    @ApiOperation(value = "Retrieve the most recent topology.")
    public ResponseEntity<Collection<TopologyEntityDTO>> getTopology() {
        TopologyGraph graph = new TopologyGraph(entityStore.constructTopology());
        try {
            policyManager.applyPolicies(graph, new GroupResolver(new TopologyFilterFactory()));
            return new ResponseEntity<>(
                graph.vertices()
                    .map(Vertex::getTopologyEntityDtoBuilder)
                    .map(TopologyEntityDTO.Builder::build)
                    .collect(Collectors.toList()),
                HttpStatus.OK);
        } catch (RuntimeException e) {
            // TODO: We probably shouldn't continue to broadcast if we cannot successfully apply policy information.
            return new ResponseEntity<>(Collections.emptyList(), HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @RequestMapping(value = "/target/{targetId}",
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
    @ApiOperation(value = "Get the most recently retrieved DTO's from a target.")
    public ResponseEntity<List<IdentifiedEntityDTO>> getTargetTopology(@PathVariable("targetId") Long targetId) {
        return new ResponseEntity<>(
                entityStore.discoveredByTarget(targetId).entrySet().stream()
                    .map(entry -> new IdentifiedEntityDTO(entry.getKey(), entry.getValue()))
                    .collect(Collectors.toList()),
                HttpStatus.OK);
    }

    /**
     * Response to REST API topology requests.
     */
    public static class SendTopologyResponse {
        @ApiModelProperty("A description of the broadcast.")
        public String message;

        @ApiModelProperty("The number of entities broadcast.")
        public long numberOfEntities;

        @ApiModelProperty("The ID of the topology that was broadcast.")
        public long topologyId;

        @ApiModelProperty(value = "The ID of the topology context for which this broadcast occurred.",
            notes = "Used to differentiate, for example, between real and plan contexts.")
        public long topologyContextId;

        private SendTopologyResponse(final String message,
                                     final long numberOfEntities,
                                     final long topologyId,
                                     final long topologyContextId) {
            this.message = message;
            this.numberOfEntities = numberOfEntities;
            this.topologyId = topologyId;
            this.topologyContextId = topologyContextId;
        }
    }

}
