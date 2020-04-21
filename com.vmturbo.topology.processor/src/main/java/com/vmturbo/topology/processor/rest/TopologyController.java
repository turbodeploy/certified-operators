package com.vmturbo.topology.processor.rest;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiModelProperty;
import io.swagger.annotations.ApiOperation;

import org.apache.commons.io.FileUtils;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.vmturbo.topology.processor.entity.EntityStore;
import com.vmturbo.topology.processor.entity.IdentifiedEntityDTO;
import com.vmturbo.topology.processor.scheduling.Scheduler;
import com.vmturbo.topology.processor.stitching.journal.StitchingJournalFactory;
import com.vmturbo.topology.processor.topology.TopologyBroadcastInfo;
import com.vmturbo.topology.processor.topology.TopologyHandler;
import com.vmturbo.topology.processor.topology.pipeline.TopologyPipeline.TopologyPipelineException;

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

    public TopologyController(@Nonnull final Scheduler scheduler,
                              @Nonnull final TopologyHandler topologyHandler,
                              @Nonnull final EntityStore entityStore) {
        this.scheduler = Objects.requireNonNull(scheduler);
        this.topologyHandler = Objects.requireNonNull(topologyHandler);
        this.entityStore = Objects.requireNonNull(entityStore);
    }

    /**
     * Triggers converting the probe DTOs from all registered targets to topology DTOs, and
     * sending them to subscribed listeners.
     *
     * @return The response entity containing the response.
     * @throws TopologyPipelineException If there is an issue broadcasting the topology.
     * @throws InterruptedException If the broadcast is interrupted.
     */
    @RequestMapping(value = "/send",
            method = RequestMethod.POST,
            produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
    @ApiOperation(value = "Send topology to subscribed listeners.",
    notes = "Triggers converting the probe DTOs from all "
            + "registered targets to topology DTOs, and "
            + "sending them to subscribed listeners."
    )
    public ResponseEntity<SendTopologyResponse> send()
            throws TopologyPipelineException, InterruptedException {
        scheduler.resetBroadcastSchedule();
        final TopologyBroadcastInfo broadcastInfo = topologyHandler
            .broadcastLatestTopology(StitchingJournalFactory.emptyStitchingJournalFactory());
        return new ResponseEntity<>(
                new SendTopologyResponse("Sent " + broadcastInfo.getEntityCount() + " entities",
                    broadcastInfo.getEntityCount(),
                    broadcastInfo.getTopologyId(),
                    broadcastInfo.getTopologyContextId(),
                    broadcastInfo.getSerializedTopologySizeBytes()),
                HttpStatus.OK);
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

        @ApiModelProperty("The size in bytes of the topology that was broadcast. " +
            "This is the serialized size of the topology, not in-memory size. Only counts topology data" +
            "messages, not start and end message sizes.")
        public long serializedTopologySizeBytes;

        @ApiModelProperty("The size of the topology that was broadcast. The serializedTopologySizeBytes" +
            " converted to a format that is easier for a human being to consume.")
        public String serializedTopologySizeHumanReadable;

        @ApiModelProperty(value = "The ID of the topology context for which this broadcast occurred.",
            notes = "Used to differentiate, for example, between real and plan contexts.")
        public long topologyContextId;

        private SendTopologyResponse(final String message,
                                     final long numberOfEntities,
                                     final long topologyId,
                                     final long topologyContextId,
                                     final long serializedTopologySizeBytes) {
            this.message = message;
            this.numberOfEntities = numberOfEntities;
            this.topologyId = topologyId;
            this.topologyContextId = topologyContextId;
            this.serializedTopologySizeBytes = serializedTopologySizeBytes;
            this.serializedTopologySizeHumanReadable =
                FileUtils.byteCountToDisplaySize(serializedTopologySizeBytes);
        }
    }

}
