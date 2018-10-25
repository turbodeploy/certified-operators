package com.vmturbo.cost.component.topology;

import java.io.IOException;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;

import com.vmturbo.common.protobuf.cost.CostDebug;
import com.vmturbo.common.protobuf.cost.CostDebugREST.GetRecordedCostsRequest;
import com.vmturbo.cost.component.topology.CostJournalRecorder;

@Api(value = "/costJournal")
@RequestMapping(value = "/costJournal")
@RestController
public class CostJournalRecorderController {

    private static final Logger logger = LogManager.getLogger();

    private final CostJournalRecorder costJournalRecorder;

    public CostJournalRecorderController(@Nonnull final CostJournalRecorder costJournalRecorder) {
        this.costJournalRecorder = Objects.requireNonNull(costJournalRecorder);
    }

    @RequestMapping(method = RequestMethod.POST,
            consumes = {MediaType.APPLICATION_JSON_UTF8_VALUE},
            produces = {"text/plain"})
    @ApiOperation(value = "Get a human-readable text representation of the recorded cost journals. " +
            "The text journal is streamed to prevent massive memory consumption in constructing " +
            "or receiving the journal. Note that there is a separate gRPC interface to stream a more " +
            "machine-parsable representation of the journal. This call will not work from the swagger-ui. " +
            "Instead you can form your query in the swagger-ui and copy the curl command into the command line.")
    public ResponseEntity<StreamingResponseBody> streamTextJournal(
            @ApiParam(value = "Specification of how to filter the information.", required = true)
            @RequestBody final GetRecordedCostsRequest request) {
        final Set<Long> targetEntities = new HashSet<>();
        final CostDebug.GetRecordedCostsRequest requestProto = request.toProto();
        if (requestProto.hasEntityId()) {
            targetEntities.add(requestProto.getEntityId());
        }

        // Use a streaming response body to allow the response to be streamed.
        // This prevents a massive buildup in memory of the log as stitching occurs.
        // It also allows the caller not to have to receive the entire log at once.
        final StreamingResponseBody body = outputStream -> {
            costJournalRecorder.getJournalDescriptions(targetEntities)
                .forEach(cost -> {
                    try {
                        outputStream.write(cost.getBytes());
                    } catch (IOException e) {
                        logger.error("Unable to write to output stream due to error: {}",
                                e.getLocalizedMessage());
                    }
                });
        };

        return new ResponseEntity<>(body, HttpStatus.OK);
    }
}
