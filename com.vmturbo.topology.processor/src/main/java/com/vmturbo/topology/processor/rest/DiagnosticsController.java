package com.vmturbo.topology.processor.rest;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Objects;
import java.util.zip.ZipOutputStream;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

import com.vmturbo.topology.processor.diagnostics.TopologyProcessorDiagnosticsHandler;
import com.vmturbo.topology.processor.targets.InvalidTargetException;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetDeserializationException;

/**
 * Controller for the REST interface for topology processor diags.
 */
@RestController
@RequestMapping(value = "/internal-state")
public class DiagnosticsController {

    private final Logger logger = LogManager.getLogger();

    private final TopologyProcessorDiagnosticsHandler diagnosticsHandler;

    public DiagnosticsController(@Nonnull final TopologyProcessorDiagnosticsHandler diagnosticsHandler) {
        this.diagnosticsHandler = Objects.requireNonNull(diagnosticsHandler);
    }

    /**
     * Get the internal state (targets, entities) of the topology processor component.
     * Zip it and add it to the diagnostics of the server.
     *
     * @param responseOutput the stream where the diagnostics are written to
     * @throws IOException when there is a problem with the stream
     */
    @RequestMapping(method = RequestMethod.GET,
            produces = {"application/zip"})
    @ResponseBody
    @ApiOperation(value = "Get the Topology Processor internal state.")
    public void getInternalState(OutputStream responseOutput) throws IOException {
        final ZipOutputStream zipOutputStream = new ZipOutputStream(responseOutput);
        diagnosticsHandler.dumpDiags(zipOutputStream);
        zipOutputStream.close();
    }

    /**
     * Restore the internal state of the topology processor component.
     *
     * @param inputStream The input stream that contains the zipped state of the component.
     * @return A {@link ResponseEntity} with a string that reports the results of the operation
     * @throws IOException when there is a problem with the stream
     */
    @RequestMapping(method = RequestMethod.POST,
            consumes = {"application/zip"},
            produces = {MediaType.APPLICATION_JSON_UTF8_VALUE})
    @ApiOperation(
            value = "Restore the topology processor state - targets, schedules and entities.",
            notes = "Use the following CURL command to restore from the file /tmp/diags/diags.zip:<br>"
                    + "curl --header 'Content-Type: application/zip' --data-binary @/tmp/diags/diags.zip 'http://address:8085/internal-state'"
    )
    @ApiResponses(value = {
            @ApiResponse(code = 400,
               message = "If the input is missing or can't be accessed.",
               response = String.class),
            @ApiResponse(code = 422,
            message = "If the content of the file can't be parsed.",
            response = String.class)
    })
    public ResponseEntity<String> restoreState(
            @RequestBody final ByteArrayResource inputStream)
            throws IOException {
        try {
            List<Target> targets = diagnosticsHandler.restore(inputStream.getInputStream());
            String response = "Restored " + targets.size() + " targets";
            return new ResponseEntity<>(response, HttpStatus.OK);
        } catch (IOException e) {
            String errorMsg = "Failed to initialize ZipInputStream";
            logger.error(errorMsg, e);
            return new ResponseEntity<>(errorMsg, HttpStatus.BAD_REQUEST);
        }
    }
}
