package com.vmturbo.plan.orchestrator.diagnostics;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.zip.ZipOutputStream;
import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.tools.StringUtils;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

/**
 * The REST endpoints to get and restore the internal state of the plan orchestrator component
 */
@RestController
@RequestMapping("/internal-state")
public class PlanOrchestratorDiagnosticsController {

    private final Logger logger = LogManager.getLogger();

    private final PlanOrchestratorDiagnosticsHandler diagnosticsHandler;

    public PlanOrchestratorDiagnosticsController(
        @Nonnull final PlanOrchestratorDiagnosticsHandler handler) {
        this.diagnosticsHandler = Objects.requireNonNull(handler);
    }

    @RequestMapping(method = RequestMethod.GET, produces = {"application/zip"})
    @ApiOperation(value = "Get the internal state.",
        notes = "Returns a zip file formatted as a base64-encoded string.")
    public ResponseEntity<ByteArrayResource> getInternalState() throws IOException {
        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

        try (final ZipOutputStream zipOutputStream = new ZipOutputStream(byteArrayOutputStream)) {
            diagnosticsHandler.dump(zipOutputStream);
        }

        return new ResponseEntity<>(new ByteArrayResource(byteArrayOutputStream.toByteArray()),
            HttpStatus.OK);
    }

    @RequestMapping(method = RequestMethod.POST, consumes = {"application/zip"},
        produces = {MediaType.APPLICATION_JSON_UTF8_VALUE})
    @ApiOperation(value = "Restore the internal state.", notes = "Consumes a zip file " +
        "formatted as a base-64 encoded string (equivalent to the output of GET).")
    @ApiResponses(value = {
        @ApiResponse(code = 400,
            message = "If the input is not a zip file, or does not contain the expected contents.",
            response = String.class)
    })
    public ResponseEntity<String> restoreInternalState(
        @RequestBody final ByteArrayResource inputStream) throws IOException {
        try {
            final List<String> errors = diagnosticsHandler.restore(inputStream.getInputStream());
            if (errors.isEmpty()) {
                return new ResponseEntity<>("Success\n", HttpStatus.OK);
            } else {
                final String errorMessage = "Errors occurred during restoration of state:\n" +
                    StringUtils.join(errors, "\n");
                logger.error(errorMessage);
                return new ResponseEntity<>(errorMessage, HttpStatus.BAD_REQUEST);
            }
        } catch (IOException e) {
            logger.error("Failed to initialize ZipInputStream");
            return new ResponseEntity<>("Failed to initialize ZipInputStream",
                HttpStatus.BAD_REQUEST);
        }
    }
}
