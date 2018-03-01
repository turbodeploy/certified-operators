package com.vmturbo.action.orchestrator.diagnostics;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Objects;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import io.swagger.annotations.ApiModelProperty;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

import com.vmturbo.components.common.InvalidRestoreInputException;

/**
 * The REST endpoints to get and restore the internal state
 * of the action orchestrator.
 */
@RestController
@RequestMapping("/internal-state")
public class DiagnosticsController {

    private final Logger logger = LogManager.getLogger();

    private final ActionOrchestratorDiagnostics diagnostics;

    public DiagnosticsController(@Nonnull final ActionOrchestratorDiagnostics diagnostics) {
        this.diagnostics = Objects.requireNonNull(diagnostics);
    }

    @RequestMapping(method = RequestMethod.GET,
            produces = {"application/zip"})
    @ApiOperation(value = "Get the internal state.",
            notes = "Returns a zip file formatted as a base64-encoded string.")
    public ResponseEntity<ByteArrayResource> getInternalState() throws IOException {
        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        final ZipOutputStream zipOutputStream = new ZipOutputStream(byteArrayOutputStream);
        diagnostics.dump(zipOutputStream);
        zipOutputStream.close();
        return new ResponseEntity<>(new ByteArrayResource(byteArrayOutputStream.toByteArray()),
                HttpStatus.OK);
    }

    @RequestMapping(method = RequestMethod.POST,
            consumes = {"application/zip"},
            produces = {MediaType.APPLICATION_JSON_UTF8_VALUE})
    @ApiOperation(value = "Restore the internal state.",
            notes = "Consumes a zip file formatted as a base64-encoded string (equivalent to the output of GET.")
    @ApiResponses(value = {
            @ApiResponse(code = 400,
                    message = "If the input is not a zip file, or does not contain the expected contents.",
                    response = RestoreResponse.class)
    })
    public ResponseEntity<RestoreResponse> restoreInternalState(
            @RequestBody final ByteArrayResource inputStream)
            throws IOException {
        try {
            final ZipInputStream zipInputStream = new ZipInputStream(inputStream.getInputStream());
            diagnostics.restore(zipInputStream);
            return new ResponseEntity<>(RestoreResponse.success(), HttpStatus.OK);
        } catch (InvalidRestoreInputException e) {
            logger.error("Failed to restore due to bad input: " + e.getMessage(), e);
            return new ResponseEntity<>(RestoreResponse.error(e.getMessage()), HttpStatus.BAD_REQUEST);
        } catch (IOException e) {
            logger.error("Failed to initialize ZipInputStream");
            return new ResponseEntity<>(RestoreResponse.error("Failed to read from the input stream."),
                    HttpStatus.BAD_REQUEST);
        }
    }

    /**
     * The JSON object returned by a POST to restore the internal state.
     */
    public static class RestoreResponse {
        @ApiModelProperty("If present, the error encountered when attempting to restore the internal state.")
        public final String error;

        private RestoreResponse(@Nullable final String error) {
            this.error = error;
        }

        public static RestoreResponse success() {
            return new RestoreResponse(null);
        }

        public static RestoreResponse error(@Nonnull final String error) {
            return new RestoreResponse(Objects.requireNonNull(error));
        }
    }

}
