package com.vmturbo.components.common.diagnostics;

import java.io.IOException;
import java.util.Objects;

import javax.annotation.Nonnull;

import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

import org.apache.commons.lang3.StringUtils;
import org.springframework.core.io.InputStreamResource;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

/**
 * The REST endpoints to get and restore the internal state
 * of the component.
 */
public class DiagnosticsControllerImportable extends DiagnosticsController {

    private IDiagnosticsHandlerImportable importableHandler;

    /**
     * Constructs importable diagnostics controller.
     *
     * @param importableHandler diagnostics handler to operate with
     */
    public DiagnosticsControllerImportable(
            @Nonnull IDiagnosticsHandlerImportable importableHandler) {
        super(importableHandler);
        this.importableHandler = Objects.requireNonNull(importableHandler);
    }

    /**
     * Restores the internal state.
     *
     * @param inputStream input stream containing an internal ZIPed data of diagnostics.
     * @return message with restore errors, if any.
     * @throws IOException on IO exceptions while reading the stream
     */
    @RequestMapping(method = RequestMethod.POST, consumes = {"application/zip"},
            produces = {MediaType.APPLICATION_JSON_UTF8_VALUE})
    @ApiOperation(value = "Restore the internal state.",
            notes = "Use the following CURL command to restore from the file /tmp/diags/diags.zip:<br>"
                    + "curl --header 'Content-Type: application/zip' --data-binary @/tmp/diags/diags.zip 'http://address:8085/internal-state'")
    @ApiResponses(value = {@ApiResponse(code = 400,
            message = "If the input is not a zip file, or does not contain the expected contents.",
            response = String.class)})
    public ResponseEntity<String> restoreInternalState(
            @RequestBody final InputStreamResource inputStream) throws IOException {
        try {
            getLogger().info("Restoring component's state from diagnostics...");
            try {
                final String message = importableHandler.restore(inputStream.getInputStream());
                getLogger().info("Restoring component's state finished: " + message);
                return new ResponseEntity<>(message, HttpStatus.OK);
            } catch (DiagnosticsException e) {
                getLogger().error("Restoring component's state failed with errors", e);
                final String errorMsg =
                        "Restoration of state failed with errors:\n"
                                + StringUtils.join(e.getErrors(), "\n");
                return new ResponseEntity<>(errorMsg, HttpStatus.BAD_REQUEST);
            }
        } catch (IOException e) {
            getLogger().error("Failed to initialize ZipInputStream");
            return new ResponseEntity<>("Failed to initialize ZipInputStream",
                    HttpStatus.BAD_REQUEST);
        }
    }
}
