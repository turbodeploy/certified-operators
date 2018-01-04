package com.vmturbo.repository.controller;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Objects;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import javax.annotation.Nonnull;

import org.apache.commons.lang3.StringUtils;
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

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

import com.vmturbo.repository.RepositoryDiagnosticsHandler;

@Api("RepositoryDiagnosticController")
@RestController
@RequestMapping(value = "/internal-state")
public class RepositoryDiagnosticController {

    private final Logger logger = LogManager.getLogger();

    private final RepositoryDiagnosticsHandler diagnosticsHandler;

    public RepositoryDiagnosticController(@Nonnull final RepositoryDiagnosticsHandler diagnosticsHandler) {
        this.diagnosticsHandler = Objects.requireNonNull(diagnosticsHandler);
    }

    /**
     * Get the internal state of the repository component.
     * Zip it and add it to the diagnostics of the server.
     *
     * @param responseOutput the stream where the diagnostics are written to
     * @throws IOException when there is a problem with the stream
     */
    @RequestMapping(method = RequestMethod.GET,
            produces = {"application/zip"})
    @ResponseBody
    @ApiOperation(value = "Get the Repository internal state.")
    public void getInternalState(OutputStream responseOutput) throws IOException {
        final ZipOutputStream zipOutputStream = new ZipOutputStream(responseOutput);
        List<String> errors = diagnosticsHandler.dump(zipOutputStream);
        zipOutputStream.close();
        if (!errors.isEmpty()) {
            logger.error("Encountered errors while trying to dump internal state: "
                    + StringUtils.join(errors, "\n"));
        }
    }

    /**
     * Restore the internal state of the repository component.
     *
     * @param inputStream The input stream that contains the zipped state of the component.
     * @return A {@link ResponseEntity} with a string that reports the results of the operation
     * @throws IOException when there is a problem with the stream
     */
    @RequestMapping(method = RequestMethod.POST,
            consumes = {"application/zip"},
            produces = {MediaType.APPLICATION_JSON_UTF8_VALUE})
    @ApiOperation(
            value = "Restore the repository state, in particular the realtime topology.",
            notes = "Use the following CURL command to restore from the file /tmp/diags/diags.zip:<br>"
                    + "curl --header 'Content-Type: application/zip' --data-binary @/tmp/diags/diags.zip 'http://address:9000/internal-state'"
    )
    @ApiResponses(value = {
            @ApiResponse(code = 400,
                    message = "If the input is missing, invalid, or can't be accessed.",
                    response = String.class)
    })
    public ResponseEntity<String> restoreState(
            @RequestBody final ByteArrayResource inputStream) {
        try {
            final List<String> errors = diagnosticsHandler.restore(inputStream.getInputStream());
            if (errors.isEmpty()) {
                return new ResponseEntity<>("Success\n", HttpStatus.OK);
            } else {
                final String errorMsg = "Restoration of state failed with errors:\n" +
                        StringUtils.join(errors, "\n");
                logger.error(errorMsg);
                return new ResponseEntity<>(errorMsg, HttpStatus.BAD_REQUEST);
            }
        } catch (IOException e) {
            String errorMsg = "Failed to initialize ZipInputStream";
            logger.error(errorMsg, e);
            return new ResponseEntity<>(errorMsg, HttpStatus.BAD_REQUEST);
        }
    }
}
