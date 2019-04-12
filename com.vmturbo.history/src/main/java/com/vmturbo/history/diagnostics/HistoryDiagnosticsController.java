package com.vmturbo.history.diagnostics;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Objects;
import java.util.zip.ZipOutputStream;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import io.swagger.annotations.ApiOperation;

/**
 * The REST endpoints to get the internal state of the history component.
 */
@RestController
@RequestMapping("/internal-state")
public class HistoryDiagnosticsController {

    private final Logger logger = LogManager.getLogger();

    private final HistoryDiagnostics diagnostics;

    public HistoryDiagnosticsController(@Nonnull final HistoryDiagnostics diagnostics) {
        this.diagnostics = Objects.requireNonNull(diagnostics);
    }

    @RequestMapping(method = RequestMethod.GET,
            produces = {"application/zip"})
    @ApiOperation(value = "Get the internal state.",
            notes = "Returns a zip file formatted as a base64-encoded string.")
    public ResponseEntity<ByteArrayResource> getInternalState() throws IOException {
        logger.info("Getting internal state...");
        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        final ZipOutputStream zipOutputStream = new ZipOutputStream(byteArrayOutputStream);
        diagnostics.dump(zipOutputStream);
        zipOutputStream.close();
        logger.info("Returned internal state!");
        return new ResponseEntity<>(new ByteArrayResource(byteArrayOutputStream.toByteArray()),
                HttpStatus.OK);
    }

}
