package com.vmturbo.components.common.diagnostics;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Objects;
import java.util.zip.ZipOutputStream;

import javax.annotation.Nonnull;

import io.swagger.annotations.ApiOperation;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

/**
 * The REST endpoints to get the internal state of the group component.
 */
@RestController
@RequestMapping("/internal-state")
public class DiagnosticsController {

    private final IDiagnosticsHandler diagnostics;
    private final Logger logger = LogManager.getLogger(getClass());

    /**
     * Constructs diagnostics controller.
     *
     * @param diagnostics diagnostics handler to operate with
     */
    public DiagnosticsController(@Nonnull final IDiagnosticsHandler diagnostics) {
        this.diagnostics = Objects.requireNonNull(diagnostics);
    }

    /**
     * Dumps the internal state of a component.
     *
     * @param responseOutput a stream containing zipped diagnostics of the component
     * @throws IOException on IO operations exceptions
     */
    @RequestMapping(method = RequestMethod.GET, produces = {"application/zip"})
    @ApiOperation(value = "Get the internal state.",
            notes = "Returns a zip file streamed through the HTTP connection.")
    public void dumpInternalState(@Nonnull OutputStream responseOutput) throws IOException {
        logger.info("Dumping component's diagnostics...");
        final ZipOutputStream zipOutputStream = new ZipOutputStream(responseOutput);
        diagnostics.dump(zipOutputStream);
        zipOutputStream.close();
        logger.info("Dumping diagnostics finished");
    }

    @Nonnull
    protected Logger getLogger() {
        return logger;
    }
}
