package com.vmturbo.components.common;

import java.io.IOException;
import java.util.zip.ZipOutputStream;

import javax.servlet.http.HttpServletResponse;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.vmturbo.components.common.diagnostics.DiagnosticsException;
import com.vmturbo.components.common.health.CompositeHealthMonitor;

/**
 * REST Endpoint for an individual {@link IVmtComponent}.
 */
@Component
@Api(value = "/")
@RestController
@RequestMapping(path = "/",
        produces = {MediaType.APPLICATION_JSON_UTF8_VALUE, MediaType.TEXT_PLAIN_VALUE})
public class ComponentController {
    private static final Logger logger = LogManager.getLogger();

    public static final String HEALTH_PATH = "/health";

    @Autowired
    @Qualifier("theComponent")
    private IVmtComponent theComponent;

    /**
     * Return information about the current component.
     *
     * <p>Note: Depends on the implementation of ServiceInstance.toString();
     *
     * @return descriptive information about the component
     */
    @RequestMapping(value = "/summary",
            method = RequestMethod.GET,
            produces = {MediaType.APPLICATION_JSON_UTF8_VALUE, MediaType.TEXT_PLAIN_VALUE})
    @ApiOperation("Return information about the current component.")
    @ResponseBody
    public String listComponentMetadata() {
        StringBuilder sb = new StringBuilder();
        return sb.append(theComponent.getComponentName())
                .append(": ")
                .append("\nstatus:\n")
                .append(getComponentStatus())
                .append("\nserviceInstance:\n")
                .append(theComponent.getComponentName())
                .append('\n')
                .toString();
    }

    @RequestMapping(path = HEALTH_PATH,
            method = RequestMethod.GET,
            produces = {MediaType.APPLICATION_JSON_UTF8_VALUE})
    @ApiOperation("Return the health of the current component.")
    @ResponseBody
    public ResponseEntity<CompositeHealthMonitor.CompositeHealthStatus> getHealth() {
        CompositeHealthMonitor.CompositeHealthStatus status = theComponent.getHealthMonitor().getHealthStatus();
        return status.isHealthy() ?
                ResponseEntity.ok(status) :
                ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(status);
    }

    /**
     * Return the status of the current component.
     *
     * @return the {@link ExecutionStatus} for the current component
     */
    @RequestMapping(path = "/state",
            method = RequestMethod.GET,
            produces = {MediaType.APPLICATION_JSON_UTF8_VALUE, MediaType.TEXT_PLAIN_VALUE})
    @ApiOperation("Return the status of the current component")
    @ResponseBody
    public String getComponentStatus() {
        return theComponent.getComponentStatus().toString();
    }

    /**
     * Fetch the diagnostic information for this component, packed into a .zip file.
     * Set the response type to indicate that this is a .zip file. The output is streamed
     * directly onto the OutputStream for the HTTPServletResponse.
     *
     * @param response the HTTPServeletResponse object for this call
     */
    @RequestMapping(path = "/diagnostics",
            method = RequestMethod.GET,
            produces = {"application/zip"})
    @ApiOperation("Fetch the diagnostic information for this component, packed into a .zip file.")
    @ResponseBody
    public void getDiagnostics(HttpServletResponse response) {
        response.setContentType("application/zip");
        ZipOutputStream diagnosticZip = null;
        try {
            diagnosticZip = new ZipOutputStream(response.getOutputStream());
        } catch (IOException e) {
            throw new RuntimeException("Error accessing the servlet response output stream", e);
        }
        try {
            theComponent.dumpDiags(diagnosticZip);
        } catch (DiagnosticsException e) {
            throw new RuntimeException("Error dumping diagnostics", e);
        }
    }
}
