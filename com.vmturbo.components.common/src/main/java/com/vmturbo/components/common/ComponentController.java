package com.vmturbo.components.common;

import java.io.IOException;
import java.util.zip.ZipOutputStream;

import javax.servlet.http.HttpServletResponse;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

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
     * Initiate a state change, moving from the current state to a target {@link ExecutionStatus}. This state transition
     * is asynchronous. As such, the current state as returned from this request may not be equal to the new state.
     *
     * @param newState the target state to initiate a transition to; this transition is asynchronous
     * @return the current state after the transition, which may not (yet) be equal to the requested target state
     */
    @RequestMapping(path = "/state",
            method = RequestMethod.PUT,
            consumes = {MediaType.TEXT_PLAIN_VALUE},
            produces = {MediaType.APPLICATION_JSON_UTF8_VALUE, MediaType.TEXT_PLAIN_VALUE})
    @ApiOperation("Initiate a state change, moving from the current state to a target " +
            "ExecutionStatus. This state transition is asynchronous. " +
            "As such, the current state as returned from this request may not be equal to the new state.")
    @ResponseBody
    public String putComponentStatus(@RequestBody String newState) {
        switch (ExecutionStatus.valueOf(newState)) {
            case STARTING:
                theComponent.startComponent();
                break;
            case STOPPING:
                theComponent.stopComponent();
                break;
            case PAUSED:
                theComponent.pauseComponent();
                break;
            case RUNNING:
                theComponent.resumeComponent();
                break;
        }
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
        theComponent.dumpDiags(diagnosticZip);
    }

}
