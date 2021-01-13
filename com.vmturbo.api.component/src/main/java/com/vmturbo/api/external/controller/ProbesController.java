package com.vmturbo.api.external.controller;

import java.nio.file.AccessDeniedException;
import java.util.List;
import java.util.Objects;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.SwaggerDefinition;
import io.swagger.annotations.Tag;

import com.vmturbo.api.component.external.api.serviceinterfaces.IProbesService;
import com.vmturbo.api.dto.probe.ProbeApiDTO;
import com.vmturbo.api.dto.probe.ProbePropertyApiDTO;
import com.vmturbo.api.dto.probe.ProbePropertyNameValuePairApiDTO;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.api.exceptions.UnauthorizedObjectException;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.api.utils.ApiAttributeValues;

/**
 * Every annotation that start with Api... is about Documentation
 *
 * This probes controller class can handle:
 * GET /probes/{probeId}
 * GET /probes/properties
 * GET /probes/{probeId}/properties/{propertyName}
 * GET /probes/{probeId}/targets/{targetId}/properties
 * GET /probes/{probeId}/targets/{targetId}/properties/{propertyName}
 * PUT /probes/{probeId}/properties
 * PUT /probes/{probeId}/properties/{propertyName}
 * PUT /probes/{probeId}/targets/{targetId}/properties
 * PUT /probes/{probeId}/targets/{targetId}/properties/{propertyName}
 * DELETE /probes/{probeId}/properties/{propertyName}
 * DELETE /probes/{probeId}/targets/{targetId}/properties/{propertyName}
 */
@Controller
@RequestMapping("/probes")
@Api(tags = ApiAttributeValues.TAGS_INTERNAL, value = "Probes")
@SwaggerDefinition(tags = {@Tag(name = "Probes", description = "Methods for managing probes and probe properties.")})
@PreAuthorize("hasAnyRole('ADMINISTRATOR')")
public class ProbesController {
    @Autowired
    private IProbesService probesService;

    /**
     * Get single probe.
     * GET /probes/{probeId}
     *
     * @param probeId Uuid of the probe to find.
     * @return probe information.
     * @throws UnknownObjectException probe not found by the specified Uuid.
     * @throws OperationFailedException if user input is wrong.
     * @throws AccessDeniedException if user does not have proper access privileges.
     * @throws UnauthorizedObjectException if user is properly authenticated.
     * @throws InterruptedException if thread is interrupted during processing.
     */
    @ApiOperation(
        value = "Get a probe.",
        response = ProbeApiDTO.class)
    @RequestMapping(
        path = "/{probeId}",
        method = RequestMethod.GET,
        produces = { MediaType.APPLICATION_JSON_VALUE })
    @ResponseBody
    @PreAuthorize("hasAnyRole('ADMINISTRATOR', 'DEPLOYER', 'AUTOMATOR', 'ADVISOR')")
    public ProbeApiDTO getProbe(
            @ApiParam(value = "The Uuid of the probe.", required = true)
            @PathVariable("probeId")
            String probeId) throws Exception {
        return probesService.getProbe(Objects.requireNonNull(probeId));
    }

    /**
     * Get a list of all probe properties from all probes.
     * GET "/probes/properties"
     *
     * @return probe properties list.
     * @throws UnknownObjectException should not happen.
     * @throws OperationFailedException should not happen.
     * @throws AccessDeniedException if user does not have proper access privileges.
     * @throws UnauthorizedObjectException if user is properly authenticated.
     * @throws InterruptedException if thread is interrupted during processing.
     */
    @ApiOperation(
        value = "Get a list of all probe properties from all probes.",
        response = ProbePropertyApiDTO.class,
        responseContainer = "List")
    @RequestMapping(
        path = "/properties/",
        method = RequestMethod.GET,
        produces = { MediaType.APPLICATION_JSON_VALUE })
    @ResponseBody
    @PreAuthorize("hasAnyRole('ADMINISTRATOR')")
    public List<ProbePropertyApiDTO> getAllProbeProperties()
            throws Exception {
        return probesService.getAllProbeProperties();
    }

    /**
     * Get all probe properties related to a probe.
     * GET "/probes/{probeId}/properties"
     *
     * @param probeId Uuid of the probe.
     * @throws UnknownObjectException if probe is not found by the specified Uuid.
     * @throws OperationFailedException if user input is wrong.
     * @throws AccessDeniedException if user does not have proper access privileges.
     * @throws UnauthorizedObjectException if user is properly authenticated.
     * @throws InterruptedException if thread is interrupted during processing.
     * @return list of name/value pairs of all probe-probe properties under the probe.
     */
    @ApiOperation(
        value = "Get all probe properties related to a probe.",
        response = ProbePropertyNameValuePairApiDTO.class,
        responseContainer = "List")
    @RequestMapping(
        path = "/{probeId}/properties",
        method = RequestMethod.GET,
        produces = { MediaType.APPLICATION_JSON_VALUE })
    @ResponseBody
    @PreAuthorize("hasAnyRole('ADMINISTRATOR')")
    public List<ProbePropertyNameValuePairApiDTO> getAllProbeSpecificProbeProperties(
            @ApiParam(value = "The Uuid of the probe.", required = true)
            @PathVariable("probeId")
            String probeId)

            throws Exception {
        return
            probesService.getAllProbeSpecificProbeProperties(Objects.requireNonNull(probeId));
    }

    /**
     * Get a probe-specific probe property.
     * GET /probes/{probeId}/properties/{propertyName}
     *
     * @param probeId Uuid of the probe.
     * @param propertyName name of the probe property.
     * @throws UnknownObjectException if probe is not found by the specified Uuid.
     * @throws OperationFailedException if user input is wrong.
     * @throws AccessDeniedException if user does not have proper access privileges.
     * @throws UnauthorizedObjectException if user is properly authenticated.
     * @throws InterruptedException if thread is interrupted during processing.
     * @return the value of the probe property (empty if no such probe property exists).
     */
    @ApiOperation(
        value = "Get a probe-specific probe property.",
        notes = "Response is empty if the probe exists but the probe property does not exist.",
        response = String.class)
    @RequestMapping(
        path = "/{probeId}/properties/{propertyName:.+}",
        method = RequestMethod.GET,
        produces = { MediaType.TEXT_PLAIN_VALUE })
    @ResponseBody
    @PreAuthorize("hasAnyRole('ADMINISTRATOR')")
    public String getProbeSpecificProbeProperty(
            @ApiParam(value = "The Uuid of the probe.", required = true)
            @PathVariable("probeId")
            String probeId,

            @ApiParam(value = "The name of the probe property.", required = true)
            @PathVariable("propertyName")
            String propertyName)

            throws Exception {
        return
            probesService.getProbeSpecificProbeProperty(
                Objects.requireNonNull(probeId),
                Objects.requireNonNull(propertyName));
    }

    /**
     * Get all probe properties related to a target.
     * GET "/probes/{probeId}/targets/{targetId}/properties"
     *
     * @param probeId Uuid of the probe that discovers the target.
     * @param targetId Uuid of the target.
     * @throws UnknownObjectException if probe or target is not found by the specified Uuid.
     * @throws OperationFailedException if user input is wrong.
     * @throws AccessDeniedException if user does not have proper access privileges.
     * @throws UnauthorizedObjectException if user is properly authenticated.
     * @throws InterruptedException if thread is interrupted during processing.
     * @return list of name/value pairs of all probe-probe properties under the target.
     */
    @ApiOperation(
        value = "Get all probe properties related to a target.",
        response = ProbePropertyNameValuePairApiDTO.class,
        responseContainer = "List")
    @RequestMapping(
        path = "/{probeId}/targets/{targetId}/properties",
        method = RequestMethod.GET,
        produces = { MediaType.APPLICATION_JSON_VALUE })
    @ResponseBody
    @PreAuthorize("hasAnyRole('ADMINISTRATOR')")
    public List<ProbePropertyNameValuePairApiDTO> getAllTargetSpecificProbeProperties(
            @ApiParam(value = "The Uuid of the probe that discovers the target.", required = true)
            @PathVariable("probeId")
            String probeId,

            @ApiParam(value = "The Uuid of the target.", required = true)
            @PathVariable("targetId")
            String targetId)

            throws Exception {
        return
            probesService.getAllTargetSpecificProbeProperties(
                Objects.requireNonNull(probeId),
                Objects.requireNonNull(targetId));
    }

    /**
     * Get a target-specific probe property.
     * GET /probes/{probeId}/targets/{targetId}/properties/{propertyName}
     *
     * @param probeId Uuid of the probe that discovers the target.
     * @param targetId Uuid of the target.
     * @param propertyName name of the probe property.
     * @throws UnknownObjectException if probe or the target is not found by the specified Uuid.
     * @throws OperationFailedException if user input is wrong.
     * @throws AccessDeniedException if user does not have proper access privileges.
     * @throws UnauthorizedObjectException if user is properly authenticated.
     * @throws InterruptedException if thread is interrupted during processing.
     * @return the value of the probe property (empty if no such probe property exists).
     */
    @ApiOperation(
        value = "Get a target-specific probe property.",
        notes = "Response is empty if the target exists but the probe property does not exist.")
    @RequestMapping(
        path = "/{probeId}/targets/{targetId}/properties/{propertyName:.+}",
        method = RequestMethod.GET,
        produces = { MediaType.TEXT_PLAIN_VALUE })
    @ResponseBody
    @PreAuthorize("hasAnyRole('ADMINISTRATOR')")
    public String getTargetSpecificProbeProperty(
            @ApiParam(value = "The Uuid of the probe that discovers the target.", required = true)
            @PathVariable("probeId")
            String probeId,

            @ApiParam(value = "The Uuid of the target.", required = true)
            @PathVariable("targetId")
            String targetId,

            @ApiParam(value = "The name of the property.", required = true)
            @PathVariable("propertyName")
            String propertyName)

            throws Exception {
        return
            probesService.getTargetSpecificProbeProperty(
                Objects.requireNonNull(probeId),
                Objects.requireNonNull(targetId),
                Objects.requireNonNull(propertyName));
    }

    /**
     * Edit all the probe properties of a probe.
     * PUT /probes/{probeId}/properties
     *
     * @param probeId Uuid of the probe whose probe properties are to change.
     * @param newProbeProperties new data for the probe properties.
     * @throws UnknownObjectException if probe is not found by the specified Uuid.
     * @throws AccessDeniedException if user does not have proper access privileges.
     * @throws UnauthorizedObjectException if user is properly authenticated.
     * @throws InterruptedException if thread is interrupted during processing.
     * @return new version of the probe property info.
     */
    @ApiOperation(
        value = "Edit all the probe properties of a probe.",
        notes = "All probe properties under the probe will be replaced.",
        response = ProbePropertyNameValuePairApiDTO.class,
        responseContainer = "List")
    @RequestMapping(
        path = "/{probeId}/properties",
        method = RequestMethod.PUT,
        produces = { MediaType.APPLICATION_JSON_VALUE },
        consumes = { MediaType.APPLICATION_JSON_VALUE })
    @ResponseBody
    @PreAuthorize("hasAnyRole('ADMINISTRATOR')")
    public List<ProbePropertyNameValuePairApiDTO> putAllProbeSpecificProperties(
            @ApiParam(value = "The Uuid of the probe.", required = true)
            @PathVariable("probeId")
            String probeId,

            @ApiParam(value = "The new probe properties.", required = true)
            @RequestBody
            List<ProbePropertyNameValuePairApiDTO> newProbeProperties)

            throws Exception {
        probesService.putAllProbeSpecificProperties(
            Objects.requireNonNull(probeId),
            Objects.requireNonNull(newProbeProperties));
        return newProbeProperties;
    }

    /**
     * Edit the value of one probe-specific probe property (Create if property does not exist).
     * PUT /probes/{probeId}/properties/{propertyName}
     *
     * @param probeId Uuid of the probe.
     * @param name name of the probe property.
     * @param value new value of the probe property.
     * @throws UnknownObjectException if probe is not found by the specified Uuid.
     * @throws OperationFailedException if user input is wrong.
     * @throws AccessDeniedException if user does not have proper access privileges.
     * @throws UnauthorizedObjectException if user is properly authenticated.
     * @throws InterruptedException if thread is interrupted during processing.
     * @return the new probe property info record.
     */
    @ApiOperation(
        value = "Edit the value of one probe-specific probe property.",
        notes = "If the probe property does not exist, it gets created.",
        response = ProbePropertyApiDTO.class,
        consumes = "text/plain")
    @RequestMapping(
        path = "/{probeId}/properties/{propertyName:.+}",
        method = RequestMethod.PUT,
        consumes = { MediaType.TEXT_PLAIN_VALUE },
        produces = { MediaType.APPLICATION_JSON_VALUE })
    @ResponseBody
    @PreAuthorize("hasAnyRole('ADMINISTRATOR')")
    public ProbePropertyApiDTO putProbeSpecificProperty(
            @ApiParam(value = "The Uuid of the probe.", required = true)
            @PathVariable("probeId")
            String probeId,

            @ApiParam(value = "The name of the probe property.", required = true)
            @PathVariable("propertyName")
            String name,

            @ApiParam(value = "The new value of the probe property.", required = true)
            @RequestBody
            String value)

            throws Exception {
        probesService.putProbeSpecificProperty(
            Objects.requireNonNull(probeId),
            Objects.requireNonNull(name),
            Objects.requireNonNull(value)
        );
        final ProbePropertyApiDTO result = new ProbePropertyApiDTO();
        result.setProbeId(Long.valueOf(probeId));
        result.setName(name);
        result.setValue(value);
        return result;
    }

    /**
     * Edit all the probe properties of a target.
     * PUT /probes/{probeId}/targets/{targetId}/properties
     *
     * @param probeId Uuid of the probe discovering the target.
     * @param targetId Uuid of the target
     * @param newProbeProperties new data for the probe properties.
     * @throws UnknownObjectException if probe or target is not found by the specified Uuid.
     * @throws OperationFailedException if user input is wrong.
     * @throws AccessDeniedException if user does not have proper access privileges.
     * @throws UnauthorizedObjectException if user is properly authenticated.
     * @throws InterruptedException if thread is interrupted during processing.
     * @return new version of the probe property info.
     */
    @ApiOperation(
        value = "Edit all the probe properties of a target.",
        notes = "All probe properties under the target will be replaced.",
        response = ProbePropertyNameValuePairApiDTO.class,
        responseContainer = "List")
    @RequestMapping(
        path = "/{probeId}/targets/{targetId}/properties",
        method = RequestMethod.PUT,
        produces = { MediaType.APPLICATION_JSON_VALUE },
        consumes = { MediaType.APPLICATION_JSON_VALUE })
    @ResponseBody
    @PreAuthorize("hasAnyRole('ADMINISTRATOR')")
    public List<ProbePropertyNameValuePairApiDTO> putAllTargetSpecificProperties(
            @ApiParam(value = "The Uuid of the probe discovering the target.", required = true)
            @PathVariable("probeId")
            String probeId,

            @ApiParam(value = "The Uuid of the target.", required = true)
            @PathVariable("targetId")
            String targetId,

            @ApiParam(value = "The new probe properties.", required = true)
            @RequestBody
            List<ProbePropertyNameValuePairApiDTO> newProbeProperties)
            throws Exception {
        probesService.putAllTargetSpecificProperties(
            Objects.requireNonNull(probeId),
            Objects.requireNonNull(targetId),
            Objects.requireNonNull(newProbeProperties));
        return newProbeProperties;
    }

    /**
     * Edit the value of one target-specific probe property (Create if property does not exist).
     * PUT /probes/{probeId}/targets/{targetId}/properties/{propertyName}
     *
     * @param probeId Uuid of the probe discovering the target.
     * @param targetId Uuid of the target.
     * @param name name of the probe property.
     * @param value new value of the probe property.
     * @throws UnknownObjectException if probe or target is not found by the specified Uuid.
     * @throws OperationFailedException if user input is wrong.
     * @throws AccessDeniedException if user does not have proper access privileges.
     * @throws UnauthorizedObjectException if user is properly authenticated.
     * @throws InterruptedException if thread is interrupted during processing.
     * @return the new probe property info record.
     */
    @ApiOperation(
        value = "Edit the value of one target-specific probe property.",
        notes = "If the probe property does not exist, it gets created.",
        response = ProbePropertyApiDTO.class,
        consumes = "text/plain")
    @RequestMapping(
        consumes = { MediaType.TEXT_PLAIN_VALUE },
        path = "/{probeId}/targets/{targetId}/properties/{propertyName:.+}",
        method = RequestMethod.PUT,
        produces = { MediaType.APPLICATION_JSON_VALUE })
    @ResponseBody
    @PreAuthorize("hasAnyRole('ADMINISTRATOR')")
    public ProbePropertyApiDTO putTargetSpecificProperty(
            @ApiParam(value = "The Uuid of the probe discovering the target.", required = true)
            @PathVariable("probeId")
            String probeId,

            @ApiParam(value = "The Uuid of the target.", required = true)
            @PathVariable("targetId")
            String targetId,

            @ApiParam(value = "The name of the probe property.", required = true)
            @PathVariable("propertyName")
            String name,

            @ApiParam(value = "The new value of the probe property.", required = true)
            @RequestBody
            String value)
            throws Exception {
        probesService.putTargetSpecificProperty(
            Objects.requireNonNull(probeId),
            Objects.requireNonNull(targetId),
            Objects.requireNonNull(name),
            Objects.requireNonNull(value));
        final ProbePropertyApiDTO result = new ProbePropertyApiDTO();
        result.setProbeId(Long.valueOf(probeId));
        result.setTargetId(Long.valueOf(targetId));
        result.setName(name);
        result.setValue(value);
        return result;
    }

    /**
     * Delete one probe-specific probe property.
     * DELETE /probes/{probeId}/properties/{propertyName}
     *
     * @param probeId Uuid of the probe.
     * @param name name of the probe property.
     * @throws UnknownObjectException if probe is not found by the specified Uuid.
     * @throws OperationFailedException if user input is wrong.
     * @throws AccessDeniedException if user does not have proper access privileges.
     * @throws UnauthorizedObjectException if user is properly authenticated.
     * @throws InterruptedException if thread is interrupted during processing.
     */
    @ApiOperation(
        value = "Delete the value of one probe-specific probe property.",
        notes = "Probe will revert to the use of default value.")
    @RequestMapping(
        path = "/{probeId}/properties/{propertyName}",
        method = RequestMethod.DELETE,
        produces = { MediaType.APPLICATION_JSON_VALUE })
    @ResponseBody
    @ResponseStatus(value = HttpStatus.OK)
    @PreAuthorize("hasAnyRole('ADMINISTRATOR')")
    public void deleteProbeSpecificProperty(
            @ApiParam(value = "The Uuid of the probe.", required = true)
            @PathVariable("probeId")
            String probeId,

            @ApiParam(value = "The name of the probe property.", required = true)
            @PathVariable("propertyName")
            String name)
            throws Exception {
        probesService.deleteProbeSpecificProperty(
            Objects.requireNonNull(probeId),
            Objects.requireNonNull(name));
    }

    /**
     * Delete one target-specific probe property.
     * DELETE /probes/{probeId}/targets/{targetId}/properties/{propertyName}
     *
     * @param probeId Uuid of the probe that discovers the target.
     * @param targetId Uuid of the target.
     * @param name name of the probe property.
     * @throws UnknownObjectException if probe is not found by the specified Uuid.
     * @throws OperationFailedException if user input is wrong.
     * @throws AccessDeniedException if user does not have proper access privileges.
     * @throws UnauthorizedObjectException if user is properly authenticated.
     * @throws InterruptedException if thread is interrupted during processing.
     */
    @ApiOperation(
        value = "Delete the value of one probe-specific probe property.",
        notes = "Probe will revert to the use of default value."
    )
    @RequestMapping(
        path = "/{probeId}/targets/{targetId}/properties/{propertyName}",
        method = RequestMethod.DELETE,
        produces = { MediaType.APPLICATION_JSON_VALUE })
    @ResponseBody
    @ResponseStatus(value = HttpStatus.OK)
    @PreAuthorize("hasAnyRole('ADMINISTRATOR')")
    public void deleteTargetSpecificProperty(
            @ApiParam(value = "The Uuid of the probe.", required = true)
            @PathVariable("probeId")
            String probeId,

            @ApiParam(value = "The Uuid of the target.", required = true)
            @PathVariable("targetId")
            String targetId,

            @ApiParam(value = "The name of the probe property", required = true)
            @PathVariable("propertyName")
            String name)
            throws Exception {
        probesService.deleteTargetSpecificProperty(
            Objects.requireNonNull(probeId),
            Objects.requireNonNull(targetId),
            Objects.requireNonNull(name));
    }
}
