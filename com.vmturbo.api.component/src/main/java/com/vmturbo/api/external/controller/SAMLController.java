package com.vmturbo.api.external.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestPart;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.multipart.MultipartFile;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;

import com.vmturbo.api.dto.user.SAMLConfigurationApiDTO;
import com.vmturbo.api.serviceinterfaces.ISAMLService;

/**
 * It's XL only controller, OpsManager can modify the SAML SSO configuration file directly.
 * This SAML controller class can handle:
 * POST     /saml             add or update SAML configuration
 * POST     /samlkeystore/    update SAML keystore
 * POST     /saml/idpmetadata update SAML IDP metadata
 */
@Controller
@RequestMapping(
    value = "/saml",
    consumes = {MediaType.ALL_VALUE},
    produces = {MediaType.APPLICATION_JSON_VALUE})
@Api(tags = "Admin")
public class SAMLController {

    private static final String DONE = "done";
    private static final String PLEASE_SELECT_A_FILE_TO_UPLOAD = "Please select a file to upload";

    @Autowired(required = false)
    private ISAMLService samlService;

    @ApiOperation(value = "Add SAML keystore")
    @RequestMapping(method = RequestMethod.POST, value = "/keystore",
        consumes = {MediaType.MULTIPART_FORM_DATA_VALUE})
    @ResponseBody
    @PreAuthorize("hasRole('ADMINISTRATOR')")
    public String createKeyStoreFromMultipartFiles(
        @ApiParam(value = "The key store file. Accepts multiple `file`.")
        @RequestPart("file")
            MultipartFile file) throws Exception {
        if (file.isEmpty()) {
            throw new java.lang.IllegalArgumentException(PLEASE_SELECT_A_FILE_TO_UPLOAD);
        }
        samlService.updateKeystore(file.getBytes());
        return DONE;

    }

    @ApiOperation(value = "Add SAML IDP metadata")
    @RequestMapping(method = RequestMethod.POST, value = "/idpmetadata",
        consumes = {MediaType.MULTIPART_FORM_DATA_VALUE})
    @ResponseBody
    @PreAuthorize("hasRole('ADMINISTRATOR')")
    public String createIdpMetadata(
        @ApiParam(value = "The IDP metadata file.  Accepts multiple `file`.")
        @RequestPart("file")
            MultipartFile file) throws Exception {
        if (file.isEmpty()) {
            throw new java.lang.IllegalArgumentException(PLEASE_SELECT_A_FILE_TO_UPLOAD);
        }
        samlService.updateIdpMetadata(file.getBytes());
        return DONE;
    }

    /**
     * create a new SAML configuration
     * POST /saml
     *
     * @param inputDto SAML configuration DTO
     * @return new created SAMLConfigurationApiDTO
     * @throws Exception
     */
    @ApiOperation(value = "Create a new SAML configuration")
    @RequestMapping(method = RequestMethod.POST,
        produces = {MediaType.APPLICATION_JSON_VALUE})
    @ResponseBody
    @PreAuthorize("hasRole('ADMINISTRATOR')")
    public SAMLConfigurationApiDTO createSamlConfiguration(@ApiParam(value = "Properties to create a SAML configuration", required = true)
                                                         @RequestBody SAMLConfigurationApiDTO inputDto) throws Exception {
        return samlService.createSamlConfiguration(inputDto);
    }

}
