package com.vmturbo.api.external.controller;

import java.io.IOException;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;

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

import com.vmturbo.api.dto.user.SAMLConfigurationApiDTO;
import com.vmturbo.api.serviceinterfaces.ISAMLService;

/**
 * This is an XL only controller, OpsManager to modify the SAML SSO configuration.
 * This SAML controller class can handle:
 * <ul>
 *     <li>POST /saml             add or update SAML configuration
 *     <li>POST /saml/keystore    update SAML keystore
 *     <li>POST /saml/idpmetadata update SAML IDP metadata
 * </ul>
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

    /**
     * Configure the SAML keystore to use in SAML authentication.
     *
     * <p>POST /saml/keystore
     *
     * @param file the byte-stream file containing the keystore to use
     * @return the string "DONE" for success
     * @throws IOException if there is an error extracting the file contents from the arguments
     * @throws IllegalArgumentException if the input file is empty
     */
    @ApiOperation(value = "Add SAML keystore")
    @RequestMapping(method = RequestMethod.POST, value = "/keystore",
        consumes = {MediaType.MULTIPART_FORM_DATA_VALUE})
    @ResponseBody
    @PreAuthorize("hasRole('ADMINISTRATOR')")
    public String createKeyStoreFromMultipartFiles(
        @ApiParam(value = "The key store file. Accepts multiple `file`.")
        @RequestPart("file")
            MultipartFile file) throws IOException {
        if (file.isEmpty()) {
            throw new java.lang.IllegalArgumentException(PLEASE_SELECT_A_FILE_TO_UPLOAD);
        }
        samlService.updateKeystore(file.getBytes());
        return DONE;

    }

    /**
     * Configure the SAML metadata downloaded from the IDP to use.
     *
     * <p>POST /saml/idpmetadata
     *
     * @param file the byte-stream file containing the IDP metadata
     * @return the string "DONE" if successful
     * @throws Exception if there is an error reading the IDP metadata file
     * @throws IllegalArgumentException if the input file is empty
     */
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
     * Store the SAML configuration details.
     *
     * <p>POST /saml
     *
     * @param inputDto SAML configuration DTO
     * @return newly created SAMLConfigurationApiDTO
     */
    @ApiOperation(value = "Create a new SAML configuration")
    @RequestMapping(method = RequestMethod.POST,
        produces = {MediaType.APPLICATION_JSON_VALUE})
    @ResponseBody
    @PreAuthorize("hasRole('ADMINISTRATOR')")
    public SAMLConfigurationApiDTO createSamlConfiguration(
        @ApiParam(value = "Properties to create a SAML configuration", required = true)
        @RequestBody SAMLConfigurationApiDTO inputDto) {
        return samlService.createSamlConfiguration(inputDto);
    }
}
