package com.vmturbo.auth.component.services;

import javax.annotation.Nonnull;

import org.springframework.http.MediaType;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

import com.vmturbo.auth.api.licensemgmt.LicenseApiInputDTO;
import com.vmturbo.auth.component.store.ILicenseStore;

/**
 * The LicenseController implements the license REST controller.
 *
 * Every annotation that start with Api... is about Documentation
 * <p>
 * This setting controller class can handle:
 * GET /license
 * POST /license
 */
@RestController
@RequestMapping("/license")
@Api(value = "/license", description = "Methods for managing license")
public class LicenseController {
    /**
     * The underlying store.
     */
    private final ILicenseStore licenseStore;

    /**
     * Constructs LicenseController
     *
     * @param licenseStore The implementation.
     */
    public LicenseController(final @Nonnull ILicenseStore licenseStore) {
        this.licenseStore = licenseStore;
    }

    /**
     * Create or update the license.
     *
     * @param dto The request DTO.
     * @throws Exception In case of an error populating license.
     */
    @ApiOperation(value = "Create or update the license.")
    @RequestMapping(path = "",
            method = RequestMethod.POST,
            consumes = {MediaType.APPLICATION_JSON_VALUE},
            produces = {MediaType.APPLICATION_JSON_VALUE})
    @ResponseBody
    public void populateLicense(@RequestBody LicenseApiInputDTO dto) throws Exception {
        try {
            licenseStore.populateLicense(dto.getLicense());
        } catch (RuntimeException e) {
            throw new SecurityException("Unable to populate license " + e.getMessage());
        }
    }


    /**
     * Get the current license.
     *
     * @return The license in plain text  if successful.
     * @throws Exception In case of an error getting license.
     */
    @ApiOperation(value = "Get the current license in plain text.")
    @RequestMapping(path = "",
            method = RequestMethod.GET,
            produces = {MediaType.APPLICATION_JSON_VALUE})
    @ResponseBody
    public @Nonnull String getLicense() throws Exception {
        return licenseStore.getLicense().orElse("");
    }

}
