package com.vmturbo.auth.component.licensing;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import com.vmturbo.auth.api.licensing.LicenseApiInputDTO;
import com.vmturbo.auth.component.store.LicenseLocalStoreTest;

/**
 * Base class for testing {@link com.vmturbo.auth.component.services.LicenseController}
 */
@Ignore // will revisit while fixing licensing authorization in OM-35910
public class LicenseRestBase {
    /**
     * The JSON builder.
     */
    private static final Gson GSON = new GsonBuilder().create();
    private static final String RET_TYPE = MediaType.APPLICATION_JSON_UTF8_VALUE;
    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();
    /**
     * The mock service.
     */
    private MockMvc mockMvc;
    @Autowired
    private WebApplicationContext wac;

    @Before
    public void setUp() throws IOException {
        // They are needed for generating default encryption key
        System.setProperty("com.vmturbo.keydir", tempFolder.newFolder().getAbsolutePath());
        System.setProperty("com.vmturbo.kvdir", tempFolder.newFolder().getAbsolutePath());

        // Setup the license folder.
        System.setProperty("com.vmturbo.license", tempFolder.newFolder().getAbsolutePath());
        mockMvc = MockMvcBuilders.webAppContextSetup(wac).build();
    }

    private String constructLicenseDTO(String license) {
        LicenseApiInputDTO dto = new LicenseApiInputDTO();
        dto.setLicense(license);
        // For debigging purposes.
        String json = GSON.toJson(dto, LicenseApiInputDTO.class);
        return json;
    }

    private MockHttpServletRequestBuilder postPopulateLicense(String license) {
        return post("/license")
                .content(constructLicenseDTO(license))
                .contentType(RET_TYPE)
                .accept(RET_TYPE);
    }

    private MockHttpServletRequestBuilder getLicense() {
        return get("/license")
                .contentType(RET_TYPE)
                .accept(RET_TYPE);
    }

    /**
     * Performs the actual logon from the token passed though.
     */
    private void logon(String role) throws Exception {
        // Local authentication
        Set<GrantedAuthority> grantedAuths = new HashSet<>();
        for (String r : role.split("\\|")) {
            grantedAuths.add(new SimpleGrantedAuthority("ROLE" + "_" + r.toUpperCase()));
        }
        SecurityContextHolder.getContext().setAuthentication(
                new UsernamePasswordAuthenticationToken("admin", "admin000", grantedAuths));
    }

    /**
     * Happy path of populating workload license.
     *
     * @throws Exception if any.
     */
    @Test
    public void testPopulateWorkloadLicense() throws Exception {
        logon("ADMINISTRATOR");
        String result = mockMvc.perform(postPopulateLicense(LicenseLocalStoreTest.WORKLOAD_LICENSE))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();
        Assert.assertEquals("", result);
        SecurityContextHolder.getContext().setAuthentication(null);
    }

    /**
     * Happy path of getting workload license.
     *
     * @throws Exception if any.
     */
    @Test
    public void testGetWorkloadLicense() throws Exception {
        logon("ADMINISTRATOR");
        // populate for the license
        mockMvc.perform(postPopulateLicense(LicenseLocalStoreTest.WORKLOAD_LICENSE))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();

        String result = mockMvc.perform(getLicense())
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();
        Assert.assertEquals(LicenseLocalStoreTest.WORKLOAD_LICENSE, result);
        SecurityContextHolder.getContext().setAuthentication(null);
    }

    /**
     * Happy path of populating C1 license.
     *
     * @throws Exception if any
     */
    @Test
    public void testPopulateC1License() throws Exception {
        logon("ADMINISTRATOR");
        String result = mockMvc.perform(postPopulateLicense(LicenseLocalStoreTest.C1_LICENSE))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();
        Assert.assertEquals("", result);
        SecurityContextHolder.getContext().setAuthentication(null);
    }

    /**
     * Happy path of getting C1 license.
     *
     * @throws Exception if any
     */
    @Test
    public void testGetC1License() throws Exception {
        logon("ADMINISTRATOR");
        // populate for the license
        mockMvc.perform(postPopulateLicense(LicenseLocalStoreTest.C1_LICENSE))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();

        String result = mockMvc.perform(getLicense())
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();
        Assert.assertEquals(LicenseLocalStoreTest.C1_LICENSE, result);
        SecurityContextHolder.getContext().setAuthentication(null);
    }

    /**
     * Negative path of populating license with ADVISER role.
     *
     * @throws Exception if any
     */
    @Test
    public void testPopulateLicenseWithoutAdminRole() throws Exception {
        logon("ADVISER");
        String result = mockMvc.perform(postPopulateLicense(LicenseLocalStoreTest.C1_LICENSE))
                .andExpect(status().is(403))
                .andReturn().getResponse().getContentAsString();
        SecurityContextHolder.getContext().setAuthentication(null);
    }

    /**
     * Ensure unauthenticated user can get license information
     *
     * @throws Exception if any
     */
    @Test
    public void testGetLicenseWithoutAdminRole() throws Exception {
        logon("ADMINISTRATOR");
        mockMvc.perform(postPopulateLicense(LicenseLocalStoreTest.C1_LICENSE))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();
        SecurityContextHolder.getContext().setAuthentication(null);

        String result = mockMvc.perform(getLicense())
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();
        Assert.assertEquals(LicenseLocalStoreTest.C1_LICENSE, result);

    }
}
