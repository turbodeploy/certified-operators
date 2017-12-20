package com.vmturbo.api.component.external.api.service;

import org.junit.Test;

import com.vmturbo.api.dto.license.LicenseApiDTO;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItems;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class LicenseServiceTest {

    @Test
    public void testGetLicense() throws Exception {
        final LicenseApiDTO license = new LicenseService().getLicense();

        assertEquals("Turbonomic XL", license.getLicenseOwner());
        assertEquals("Jan 01 2019", license.getExpirationDate());
        assertEquals(28, license.getFeatures().size());
        assertThat(license.getFeatures(), hasItems("vmturbo_api"));
        assertTrue(license.getIsValid());
    }
}
