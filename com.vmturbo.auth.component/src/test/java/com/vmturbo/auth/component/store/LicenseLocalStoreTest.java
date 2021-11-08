package com.vmturbo.auth.component.store;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import com.vmturbo.auth.component.licensing.store.ILicenseStore;
import com.vmturbo.auth.component.licensing.store.LicenseLocalStore;
import com.vmturbo.common.protobuf.licensing.Licensing.LicenseDTO;
import com.vmturbo.licensing.utils.LicenseDeserializer;

/**
 * The LicenseStoreTest tests the LicenseStore.
 * {@link LicenseLocalStore}
 */
public class LicenseLocalStoreTest {

    // when this license is coming up for expiration, we should make sure to address
    // https://vmturbo.atlassian.net/browse/OM-53399 so we don't have to update these again. (unless
    // they change for other reasons)
    public static final String C1_LICENSE = "INCREMENT C1-ECS-WOM-A cisco 1 15-jan-2022 uncounted \\\n" +
            "\tVENDOR_STRING=<Count>50</Count> HOSTID=ANY \\\n" +
            "\tNOTICE=\"<LicFileID>20191210073846581</LicFileID><LicLineID>1</LicLineID> \\\n" +
            "\t<PAK></PAK>\" SIGN=\"168C 9BF8 E102 E42D 4F02 C1FF 85AB A65F \\\n" +
            "\t2485 C056 13D0 3683 329A 85B6 C2D7 1928 5373 7021 0380 0E52 \\\n" +
            "\t7AA2 FC47 7697 A46F FF5E 29A9 2300 0B56 6E84 6C19\"";
    public static final String C1_INVALID_LICENSE = "INCREMENT C1-ECS-WOM-P cisco 1 05-dec-2017 uncounted \\\n" +
            "      VENDOR_STRING=<Count>200</Count> HOSTID=ANY \\\n" +
            "      NOTICE=\"<LicFileID>20171120165449055</LicFileID><LicLineID>1</LicLineID> \\\n" +
            "      <PAK></PAK>\" SIGN=\"0776 BCE6 6051 0697 F19F 2914 1BD1 1404 \\\n" +
            "      87F1 F42C E11F 1D2D F1A8 8AB0 1399 0303 2001 B951 5754 33B7 \\\n" +
            "      9A0E B004 692D 2FAA D7AB 1BF2 B420 AE04 4B5B 5F4B\"";
    public static final String WORKLOAD_LICENSE = "<?xml version=\"1.0\"?>\n" + "\n" + "<license>\n"
            + "    <first-name>test</first-name>\n"
            + "    <last-name>test</last-name>\n"
            + "    <email>testOnly@turbonomic.com</email>\n"
            + "    <vm-total>0</vm-total>\n"
            + "    <edition>Premier</edition>\n"
            + "    <expiration-date>2150-01-31</expiration-date>\n"
            + "    <lock-code>0d547f86c18c904efbad5eb5fc9becc4</lock-code>\n"
            + "    <feature FeatureName=\"test_feature\"/>\n" + "</license>";

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Before
    public void init() throws Exception {
        // They are needed for generating default encryption key
        System.setProperty("com.vmturbo.keydir", tempFolder.newFolder().getAbsolutePath());
        System.setProperty("com.vmturbo.kvdir", tempFolder.newFolder().getAbsolutePath());

        // Setup the license folder.
        System.setProperty("com.vmturbo.license", tempFolder.newFolder().getAbsolutePath());
    }

    @Test
    public void testStoreAndRetrieveLicense() throws Exception {
        ILicenseStore licenseStore = new LicenseLocalStore();

        // ensuring we don't have license before initialization.
        Assert.assertTrue(licenseStore.getLicenses().isEmpty());

        // C1 license.
        LicenseDTO c1LicenseDTO = LicenseDeserializer.deserialize(C1_LICENSE, null)
                .toBuilder()
                .setUuid("C1")
                .build();
        licenseStore.storeLicense(c1LicenseDTO);
        LicenseDTO first = licenseStore.getLicenses().stream().findFirst().get();
        Assert.assertEquals(c1LicenseDTO, licenseStore.getLicenses().stream().findFirst().get());

        // Remove the C1 license
        licenseStore.removeLicense("C1");
        Assert.assertEquals(0, licenseStore.getLicenses().size());

        // workload license.
        LicenseDTO workloadLicenseDTO = LicenseDeserializer.deserialize(WORKLOAD_LICENSE, null)
                .toBuilder()
                .setUuid("Workload")
                .build();
        licenseStore.storeLicense(workloadLicenseDTO);
        Assert.assertEquals(workloadLicenseDTO, licenseStore.getLicenses().stream().findFirst().get());
    }
}
