package com.vmturbo.mediation.azure.pricing.fetcher;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

import javax.annotation.Nonnull;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.http.Request;
import com.github.tomakehurst.wiremock.http.Response;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.vmturbo.mediation.azure.AzureApiException;
import com.vmturbo.mediation.azure.AzureConfig;
import com.vmturbo.mediation.azure.pricing.AzurePricingAccount;
import com.vmturbo.mediation.connector.azure.AzureCloudType;
import com.vmturbo.mediation.shared.httpsimulator.RecordMode;
import com.vmturbo.mediation.shared.httpsimulator.WireMockRecorder;
import com.vmturbo.platform.sdk.common.util.Pair;
import com.vmturbo.platform.sdk.probe.properties.IProbePropertySpec;
import com.vmturbo.platform.sdk.probe.properties.IPropertyProvider;

/**
 * Unit tests for {@link MCAPricesheetFetcher}.
 */
public class MCAPricesheetFetcherTest {
    private static final String PROXY_HOST = "localhost";
    private static final String PROXY_USER = "proxyUser";
    private static final String PROXY_PASSWORD = "proxyPassword";
    private static final String MCA_ACCOUNT_ID = "095c59bb-6d31-5f7d-98e2-40193a9b47ce:2d73379e-a5b1-412e-9d41-38a8236f47d1_2019-05-31";
    private static final String MCA_BILLING_PROFILE = "2N7B-VHO5-BG7-PGB";
    private static final String WIREMOCK_TENANT_ID = "5e80ef1e-d9e0-41e3-9b87-c343f1122786";
    private static final String WIREMOCK_CLIENT_ID = "SomeClientId";
    private static final String WIREMOCK_SECRET_KEY = "SomeSecretKey";

    private static final Logger logger = LogManager.getLogger();
    private static final String OVERRIDE_API_VERSION = "2525-02-05";
    private static final String INITIAL_FALURE_PROFILE = "initialFailure";
    private static final String POLLING_FALURE_PROFILE = "pollingFailure";
    private static final String DOWNLOAD_FALURE_PROFILE = "downloadFailure";

    private final Map<String, String> environ = System.getenv();

    /**
     * Allocates and cleans up a directory for temporary files for this test.
     */
    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    private final WireMockServer wireMockServer = new WireMockServer(
            new WireMockConfiguration().dynamicHttpsPort().dynamicPort()
                    .maxRequestJournalEntries(100)
                    .withRootDirectory(getWireMockSourcePath()));

    private String getWireMockSourcePath() {
        return MCAPricesheetFetcherTest.class.getClassLoader().getResource("responses").getPath();
    }

    private AzurePricingAccount getAccount() {
        final String tenantId = environ.get("MCA_TENANT_ID");
        final String clientId = environ.get("MCA_APP_ID");
        final String key = environ.get("MCA_SECRET_KEY");
        final String accountId = environ.get("MCA_ACCOUNT_ID");
        final String billingProfile = environ.get("MCA_BILLING_PROFILE");

        if (tenantId != null && clientId != null && key != null
                && accountId != null && billingProfile != null) {
            System.out.println("Testing live download");
            return new AzurePricingAccount(accountId, billingProfile, "0001", "test", tenantId,
                    clientId, key, AzureCloudType.GLOBAL, null, 0, null, null, false);
        } else {
            System.out.println("Using wiremock on port " + wireMockServer.port());
            return new AzurePricingAccount(MCA_ACCOUNT_ID, MCA_BILLING_PROFILE, "0001", "test", WIREMOCK_TENANT_ID,
                    WIREMOCK_CLIENT_ID, WIREMOCK_SECRET_KEY, AzureCloudType.GLOBAL,
                    PROXY_HOST,  wireMockServer.port(), PROXY_USER, PROXY_PASSWORD, false);
        }
    }

    /**
     * Get an account for use in testing failure cases. Different profiles select
     * different wiremock interactions.
     *
     * @param profile A billing profile ID, in this case causing different wiremock
     *   recordings to be used, to simulate different failure modes
     * @return an account for testing
     */
    private AzurePricingAccount getFailAccount(@Nonnull final String profile) {
        System.out.println("Using wiremock on port " + wireMockServer.port());

        return new AzurePricingAccount(MCA_ACCOUNT_ID, profile, "0001", "test", WIREMOCK_TENANT_ID,
            WIREMOCK_CLIENT_ID, WIREMOCK_SECRET_KEY, AzureCloudType.GLOBAL,
            PROXY_HOST,  wireMockServer.port(), PROXY_USER, PROXY_PASSWORD, false);
    }

    /**
     * Test that the downloader can construct the proper URL to initiate download.
     *
     * @throws URISyntaxException if the test fails due to generating an invalid URL
     */
    @Test
    public void testUrlCreation() throws URISyntaxException {
        AzurePricingAccount account =
            new AzurePricingAccount("TheMcaAccountId", "TheBillingProfileId",
                    "0001", "test", "tenant", "clientId", "key",
                    AzureCloudType.GLOBAL, null, 0, null, null, false);
        MCAPricesheetFetcher fetcher = new MCAPricesheetFetcher(Paths.get("/notused"));

        IPropertyProvider propertyProvider = IProbePropertySpec::getDefaultValue;
        URI uri = fetcher.buildRequestUrl(account, propertyProvider, new AzureConfig(propertyProvider));
        assertEquals("https://management.azure.com/providers/Microsoft.Billing"
                + "/billingAccounts/TheMcaAccountId/billingProfiles/TheBillingProfileId"
                + "/providers/Microsoft.CostManagement/pricesheets/default/download?api-version="
                + MCAPricesheetFetcher.DEFAULT_API_MCA_PRICESHEET_VERSION
                // This is needed due to a bug in Azure's API:
                + "&format=csv",
            uri.toString());

        // Verify that version override by property works

        IPropertyProvider apiVersionOverrideProvider = new IPropertyProvider() {
            @SuppressWarnings("unchecked")
            @Override
            @Nonnull
            public <T> T getProperty(@Nonnull IProbePropertySpec<T> property) {
                if (MCAPricesheetFetcher.API_MCA_PRICESHEET_VERSION.equals(property)) {
                    return (T)OVERRIDE_API_VERSION;
                } else {
                    return property.getDefaultValue();
                }
            }
        };

        uri = fetcher.buildRequestUrl(account, apiVersionOverrideProvider,
            new AzureConfig(apiVersionOverrideProvider));

        assertEquals("https://management.azure.com/providers/Microsoft.Billing"
                + "/billingAccounts/TheMcaAccountId/billingProfiles/TheBillingProfileId"
                + "/providers/Microsoft.CostManagement/pricesheets/default/download?api-version="
                + OVERRIDE_API_VERSION
                + "&format=csv",
            uri.toString());
    }

    /**
     * Test downloading a price sheet. Define all of MCA_TENANT_ID, MCA_APP_ID, MCA_SECRET_KEY,
     * MCA_ACCOUNT_ID, and MCA_BILLING_PROFILE in the environment to test live download. Otherwise,
     * tests using wiremock.
     *
     * @throws Exception indicates a test failure
     */
    @Test
    public void testPriceSheetDownload() throws Exception {
        IPropertyProvider propertyProvider;

        wireMockServer.addMockServiceRequestListener(MCAPricesheetFetcherTest::requestReceived);
        System.out.println("Stub mapping size: " + wireMockServer.getStubMappings().size());
        wireMockServer.start();

        final AzurePricingAccount account = getAccount();

        boolean liveDownload = false;
        if (StringUtils.isNotBlank(account.getProxyHost())) {
            // Wiremock simulation
            propertyProvider = getPropertyProvider(RecordMode.PLAYBACK);
        } else {
            propertyProvider = IProbePropertySpec::getDefaultValue;
            liveDownload = true;
        }

        try {
            final File downloadDir = tmpFolder.newFolder("pricesheet");

            MCAPricesheetFetcher client = new MCAPricesheetFetcher(downloadDir.toPath());
            Pair<Path, String> result = client.fetchPricing(account, propertyProvider);
            final Path downloadedFile = result.getFirst();
            logger.info("Downloaded file to {}", downloadedFile);
            String expectedPathPrefix = downloadDir.toPath().resolve(String.format("%s_%s_",
                    account.getMcaBillingAccountId(), account.getMcaBillingProfileId())).toString();
            System.out.println(expectedPathPrefix);
            System.out.println(downloadedFile);

            Assert.assertTrue(StringUtils.isNotBlank(result.getSecond()));
            Assert.assertTrue(downloadedFile.toString().startsWith(expectedPathPrefix));
            Assert.assertTrue(downloadedFile.toString().endsWith(".zip"));

            if (!liveDownload) {
                File expectedData = Paths.get(getWireMockSourcePath()).resolve(
                        "__files/pricesheet.zip").toFile();
                Assert.assertTrue(FileUtils.contentEquals(expectedData, downloadedFile.toFile()));
            }
        } finally {
            wireMockServer.shutdown();
            if (!liveDownload) {
                wireMockServer.checkForUnmatchedRequests();
            }
        }
    }

    /**
     * Test downloading a price sheet, with various failure modes: during initial request, during
     * polling, and during the final download.
     *
     * @throws Exception indicates a test failure
     */
    @Test
    public void testFailedRequests() throws Exception {
        wireMockServer.addMockServiceRequestListener(MCAPricesheetFetcherTest::requestReceived);
        System.out.println("Stub mapping size: " + wireMockServer.getStubMappings().size());
        wireMockServer.start();

        final IPropertyProvider propertyProvider = getPropertyProvider(RecordMode.PLAYBACK);
        final AzurePricingAccount intialFailureAccount = getFailAccount(INITIAL_FALURE_PROFILE);
        final AzurePricingAccount pollingFailureAccount = getFailAccount(POLLING_FALURE_PROFILE);
        final AzurePricingAccount downloadFailureAccount = getFailAccount(DOWNLOAD_FALURE_PROFILE);

        try {
            final File downloadDir = tmpFolder.newFolder("initialFailure");

            final MCAPricesheetFetcher client = new MCAPricesheetFetcher(downloadDir.toPath());
            assertThrows(AzureApiException.class, () -> {
                client.fetchPricing(intialFailureAccount, propertyProvider);
            });

            assertThrows(AzureApiException.class, () -> {
                client.fetchPricing(pollingFailureAccount, propertyProvider);
            });

            assertThrows(AzureApiException.class, () -> {
                client.fetchPricing(downloadFailureAccount, propertyProvider);
            });
        } finally {
            wireMockServer.checkForUnmatchedRequests();
            wireMockServer.shutdown();
        }
    }

    @Nonnull
    private static IPropertyProvider getPropertyProvider(@Nonnull RecordMode recordMode) {
        return new IPropertyProvider() {
            @SuppressWarnings("unchecked")
            @Override
            @Nonnull
            public <T> T getProperty(@Nonnull IProbePropertySpec<T> property) {
                if (WireMockRecorder.RECORD_MODE.equals(property)) {
                    return (T)recordMode;
                } else if (WireMockRecorder.ENABLE_JSON_PRETTY_PRINT.equals(property)) {
                    return (T)Boolean.TRUE;
                } else {
                    return property.getDefaultValue();
                }
            }
        };
    }

    /**
     * Logger util to aid in debugging failing tests.
     *
     * @param inRequest The request received by wiremock
     * @param inResponse The response returned by wiremock
     */
    private static void requestReceived(Request inRequest, Response inResponse) {
        logger.info("WireMock request at URL: {}", inRequest.getAbsoluteUrl());
        logger.info("WireMock request headers: {}\n", inRequest.getHeaders());
        logger.info("WireMock response body: {}\n", inResponse.getBodyAsString());
        logger.info("WireMock response headers: {}\n\n", inResponse.getHeaders());
    }
}
