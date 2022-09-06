package com.vmturbo.mediation.azure.pricing.fetcher;

import static com.vmturbo.mediation.azure.AzureConnector.GSON;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nonnull;

import org.apache.commons.io.IOUtils;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import com.vmturbo.mediation.azure.AzureApiException;
import com.vmturbo.mediation.azure.AzureConfig;
import com.vmturbo.mediation.azure.AzureConnectionException;
import com.vmturbo.mediation.azure.AzureConnector;
import com.vmturbo.mediation.azure.AzureConnector.HttpMethod;
import com.vmturbo.mediation.azure.AzureProbeException;
import com.vmturbo.mediation.azure.pricing.AzurePricingAccount;
import com.vmturbo.mediation.azure.util.AzureUtils;
import com.vmturbo.mediation.shared.httpsimulator.RecordMode;
import com.vmturbo.platform.sdk.common.util.Pair;
import com.vmturbo.platform.sdk.probe.properties.IProbePropertySpec;
import com.vmturbo.platform.sdk.probe.properties.IPropertyProvider;
import com.vmturbo.platform.sdk.probe.properties.PropertySpec;

/**
 * Azure API client encapsulating communication with Azure API for Azure Service Principal probe.
 */
public class MCAPricesheetFetcher implements PricingFileFetcher<AzurePricingAccount> {
    /**
     * The default version of the price sheet API to use.
     */
    public static final String DEFAULT_API_MCA_PRICESHEET_VERSION = "2022-02-01-preview";

    /**
     * Configurable property for the API version to use for MCA pricesheets.
     */
    public static final IProbePropertySpec<String>
        API_MCA_PRICESHEET_VERSION = new PropertySpec("api.mca.pricesheet.version",
            String::valueOf, DEFAULT_API_MCA_PRICESHEET_VERSION);

    private static final String AZURE_PRICE_SHEET_REQUEST_PATH =
        "providers/Microsoft.Billing/billingAccounts/%s/billingProfiles/%s"
        + "/providers/Microsoft.CostManagement/pricesheets/default/download";
    private static final String AZURE_SCHEME = "https";

    private static final SimpleDateFormat fileDate = new SimpleDateFormat("yyyyMMddHHmmss");

    private static final Logger LOGGER = LogManager.getLogger();

    private Path savePath;

    /**
     * Create a price sheet fetcher.
     *
     * @param savePath the path to which to save the downloaded file
     */
    public MCAPricesheetFetcher(@Nonnull Path savePath) {
        this.savePath = savePath;
    }

    /**
     * Fetch a price sheet to a local file.
     *
     * @param account the account for which to fetch a pricing file
     * @param propertyProvider properties configuring the discovery
     * @return the filename to which the price sheet was saved, and a status message
     * @throws AzureProbeException can be thrown due to Azure API call errors
     * @throws IOException in case of other communication errors
     * @throws URISyntaxException if the URL returned by Azure were somehow invalid
     * @throws InterruptedException in case of an interrupted thread
     */
    @Nonnull
    public Pair<Path, String> fetchPricing(@Nonnull AzurePricingAccount account,
            @Nonnull IPropertyProvider propertyProvider)
            throws AzureProbeException, IOException, URISyntaxException, InterruptedException {
        String token = "NO_TOKEN";

        AzureConfig azureConfig = new AzureConfig(propertyProvider);

        // Don't authenticate if using wiremock for testing
        if (azureConfig.getWireMockRecordMode() != RecordMode.PLAYBACK) {
            try (CloseableHttpClient httpClient = AzureUtils.buildHttpClient(account, azureConfig)) {
                token = AzureConnector.getAccessToken(account, httpClient);
            }
        }

        try (CloseableHttpClient httpClient = AzureUtils.buildHttpClient(account, azureConfig)) {
            AzureConnector connector = new AzureConnector(account, token, null,
                    httpClient);

            final String url = getDownloadUrl(account, connector, propertyProvider, azureConfig);

            connector = new AzureConnector(account, null, null, httpClient);

            final Path downloadedFile = downloadSheet(account, url, connector);
            final long size = Files.size(downloadedFile);
            final String status = String.format("Downloaded %d bytes", size);

            return new Pair<>(downloadedFile, status);
        }
    }

    /**
     * Given the generated download URL, download the price sheet.
     *
     * @param account the account for which to fetch a pricing file
     * @param url the download URL returned by the Azure API
     * @param connector the connecor used for communication.
     * @return the filename to which the price sheet was saved
     * @throws AzureProbeException can be thrown due to Azure API call errors
     * @throws IOException in case of other communication errors.
     */
    @Nonnull
    private Path downloadSheet(
            @Nonnull AzurePricingAccount account,
            @Nonnull String url,
            @Nonnull AzureConnector connector)
            throws AzureProbeException, IOException {
        final List<Pair<String, String>> headers = connector.createBasicHeader();
        final Path savedFilePath = getDownloadSaveFile(account);
        LOGGER.info("Downloading price sheet from {} to {}", url, savedFilePath);

        try (InputStream inStream = connector.sendApiRequest(
                url, HttpMethod.GET, null, ContentType.DEFAULT_BINARY, headers)) {

            try (OutputStream outStream = Files.newOutputStream(savedFilePath)) {
                 IOUtils.copy(inStream, outStream);
            }
        }

        return savedFilePath;
    }

    /**
     * Cause Azure to generate a price sheet and return its download URL. Makes the API
     * request, and then polls until the operation is complete and the download
     * URL is known.
     *
     * @param account the account for which to get a pricing file download URL
     * @param connector the connector to use for communication
     * @param propertyProvider configures the API version used
     * @param azureConfig configures Azure communication
     * @return the URL from which to download the price sheet
     * @throws AzureProbeException can be thrown due to Azure API call errors
     * @throws IOException in case of other communication errors
     * @throws URISyntaxException if the URL returned by Azure were somehow invalid
     * @throws InterruptedException in case of an interrupted thread
     */
    @Nonnull
    private String getDownloadUrl(
            @Nonnull AzurePricingAccount account,
            @Nonnull AzureConnector connector,
            @Nonnull IPropertyProvider propertyProvider,
            @Nonnull AzureConfig azureConfig)
            throws URISyntaxException, AzureConnectionException, InterruptedException,
            AzureApiException, IOException {

        final List<Pair<String, String>> headers = connector.createBasicHeader();
        String url = buildRequestUrl(account, propertyProvider, azureConfig).toString();
        LOGGER.debug("URL: {}", url);
        CloseableHttpResponse response;

        response = connector.getHttpResponseFromAzure(
                url, HttpMethod.POST, "", ContentType.APPLICATION_JSON, headers);
        int statusCode = response.getStatusLine().getStatusCode();

        while (statusCode == HttpStatus.SC_ACCEPTED) {
            final Header location = response.getFirstHeader(HttpHeaders.LOCATION);
            if (location == null) {
                throw new AzureApiException(statusCode, "Received Accepted status but no Location header");
            } else {
                url = location.getValue();
            }

            final Header retryAfter = response.getFirstHeader(HttpHeaders.RETRY_AFTER);
            if (retryAfter != null) {
                long sleepMs = 10000L;
                try {
                    sleepMs = Integer.valueOf(retryAfter.getValue()) * 1000L;
                } catch (NumberFormatException ex) {
                    LOGGER.warn("Unable to parse Retry-After value: {}", retryAfter.getValue());
                }
                Thread.sleep(sleepMs);
            }

            response = connector.getHttpResponseFromAzure(
                    url, HttpMethod.GET, null, ContentType.APPLICATION_JSON, headers);
            statusCode = response.getStatusLine().getStatusCode();
        }

        // At ths point we have done the last request and have the final response or error

        final HttpEntity responseEntity = response.getEntity();

        if (statusCode > HttpStatus.SC_OK) {
            if (responseEntity != null) {
                final String errorMessage = EntityUtils.toString(responseEntity);
                throw new AzureApiException(statusCode, errorMessage);
            } else {
                throw new AzureApiException(statusCode, "No response body");
            }
        }

        try (InputStream is = responseEntity.getContent()) {
            String json = IOUtils.toString(is, Charset.defaultCharset());

            MCAPricesheetResponse priceSheetResponse = GSON.fromJson(json, MCAPricesheetResponse.class);
            if (priceSheetResponse.getErrorMessage() != null) {
                throw new AzureConnectionException(priceSheetResponse.getErrorMessage());
            }

            if (priceSheetResponse.getPublishedEntity() == null
                || priceSheetResponse.getPublishedEntity().getProperties() == null
                || priceSheetResponse.getPublishedEntity().getProperties().getDownloadUrl() == null) {
                throw new AzureConnectionException("No download URL present in response");
            }

            return priceSheetResponse.getPublishedEntity().getProperties().getDownloadUrl();
        }
    }

    /**
     * Create a request URL for initiating a price sheet download.
     *
     * @param account the MCA account ID to which this price sheet applies
     * @param propertyProvider configures the API version used
     * @param azureConfig Azure communication configuration
     * @return the URL to call to initiate download
     * @throws URISyntaxException if the generated URL is not valid (should not happen)
     */
    public URI buildRequestUrl(@Nonnull AzurePricingAccount account,
            @Nonnull IPropertyProvider propertyProvider,
            @Nonnull AzureConfig azureConfig) throws URISyntaxException {
        final String host = account.getAzureCloudType().getMgmtServer();
        final String apiVersion = propertyProvider.getProperty(API_MCA_PRICESHEET_VERSION);
        final URIBuilder builder = new URIBuilder();

        builder.setScheme(azureConfig.getWireMockRecordMode() == RecordMode.PLAYBACK ? "http" : AZURE_SCHEME)
                .setHost(host)
                .setPath(String.format(AZURE_PRICE_SHEET_REQUEST_PATH,
                        account.getMcaBillingAccountId(), account.getMcaBillingProfileId()))
                .addParameter("api-version", apiVersion)
                // AZURE'S API BUG: The following is undocumented and is not supposed to be necessary
                .addParameter("format", "csv");

        return builder.build();
    }

    @Nonnull
    private Path getDownloadSaveFile(@Nonnull AzurePricingAccount account) {
        final String filename = String.format("%s_%s_%s.zip",
                account.getMcaBillingAccountId(),
                account.getMcaBillingProfileId(),
                fileDate.format(new Date()));

        return savePath.resolve(filename);
    }

    @NotNull
    @Override
    public Object getCacheKey(@NotNull AzurePricingAccount account) {
        return new MCAPricesheetKey(account.getMcaBillingAccountId(), account.getMcaBillingProfileId());
    }

    /**
     * Class representing a cache key for a AzurePricingAccount.
     */
    private static class MCAPricesheetKey {
        private String mcaAccountId;
        private String mcaBillingProfileId;

        private MCAPricesheetKey(@Nonnull String mcaAccountId, @Nonnull String mcaBillingProfileId) {
            this.mcaAccountId = mcaAccountId;
            this.mcaBillingProfileId = mcaBillingProfileId;
        }

        @Override
        public String toString() {
            return String.format("Account %s Profile %s", mcaAccountId, mcaBillingProfileId);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            MCAPricesheetKey that = (MCAPricesheetKey)o;
            return Objects.equals(mcaAccountId, that.mcaAccountId) && Objects.equals(
                    mcaBillingProfileId, that.mcaBillingProfileId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(mcaAccountId, mcaBillingProfileId);
        }
    }
}