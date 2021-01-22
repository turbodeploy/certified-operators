package com.vmturbo.extractor.grafana.client;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.http.client.support.BasicAuthenticationInterceptor;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.StringHttpMessageConverter;
import org.springframework.http.converter.json.GsonHttpMessageConverter;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

import com.vmturbo.auth.api.authorization.jwt.SecurityConstant;
import com.vmturbo.components.api.FormattedString;
import com.vmturbo.extractor.grafana.model.CreateDatasourceResponse;
import com.vmturbo.extractor.grafana.model.DashboardSpec.UpsertDashboardRequest;
import com.vmturbo.extractor.grafana.model.DashboardVersion;
import com.vmturbo.extractor.grafana.model.Datasource;
import com.vmturbo.extractor.grafana.model.DatasourceInput;
import com.vmturbo.extractor.grafana.model.Folder;
import com.vmturbo.extractor.grafana.model.FolderInput;
import com.vmturbo.extractor.grafana.model.UserInput;

/**
 * Hides the Grafana REST API.
 */
public class GrafanaClient {

    private static final Logger logger = LogManager.getLogger();

    private static final Gson GSON = new GsonBuilder()
            // This is important - certain fields in dashboards need to be set to null for the
            // uploaded dashboard to work properly.
            .serializeNulls()
            .create();

    private final RestTemplate restTemplate;

    private final GrafanaClientConfig grafanaClientConfig;

    /**
     * Create a new {@link GrafanaClient}.
     *
     * @param config Configuration for the client.
     */
    public GrafanaClient(@Nonnull final GrafanaClientConfig config) {
        this(config, createRestTemplate(config));
    }

    GrafanaClient(@Nonnull final GrafanaClientConfig clientConfig,
                  @Nonnull final RestTemplate restTemplate) {
        this.grafanaClientConfig = clientConfig;
        this.restTemplate = restTemplate;
    }

    private static RestTemplate createRestTemplate(@Nonnull final GrafanaClientConfig grafanaClientConfig) {
        final RestTemplate restTemplate = new RestTemplate();
        final List<ClientHttpRequestInterceptor> interceptorList = new ArrayList<>();
        interceptorList.add(new BasicAuthenticationInterceptor(grafanaClientConfig.getAdminUser(), grafanaClientConfig
                .getAdminPassword()));
        restTemplate.setInterceptors(interceptorList);
        final GsonHttpMessageConverter gsonConverter = new GsonHttpMessageConverter(GSON);
        gsonConverter.setSupportedMediaTypes(Arrays.asList(MediaType.APPLICATION_JSON));
        final List<HttpMessageConverter<?>> converters = new ArrayList<>();
        converters.add(new StringHttpMessageConverter());
        converters.add(gsonConverter);
        restTemplate.setMessageConverters(converters);

        HttpClient client = HttpClients.createDefault();
        HttpComponentsClientHttpRequestFactory reqFact = new HttpComponentsClientHttpRequestFactory(client);
        restTemplate.setRequestFactory(reqFact);
        return restTemplate;
    }

    /**
     * Get the current folders on the Grafana server, arranged by UID.
     *
     * @return (uid) -> ({@link Folder}).
     */
    @Nonnull
    public Map<String, Folder> foldersByUid() {
        final URI uri = grafanaClientConfig.getGrafanaUrl("folders");
        final ResponseEntity<Folder[]> folders = restTemplate.getForEntity(uri, Folder[].class);
        final Map<String, Folder> retMap = new HashMap<>(folders.getBody().length);
        for (Folder folder : folders.getBody()) {
            retMap.put(folder.getUid(), folder);
        }
        return retMap;
    }

    /**
     * Get the ids of dashboards on the Grafana server, arranged by UID.
     *
     * @return (uid) -> (id).
     */
    @Nonnull
    public Map<String, Long> dashboardIdsByUid() {
        final URI uri = grafanaClientConfig.getGrafanaUrl(Collections.singletonMap("type", "dash-db"), "search");
        final ResponseEntity<JsonObject[]> resp = restTemplate.getForEntity(uri, JsonObject[].class);
        final Map<String, Long> retUuids = new HashMap<>();
        for (final JsonObject obj : resp.getBody()) {
            retUuids.put(obj.get("uid").getAsString(), obj.get("id").getAsLong());
        }
        return retUuids;
    }

    @Nonnull
    private Optional<Long> getExistingUser(@Nonnull final String username) {
        final URI checkUri = grafanaClientConfig.getGrafanaUrl(
                Collections.singletonMap("loginOrEmail", username), "users", "lookup");
        try {
            ResponseEntity<JsonObject> user = restTemplate.getForEntity(checkUri, JsonObject.class);
            if (user.getBody() != null) {
                JsonElement id = user.getBody().get("id");
                return Optional.ofNullable(id).map(JsonElement::getAsLong);
            } else {
                return Optional.empty();
            }
        } catch (HttpClientErrorException e) {
            if (e.getStatusCode() == HttpStatus.NOT_FOUND) {
                return Optional.empty();
            } else {
                throw e;
            }
        }
    }

    /**
     * This is a temporary method, that should get removed when we have no more reporting users
     * on pre-7.22.9 installations.
     *
     * <p/>This ensures that the grafana user created to represent the turbo administrator has
     * the "Admin" role in the default organization. This is necessary because only admins have
     * access to the reports button.
     *
     * @param operationSummary To record the operations.
     */
    public void ensureTurboAdminIsAdmin(@Nonnull final OperationSummary operationSummary) {
        final String defaultOrgId = "1";
        String requiredRole = "Admin";
        final URI checkUri =  grafanaClientConfig.getGrafanaUrl("orgs", defaultOrgId, "users");
        ResponseEntity<JsonArray> response = restTemplate.getForEntity(checkUri, JsonArray.class);
        if (response.getBody() == null) {
            logger.warn("No response when getting users in default org: {}", defaultOrgId);
            return;
        }

        for (int i = 0; i < response.getBody().size(); ++i) {
            final JsonObject userObj = response.getBody().get(i).getAsJsonObject();
            final String username = userObj.get("login").getAsString();
            final long userId = userObj.get("userId").getAsLong();
            if (username.equalsIgnoreCase(SecurityConstant.ADMINISTRATOR)) {
                if (!userObj.get("role").getAsString().equalsIgnoreCase(requiredRole)) {
                    // The administrator user needs to have the "Admin" role.
                    if (ensureUserRole(defaultOrgId, userId, requiredRole)) {
                        operationSummary.recordUserChanged(username, userId);
                    } else {
                        operationSummary.recordUserUnchanged(username, userId);
                    }
                } else {
                    operationSummary.recordUserUnchanged(username, userId);
                }
                // No need to continue once we find the right user.
                break;
            }
        }
    }

    /**
     * Ensure that a particular user exists in the system.
     *
     * @param userInput The {@link UserInput}.
     * @param refreshSummary The {@link OperationSummary} to record the operation.
     */
    public void ensureUserExists(@Nonnull final UserInput userInput,
                                 @Nonnull final OperationSummary refreshSummary) {
        final Optional<Long> existingId = getExistingUser(userInput.getUsername());
        Long userId = null;
        if (!existingId.isPresent()) {
            final URI uri = grafanaClientConfig.getGrafanaUrl("admin", "users");
            ResponseEntity<JsonObject> response = restTemplate.postForEntity(uri, userInput, JsonObject.class);
            if (response.getBody() != null) {
                logger.info(response);
                JsonElement id = response.getBody().get("id");
                if (id != null) {
                    userId = id.getAsLong();
                    refreshSummary.recordUserCreate(userInput.getUsername(), userId);
                }
            }
        } else {
            userId = existingId.get();
            refreshSummary.recordUserUnchanged(userInput.getUsername(), userId);
        }

        // Ensure the user has a "viewer" role.
        if (userId != null) {
            ensureUserRole(Integer.toString(userInput.getOrgId()), userId, "Viewer");
        }
    }

    private boolean ensureUserRole(String orgId, long userId, String role) {
        final URI orgRole = grafanaClientConfig.getGrafanaUrl("orgs", orgId, "users", Long.toString(userId));
        JsonParser jsonParser = new JsonParser();
        JsonElement jsonElement = jsonParser.parse(FormattedString.format("{ \"role\" : \"{}\" }", role));
        final ResponseEntity<JsonObject> userUpdateResp = restTemplate.exchange(orgRole, HttpMethod.PATCH,
            new HttpEntity<>(jsonElement), JsonObject.class);
        logger.info(userUpdateResp.getBody());
        if (userUpdateResp.getStatusCode() == HttpStatus.OK) {
            return true;
        } else {
            logger.warn("Failed to change the role of user {} in org {} to {}", userId, orgId, role);
            return false;
        }
    }


    /**
     * Create a folder, or update it if it already exists, and is not equal to the provided spec.
     *
     * @param spec The spec for the folder.
     * @param existingFoldersByUid Existing folders, arranged by UID.
     * @param operationSummary {@link OperationSummary} to record the operation.
     * @return The upserted {@link Folder}.
     */
    @Nonnull
    public Folder upsertFolder(@Nonnull final FolderInput spec,
            @Nonnull final Map<String, Folder> existingFoldersByUid,
            @Nonnull final OperationSummary operationSummary) {
        final Folder existing = existingFoldersByUid.get(spec.getUid());
        if (existing != null) {
            final Optional<Folder> updatedFolder = existing.applyInput(spec);
            if (updatedFolder.isPresent()) {
                final URI uri = grafanaClientConfig.getGrafanaUrl("folders", existing.getUid());
                restTemplate.put(uri, updatedFolder.get());
                operationSummary.recordFolderUpdate(existing, updatedFolder.get());
            } else {
                operationSummary.recordFolderUnchanged(existing);
            }
            return updatedFolder.orElse(existing);
        } else {
            final URI uri = grafanaClientConfig.getGrafanaUrl("folders");
            final ResponseEntity<Folder> resp = restTemplate.postForEntity(uri, spec, Folder.class);
            final Folder createdFolder = resp.getBody();
            operationSummary.recordFolderCreate(createdFolder);
            return createdFolder;
        }
    }

    /**
     * Set the permissions on a folder.
     *
     * <p/>Note - we always set permissions, because there is no way to look up permissions in bulk,
     * and doing an extra API call to get permissions is wasteful. This means we may override
     * permissions set on these folders on the grafana server.
     *
     * @param folderUid The uid of the folder.
     * @param permissions The permissions for the folder.
     * @param operationSummary The {@link OperationSummary} to record operations.
     */
    public void setFolderPermissions(@Nonnull final String folderUid,
            @Nonnull final JsonObject permissions,
            @Nonnull final OperationSummary operationSummary) {
        final URI uri = grafanaClientConfig.getGrafanaUrl("folders", folderUid, "permissions");
        final ResponseEntity<JsonObject> resp = restTemplate.postForEntity(uri, permissions, JsonObject.class);
        operationSummary.recordFolderPermissions(folderUid, permissions, resp.getBody());
    }

    /**
     * Delete a folder (and its contents).
     *
     * @param folder The {@link Folder} to delete.
     * @param operationSummary The {@link OperationSummary} to record operations.
     */
    public void deleteFolder(@Nonnull final Folder folder,
            @Nonnull final OperationSummary operationSummary) {
        final URI uri = grafanaClientConfig.getGrafanaUrl("folders", folder.getUid());
        restTemplate.delete(uri);
        operationSummary.recordFolderDelete(folder);
    }

    /**
     * Create or update a dashboard. Note - the Grafana API creates or updates based on whether
     * an "id" is present in the request.
     *
     * @param request The {@link UpsertDashboardRequest}.
     * @param operationSummary {@link OperationSummary} to record operations.
     */
    public void upsertDashboard(@Nonnull final UpsertDashboardRequest request,
            @Nonnull final OperationSummary operationSummary) {
        final URI uri = grafanaClientConfig.getGrafanaUrl("dashboards", "db");
        final ResponseEntity<JsonObject> resp = restTemplate.postForEntity(uri, request, JsonObject.class);
        operationSummary.recordDashboardUpsert(request, resp.getBody());
    }

    @Nonnull
    private Stream<Datasource> getDatasources() {
        URI uri = grafanaClientConfig.getGrafanaUrl("datasources");
        ResponseEntity<Datasource[]> resp = restTemplate.getForEntity(uri, Datasource[].class);

        return Stream.of(resp.getBody());
    }

    /**
     * Create the datasource as the "default" datasource in Grafana. If a datasource pointing
     * at the same database exists, update it if it's not already equal to the input.
     *
     * @param input The desired datasource configuration.
     * @param operationSummary {@link OperationSummary} to record the operations.
     */
    public void upsertDefaultDatasource(@Nonnull final DatasourceInput input,
            @Nonnull final OperationSummary operationSummary) {
        final List<Datasource> existingDs = getDatasources()
                .filter(ds -> ds.matchesInput(input))
                .collect(Collectors.toList());

        final Datasource existing;
        if (existingDs.size() > 0) {
            if (existingDs.size() > 1) {
                // This shouldn't happen unless someone was being naughty.
                // Means there are multiple data sources pointing at the same database.
                logger.warn("Multiple existing datasources matching input: {}",
                    existingDs.stream()
                        .map(Datasource::toString)
                        .collect(Collectors.joining(", ")));
            }
            existing = existingDs.get(0);
        } else {
            existing = null;
        }

        if (existing == null) {
            final URI uri = grafanaClientConfig.getGrafanaUrl("datasources");
            final ResponseEntity<CreateDatasourceResponse> respEntity = restTemplate.postForEntity(uri, input, CreateDatasourceResponse.class);
            final CreateDatasourceResponse resp = respEntity.getBody();
            boolean success = false;
            if (resp != null && resp.getDatasource() != null) {
                success = testDatasourceConnection(resp.getDatasource());
                operationSummary.recordDatasourceCreation(resp.getDatasource(), success);
            }
        } else {
            final Optional<Datasource> updatedDatasource = existing.applyInput(input);
            if (updatedDatasource.isPresent()) {
                URI uri = grafanaClientConfig.getGrafanaUrl("datasources", Long.toString(existing.getId()));
                restTemplate.put(uri, updatedDatasource.get());
                operationSummary.recordDatasourceUpdate(existing, updatedDatasource.get());
            } else {
                operationSummary.recordDatasourceUnchanged(existing);
            }
        }
    }

    private boolean testDatasourceConnection(final Datasource datasource) {
        String refId = "tempvar";
        JsonObject query = new JsonParser().parse(FormattedString.format("{\"queries\":[{\"refId\":\"{}"
                + "\",\"datasourceId\": {}, \"rawSql\":\"SELECT 1\",\"format\":\"table\"}],"
                + "\"from\":\"{}\",\"to\":\"{}\"}",
                refId, datasource.getId(), System.currentTimeMillis() - 10_000, System.currentTimeMillis())).getAsJsonObject();
        URI uri = grafanaClientConfig.getGrafanaUrl("tsdb", "query");
        try {
            ResponseEntity<JsonObject> responseEntity =
                    restTemplate.postForEntity(uri, query, JsonObject.class);
            JsonObject respBody = responseEntity.getBody();
            if (respBody != null) {
                JsonElement resultObj = respBody.get("results");
                if (resultObj != null) {
                    return resultObj.getAsJsonObject().has(refId);
                }
            }
            return false;
        } catch (HttpClientErrorException e) {
            return false;
        }
    }

    /**
     * Get the latest version of a particular dashboard.
     *
     * @param existingDashboardId The id of the dashboard.
     * @return The {@link DashboardVersion} for the latest version of this dashboard. This MIGHT
     * be null, but shouldn't be if the input dashboard ID is valid.
     */
    @Nullable
    public DashboardVersion getLatestVersion(long existingDashboardId) {
        final URI uri = grafanaClientConfig.getGrafanaUrl("dashboards", "id", Long.toString(existingDashboardId), "versions");
        final ResponseEntity<DashboardVersion[]> allVersions = restTemplate.getForEntity(uri, DashboardVersion[].class);
        DashboardVersion latestVersion = null;
        for (DashboardVersion version : allVersions.getBody()) {
            if (latestVersion == null || latestVersion.getVersion() < version.getVersion()) {
                latestVersion = version;
            }
        }
        return latestVersion;
    }
}
