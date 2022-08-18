package com.vmturbo.api.component.external.api.service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.base.Strings;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponents;
import org.springframework.web.util.UriComponentsBuilder;

import com.vmturbo.api.dto.client.ClientInputDTO;
import com.vmturbo.api.dto.client.ClientNetworkTokenApiDTO;
import com.vmturbo.api.dto.client.ClientNetworkTokensMetadataApiDTO;
import com.vmturbo.api.dto.client.ClientServiceApiDTO;
import com.vmturbo.api.serviceinterfaces.IClientsService;
import com.vmturbo.auth.api.auditing.AuditAction;
import com.vmturbo.auth.api.auditing.AuditLogUtils;
import com.vmturbo.auth.api.authorization.jwt.SecurityConstant;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;

/**
 * ClientsService.
 */
public class ClientsService implements IClientsService {

    private final Logger logger = LogManager.getLogger();

    private String clientMessage = "%s client request %s for client: %s";

    private String clientNetworkTokenMessage = "%s client network token %s for token: %s";

    private final RestTemplate restTemplate;

    private final String clientServiceHost;

    private final Integer clientServicePort;

    private final String clientServiceScheme;

    private final String clientServicePath;

    private final String clientNetworkHost;

    private final Integer clientNetworkPort;

    private final String clientNetworkScheme;

    private final String clientNetworkPath;

    /**
     * ClientsService.
     *
     * @param restTemplate restTemplate.
     * @param clientServiceHost host.
     * @param clientServicePort port.
     * @param clientServiceScheme scheme.
     * @param clientServicePath path.
     */
    public ClientsService(RestTemplate restTemplate, final String clientServiceHost,
            final Integer clientServicePort, final String clientServiceScheme,
            final String clientServicePath,
            final String clientNetworkHost,
            final Integer clientNetworkPort, final String clientNetworkScheme,
            final String clientNetworkPath) {
        this.restTemplate = restTemplate;
        this.clientServiceHost = clientServiceHost;
        this.clientServicePort = clientServicePort;
        this.clientServiceScheme = clientServiceScheme;
        this.clientServicePath = clientServicePath;
        this.clientNetworkHost = clientNetworkHost;
        this.clientNetworkPort = clientNetworkPort;
        this.clientNetworkScheme = clientNetworkScheme;
        this.clientNetworkPath = clientNetworkPath;
    }

    @Override
    public ClientServiceApiDTO createClientService(ClientInputDTO clientInputDTO) throws Exception {
        validateClientInputDTO(clientInputDTO);
        try {
            ResponseEntity<JsonObject> response = restTemplate
                .postForEntity(prepareClientServiceUri().toUriString(),
                    prepareCreateClientServiceRequest(clientInputDTO), JsonObject.class);
            ClientServiceApiDTO client = buildClientServiceApiDTO(response.getBody());
            AuditLogUtils.logSecurityAudit(AuditAction.CREATE_CLIENT,
                String.format(clientMessage, "Create", "succeeded", clientInputDTO.getName()), true);
            return client;
        } catch (RestClientException e) {
            logger.error(String.format(clientMessage, "Create", "failed", clientInputDTO.getName()), e);
            AuditLogUtils.logSecurityAudit(AuditAction.CREATE_CLIENT,
                String.format(clientMessage, "Create", "failed", clientInputDTO.getName()), false);
            throw e;
        }
    }

    @Override
    public List<ClientServiceApiDTO> getClientServices() throws Exception {
        ResponseEntity<JsonArray> response = restTemplate
            .getForEntity(prepareClientServiceUri().toUriString(), JsonArray.class);
        List<ClientServiceApiDTO> clientsList = new ArrayList<>();
        response.getBody().forEach(client -> {
            try {
                clientsList.add(buildClientServiceApiDTO((JsonObject)client));
            } catch (Exception e) {
                logger.error(e);
            }
        });
        return clientsList;
    }

    @Override
    public ClientServiceApiDTO getClientService(String clientId) throws Exception {
        ResponseEntity<JsonObject> response = restTemplate
            .getForEntity(prepareClientServiceUri("/" + clientId).toUriString(), JsonObject.class);
        ClientServiceApiDTO client = buildClientServiceApiDTO(response.getBody());
        return client;
    }

    @Override
    public Boolean deleteClientService(String clientId) throws Exception {
        try {
            restTemplate.delete(prepareClientServiceUri("/" + clientId).toUri());
            AuditLogUtils.logSecurityAudit(AuditAction.DELETE_CLIENT,
                    String.format(clientMessage, "Delete", "succeeded", clientId), true);
        } catch (RestClientException e) {
            logger.error(String.format(clientMessage, "Delete", "failed", clientId), e);
            AuditLogUtils.logSecurityAudit(AuditAction.DELETE_CLIENT,
                String.format(clientMessage, "Delete", "failed", clientId), false);
            throw e;
        }
        return true;
    }

    protected ClientServiceApiDTO buildClientServiceApiDTO(JsonObject clientJson) {
        List<String> scopes = Arrays.asList(clientJson.get(SecurityConstant.CLIENT_SCOPE)
            .getAsString().split(","));
        ClientServiceApiDTO client = new ClientServiceApiDTO();
        client.setSupportedServices(scopes);
        if (clientJson.get(SecurityConstant.CLIENT_SECRET) != null) {
            client.setSecret(clientJson.get(SecurityConstant.CLIENT_SECRET).getAsString());
        }
        client.setId(clientJson.get(SecurityConstant.CLIENT_ID).getAsString());
        client.setName(clientJson.get(SecurityConstant.CLIENT_NAME).getAsString());
        return client;
    }

    protected void validateClientInputDTO(ClientInputDTO clientInputDTO) throws Exception {
        // Name not empty.
        if (Strings.isNullOrEmpty(clientInputDTO.getName())) {
            throw new IllegalArgumentException("Name for Client Service is required.");
        }
        // Supported services specified.
        if (clientInputDTO.getSupportedServices() == null || clientInputDTO.getSupportedServices().isEmpty()) {
            throw new IllegalArgumentException("Supported Service Specification is required.");
        }
        // Supported service value cannot be empty.
        if (clientInputDTO.getSupportedServices().contains("")) {
            throw new IllegalArgumentException("Supported Service Specification cannot be empty.");
        }
        // Valid supported service passed. Log warn if unexpected value.
        Set<String> currentProbes = Arrays.stream(SDKProbeType.values())
                .map(SDKProbeType::getProbeType).collect(Collectors.toSet());
        clientInputDTO.getSupportedServices().forEach(service -> {
            if (!currentProbes.contains(service)) {
                logger.warn("Client service " + service + " is atypical.");
            }
        });
        // Name doesn't already exist.
        if (getClientServices().stream().anyMatch(existingService
            -> clientInputDTO.getName().equals(existingService.getName()))) {
                throw new IllegalArgumentException(
                    String.format("Client name %s already exists. Must be unique.",
                    clientInputDTO.getName()));
        }
    }

    protected UriComponents prepareClientServiceUri() {
        return prepareClientServiceUri("");
    }

    protected UriComponents prepareClientServiceUri(String route) {
        UriComponentsBuilder uriBuilder = UriComponentsBuilder.newInstance()
            .scheme(clientServiceScheme)
            .host(clientServiceHost)
            .port(clientServicePort)
            .path(clientServicePath + route);
        return uriBuilder.build();
    }

    protected HttpEntity<String> prepareCreateClientServiceRequest(ClientInputDTO clientInputDTO) {
        JSONObject payload = new JSONObject();
        payload.put(SecurityConstant.CLIENT_GRANTS, new JSONArray().put(SecurityConstant.CLIENT_CREDENTIALS));
        payload.put(SecurityConstant.CLIENT_AUTH_METHOD, SecurityConstant.CLIENT_SECRET_POST);
        payload.put(SecurityConstant.CLIENT_NAME, clientInputDTO.getName());
        payload.put(SecurityConstant.CLIENT_SCOPE, String.join(",", clientInputDTO.getSupportedServices()));
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<String> clientRequest = new HttpEntity<String>(payload.toString(), headers);
        return clientRequest;
    }

    protected UriComponents prepareClientNetworkUri(String route) {
        UriComponentsBuilder uriBuilder = UriComponentsBuilder.newInstance()
            .scheme(clientNetworkScheme)
            .host(clientNetworkHost)
            .port(clientNetworkPort)
            .path(clientNetworkPath + route);
        return uriBuilder.build();
    }

    @Override
    public ClientNetworkTokenApiDTO createClientNetworkToken() throws Exception {
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            ResponseEntity<JsonObject> response = restTemplate
                .postForEntity(prepareClientNetworkUri(SecurityConstant.TOKEN).toString(),
                    new HttpEntity<>("", headers), JsonObject.class);
            ClientNetworkTokenApiDTO client = new ClientNetworkTokenApiDTO();
            client.setId(response.getBody().get("id").getAsString());
            client.setTokenData(response.getBody().get(SecurityConstant.TOKEN).getAsString());
            return client;
    }

    @Override
    public List<ClientNetworkTokensMetadataApiDTO> getClientNetworksTokens() throws Exception {
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            ResponseEntity<JsonArray> response = restTemplate
                .getForEntity(prepareClientNetworkUri(SecurityConstant.TOKENS).toUriString(), JsonArray.class);
            List<ClientNetworkTokensMetadataApiDTO> clientNetworkList = new ArrayList<>();
            response.getBody().forEach(client -> {
                try {
                    clientNetworkList.add(buildClientNetworkTokensMetadataApiDTO((JsonObject)client));
                } catch (Exception e) {
                    logger.error(e);
                }
            });
            return clientNetworkList;
    }

    protected ClientNetworkTokensMetadataApiDTO buildClientNetworkTokensMetadataApiDTO(JsonObject clientJson) {
        ClientNetworkTokensMetadataApiDTO client = new ClientNetworkTokensMetadataApiDTO();
        client.setId(clientJson.get(SecurityConstant.TOKEN_NAME).getAsString());
        client.setCreated(clientJson.get(SecurityConstant.CREATED).getAsString());
        client.setClaimsMade(clientJson.get(SecurityConstant.CLAIMS_MADE).getAsInt());
        client.setClaimsRemaining(clientJson.get(SecurityConstant.CLAIMS_REMAINING).getAsInt());
        client.setClaimExpiration(clientJson.get(SecurityConstant.CLAIMS_EXPIRATION).getAsString());
        return client;
    }

    @Override
    public Boolean deleteClientNetworkToken(String tokenId) throws Exception {
        try {
            restTemplate.delete(prepareClientNetworkUri(SecurityConstant.TOKEN + "/" + tokenId).toUri());
        } catch (RestClientException e) {
            logger.error(String.format(clientNetworkTokenMessage, "Delete", "failed", tokenId), e);
            throw e;
        }
        return true;
    }
}