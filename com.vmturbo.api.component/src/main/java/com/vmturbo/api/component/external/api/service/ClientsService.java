package com.vmturbo.api.component.external.api.service;

import com.google.gson.JsonObject;

import org.apache.commons.lang3.NotImplementedException;
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
import com.vmturbo.api.dto.client.ClientServiceApiDTO;
import com.vmturbo.api.serviceinterfaces.IClientsService;
import com.vmturbo.auth.api.auditing.AuditAction;
import com.vmturbo.auth.api.auditing.AuditLogUtils;
import com.vmturbo.auth.api.authorization.jwt.SecurityConstant;
import com.vmturbo.components.api.ComponentRestTemplate;

/**
 * ClientsService.
 */
public class ClientsService implements IClientsService {

    private final Logger logger = LogManager.getLogger();

    private String clientMessage = "%s client request %s for client: %s";

    private final String clientServiceHost;

    private final Integer clientServicePort;

    private final String clientServiceScheme;

    private final String clientServicePath;

    /**
     * ClientsService.
     *
     * @param clientServiceHost host.
     * @param clientServicePort port.
     * @param clientServiceScheme scheme.
     * @param clientServicePath path.
     */
    public ClientsService(final String clientServiceHost, final Integer clientServicePort,
            final String clientServiceScheme, final String clientServicePath) {
        this.clientServiceHost = clientServiceHost;
        this.clientServicePort = clientServicePort;
        this.clientServiceScheme = clientServiceScheme;
        this.clientServicePath = clientServicePath;
    }

    @Override
    public ClientServiceApiDTO createClientService(ClientInputDTO clientInputDTO) throws Exception {
        final String uri = prepareClientServiceUri().toUriString();
        final HttpEntity<String> clientRequest = prepareCreateClientServiceRequest(clientInputDTO);
        try {
            RestTemplate restTemplate = ComponentRestTemplate.create();
            ResponseEntity<JsonObject> response = restTemplate.postForEntity(uri, clientRequest, JsonObject.class);
            JsonObject respJson = response.getBody();
            ClientServiceApiDTO client = new ClientServiceApiDTO(clientInputDTO.getSupportedServices(),
                    respJson.get(SecurityConstant.CLIENT_SECRET).getAsString());
            client.setClientId(respJson.get(SecurityConstant.CLIENT_ID).getAsString());
            client.setDisplayName(clientInputDTO.getName());
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

    private UriComponents prepareClientServiceUri() {
        UriComponentsBuilder uriBuilder = UriComponentsBuilder.newInstance()
            .scheme(clientServiceScheme)
            .host(clientServiceHost)
            .port(clientServicePort)
            .path(clientServicePath);
        return uriBuilder.build();
    }

    private HttpEntity<String> prepareCreateClientServiceRequest(ClientInputDTO clientInputDTO) {
        JSONObject payload = new JSONObject();
        payload.put(SecurityConstant.CLIENT_GRANTS, new JSONArray().put(SecurityConstant.CLIENT_CREDENTIALS));
        payload.put(SecurityConstant.CLIENT_AUTH_METHOD, SecurityConstant.CLIENT_SECRET_POST);
        payload.put(SecurityConstant.CLIENT_NAME, clientInputDTO.getName());
        payload.put(SecurityConstant.CLIENT_SCOPE, clientInputDTO.getSupportedServices().isEmpty()
            ? SecurityConstant.DEFAULT_CLIENT_SCOPE
            : SecurityConstant.DEFAULT_CLIENT_SCOPE + " " + String.join(" ", clientInputDTO.getSupportedServices()));
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<String> clientRequest = new HttpEntity<String>(payload.toString(), headers);
        return clientRequest;
    }

    @Override
    public ClientNetworkTokenApiDTO createClientNetworkToken(ClientInputDTO arg0) throws Exception {
        throw new NotImplementedException();
    }
}