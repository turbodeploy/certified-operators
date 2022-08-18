package com.vmturbo.api.component.external.api.service;

import java.util.List;

import com.google.common.collect.Lists;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

import com.vmturbo.api.dto.client.ClientInputDTO;
import com.vmturbo.api.dto.client.ClientNetworkTokenApiDTO;
import com.vmturbo.api.dto.client.ClientNetworkTokensMetadataApiDTO;
import com.vmturbo.api.dto.client.ClientServiceApiDTO;
import com.vmturbo.auth.api.authorization.jwt.SecurityConstant;

/**
 * Test ClientsService.
 */
public class ClientsServiceTest {

    private static final String clientName = "testName";
    private static final String clientName2 = "testName2";
    private static final String clientSecret = "testSecret";
    private static final String clientId = "testId";
    private static final List<String> clientServices = Lists.newArrayList("VCENTER");

    private static final String networkTokenId = "testNetworkTokenId";
    private static final String networkTokenData = "testNetworkTokenData";
    private static final String networkTokenCreated = "testNetworkTokenCreated";
    private static final Integer networkTokenClaimsMade = 0;
    private static final Integer networkTokenClaimsRemaining = 1;
    private static final String networkTokenClaimsExpiration = "testNetworkTokenClaimsExpiration";

    private static final Logger logger = LogManager.getLogger();

    ClientsService clientsService;

    private RestTemplate mockRestTemplate;

    /**
     * Set ClientsService.
     */
    @Before
    public void startup() {
        mockRestTemplate = Mockito.mock(RestTemplate.class);
        clientsService = new ClientsService(mockRestTemplate,
            SecurityConstant.HYDRA_ADMIN,
            Integer.parseInt(SecurityConstant.HYDRA_ADMIN_PORT),
            SecurityConstant.HTTP,
            SecurityConstant.HYDRA_CLIENTS_PATH,
            SecurityConstant.CLIENT_NETWORK,
            Integer.parseInt(SecurityConstant.CLIENT_NETWORK_PORT),
            SecurityConstant.HTTP,
            SecurityConstant.CLIENT_NETWORK_PATH);
    }

    /**
     * Test client create.
     *
     * @throws Exception exception.
     */
    @Test
    public void testCreateClientService() throws Exception {
        setupWhenGetClients();
        ClientInputDTO clientInput = setupClientInputDTO();
        JsonObject jsonObject = setupJsonResponse(clientInput.getName());
        ResponseEntity<JsonObject> response
            = new ResponseEntity<JsonObject>(jsonObject, HttpStatus.OK);
        Mockito.when(mockRestTemplate
                .postForEntity(clientsService.prepareClientServiceUri().toString(),
                    clientsService.prepareCreateClientServiceRequest(clientInput), JsonObject.class))
            .thenReturn(response);
        ClientServiceApiDTO responseDTO = clientsService.createClientService(clientInput);
        assertClientInfo(responseDTO, clientName2);
    }

    /**
     * Test clients get.
     *
     * @throws Exception exception.
     */
    @Test
    public void testGetClientServices() throws Exception {
        setupWhenGetClients();
        List<ClientServiceApiDTO> clientsList = clientsService.getClientServices();
        clientsList.forEach(client -> {
            assertClientInfo(client, client.getName());
        });
    }

    /**
     * Test client get.
     *
     * @throws Exception exception.
     */
    @Test
    public void testGetClientService() throws Exception {
        JsonObject jsonObject = setupJsonResponse(clientName);
        ResponseEntity<JsonObject> response
            = new ResponseEntity<JsonObject>(jsonObject, HttpStatus.OK);
        Mockito.when(mockRestTemplate
            .getForEntity(clientsService.prepareClientServiceUri("/" + clientId).toString(),
                JsonObject.class))
            .thenReturn(response);
        ClientServiceApiDTO client = clientsService.getClientService(clientId);
        assertClientInfo(client, client.getName());
    }

    /**
     * Test client delete.
     *
     * @throws Exception exception.
     */
    @Test
    public void testDeleteClientService() throws Exception {
        Mockito.doNothing().when(mockRestTemplate)
            .delete(clientsService.prepareClientServiceUri("/" + clientId).toUri());
        Boolean resp = clientsService.deleteClientService(clientId);
        Assert.assertTrue(resp);
    }

    private ClientInputDTO setupClientInputDTO() {
        ClientInputDTO clientInput = new ClientInputDTO();
        clientInput.setName(clientName2);
        clientInput.setSupportedServices(clientServices);
        return clientInput;
    }

    private void setupWhenGetClients() {
        JsonArray clientsArray = new JsonArray();
        clientsArray.add(setupJsonResponse(clientName));
        ResponseEntity<JsonArray> response
            = new ResponseEntity<JsonArray>(clientsArray, HttpStatus.OK);
        Mockito.when(mockRestTemplate
                .getForEntity(clientsService.prepareClientServiceUri().toString(), JsonArray.class))
            .thenReturn(response);
    }

    private JsonObject setupJsonResponse(String expectedName) {
        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("client_name", expectedName);
        jsonObject.addProperty("client_secret", clientSecret);
        jsonObject.addProperty("client_id", clientId);
        jsonObject.addProperty("scope", String.join(" ", clientServices));
        return jsonObject;
    }

    private void assertClientInfo(ClientServiceApiDTO clientDTO, String expectedName) {
        Assert.assertEquals(clientDTO.getName(), expectedName);
        Assert.assertEquals(clientDTO.getSecret(), clientSecret);
        Assert.assertEquals(clientDTO.getId(), clientId);
        Assert.assertEquals(clientDTO.getSupportedServices(), clientServices);
    }

    private JsonObject setupNetworkTokenJsonResponse() {
        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("id", networkTokenId);
        jsonObject.addProperty(SecurityConstant.TOKEN, networkTokenData);
        return jsonObject;
    }

    private void assertClientNetworkTokenInfo(ClientNetworkTokenApiDTO clientDTO) {
        Assert.assertEquals(clientDTO.getId(), networkTokenId);
        Assert.assertEquals(clientDTO.getTokenData(), networkTokenData);
    }

    /**
     * Test client network token create.
     *
     * @throws Exception exception.
     */
    @Test
    public void testCreateClientNetworkToken() throws Exception {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        JsonObject jsonObject = setupNetworkTokenJsonResponse();
        ResponseEntity<JsonObject> response
            = new ResponseEntity<JsonObject>(jsonObject, HttpStatus.OK);
        Mockito.when(mockRestTemplate
            .postForEntity(clientsService.prepareClientNetworkUri(SecurityConstant.TOKEN).toString(),
                new HttpEntity<>("", headers), JsonObject.class))
            .thenReturn(response);
        ClientNetworkTokenApiDTO responseDTO = clientsService.createClientNetworkToken();
        assertClientNetworkTokenInfo(responseDTO);
    }

    private JsonObject setupNetworkTokenMetadataJsonResponse() {
        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty(SecurityConstant.TOKEN_NAME, networkTokenId);
        jsonObject.addProperty(SecurityConstant.CREATED, networkTokenCreated);
        jsonObject.addProperty(SecurityConstant.CLAIMS_MADE, networkTokenClaimsMade);
        jsonObject.addProperty(SecurityConstant.CLAIMS_REMAINING, networkTokenClaimsRemaining);
        jsonObject.addProperty(SecurityConstant.CLAIMS_EXPIRATION, networkTokenClaimsExpiration);
        return jsonObject;
    }

    private void assertClientNetworkTokenMetadataInfo(ClientNetworkTokensMetadataApiDTO clientDTO) {
        Assert.assertEquals(clientDTO.getId(), networkTokenId);
        Assert.assertEquals(clientDTO.getCreated(), networkTokenCreated);
        Assert.assertEquals(clientDTO.getClaimsMade(), networkTokenClaimsMade);
        Assert.assertEquals(clientDTO.getClaimsRemaining(), networkTokenClaimsRemaining);
        Assert.assertEquals(clientDTO.getClaimExpiration(), networkTokenClaimsExpiration);
    }

    /**
     * Test get client network tokens.
     *
     * @throws Exception exception.
     */
    @Test
    public void testgetClientNetworksTokens() throws Exception {
        JsonArray clientNetworkTokenArray = new JsonArray();
        clientNetworkTokenArray.add(setupNetworkTokenMetadataJsonResponse());
        ResponseEntity<JsonArray> response
            = new ResponseEntity<JsonArray>(clientNetworkTokenArray, HttpStatus.OK);
        Mockito.when(mockRestTemplate
            .getForEntity(clientsService.prepareClientNetworkUri(SecurityConstant.TOKENS).toUriString(), JsonArray.class))
            .thenReturn(response);
        List<ClientNetworkTokensMetadataApiDTO> tokenList = clientsService.getClientNetworksTokens();
        tokenList.forEach(token -> {
            assertClientNetworkTokenMetadataInfo(token);
        });
    }

    /**
     * Test client network token delete.
     *
     * @throws Exception exception.
     */
    @Test
    public void testDeleteClientNetworkToken() throws Exception {
        Mockito.doNothing().when(mockRestTemplate)
            .delete(clientsService.prepareClientNetworkUri(SecurityConstant.TOKEN + "/" + networkTokenId).toUri());
        Boolean resp = clientsService.deleteClientNetworkToken(networkTokenId);
        Assert.assertTrue(resp);
    }
}