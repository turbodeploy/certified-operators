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
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

import com.vmturbo.api.dto.client.ClientInputDTO;
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
            SecurityConstant.HYDRA_CLIENTS_PATH);
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
}