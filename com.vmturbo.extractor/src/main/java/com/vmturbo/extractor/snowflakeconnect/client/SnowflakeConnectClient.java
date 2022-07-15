package com.vmturbo.extractor.snowflakeconnect.client;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.annotation.Nonnull;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import io.jsonwebtoken.lang.Collections;

import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.http.MediaType;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.StringHttpMessageConverter;
import org.springframework.http.converter.json.GsonHttpMessageConverter;
import org.springframework.web.client.RestTemplate;

import com.vmturbo.extractor.snowflakeconnect.model.Config;
import com.vmturbo.extractor.snowflakeconnect.model.Connector;

/**
 * Class used to invoke the kafka connect REST API of the snowflake connector.
 */
public class SnowflakeConnectClient {

    private static final Logger logger = LogManager.getLogger();

    private static final Gson GSON = new GsonBuilder().create();

    private final RestTemplate restTemplate = createRestTemplate();

    private final SnowflakeConnectClientConfig config;

    /**
     * Create a new snowflake REST client.
     *
     * @param config configuration for the client
     */
    public SnowflakeConnectClient(SnowflakeConnectClientConfig config) {
        this.config = config;
    }

    /**
     * Performs GET connectors/ to retrieve all connectors.
     *
     * @return list of connector names
     */
    @Nonnull
    public List<String> getConnectors() {
        URI uri = config.getConnectorUrl("connectors");
        String[] connectors = restTemplate.getForEntity(uri, String[].class).getBody();
        logger.debug("Live connectors are {}", connectors);
        return Collections.arrayToList(connectors);
    }

    /**
     * Performs GET connectors/{connectorName} to retrieve info about a specific connector.
     *
     * @param connectorName the name of the connector as it is returned by {@link
     *         SnowflakeConnectClient#getConnectors()}
     * @return the connector object which contains the config info
     */
    @Nonnull
    public Connector getConnector(String connectorName) {
        URI uri = config.getConnectorUrl("connectors", connectorName);
        Connector connector = restTemplate.getForEntity(uri, Connector.class).getBody();
        logger.debug("Got connector {}", connector);
        return connector;
    }

    /**
     * Performs POST connectors/ to create a new connector.
     *
     * @param configuration the configuration object for the new connector
     * @return the newly created connector object
     */
    @Nonnull
    public Connector createConnector(Config configuration) {
        URI uri = config.getConnectorUrl("connectors");
        Connector connector = new Connector();
        connector.setName(configuration.getName());
        connector.setConfig(configuration);
        Connector newConnector = restTemplate.postForObject(uri, connector, Connector.class);
        logger.debug("Created connector {}", newConnector);
        return newConnector;
    }

    /**
     * Performs DELETE connectors/{connectorName} to delete a connector.
     *
     * @param connectorName the name of the connector as it is returned by {@link
     *         SnowflakeConnectClient#getConnectors()}
     */
    public void deleteConnector(String connectorName) {
        URI uri = config.getConnectorUrl("connectors", connectorName);
        restTemplate.delete(uri);
        logger.debug("Deleted connector {}", connectorName);
    }

    private static RestTemplate createRestTemplate() {
        final RestTemplate restTemplate = new RestTemplate();

        final GsonHttpMessageConverter gsonConverter = new GsonHttpMessageConverter(GSON);
        gsonConverter.setSupportedMediaTypes(Arrays.asList(MediaType.APPLICATION_JSON));
        final List<HttpMessageConverter<?>> converters = new ArrayList<>();
        converters.add(new StringHttpMessageConverter());
        converters.add(gsonConverter);
        restTemplate.setMessageConverters(converters);

        HttpClient client = HttpClients.createDefault();
        HttpComponentsClientHttpRequestFactory reqFact =
                new HttpComponentsClientHttpRequestFactory(client);
        restTemplate.setRequestFactory(reqFact);
        return restTemplate;
    }
}
