package com.vmturbo.api.component.swagger;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import javax.ws.rs.core.MediaType;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Class to test that generated swagger file is valid.
 *
 * Validation is performed using an online validator. Attempts to use a purported swagger validation
 * maven plugin (com.github.sylvainlaurent.maven:swagger-validator-maven-plugin) failed when that
 * plugin executed but gave no signs of ever processing a file. Attempts to implement a test class
 * that used the official java OpenAPI parser (or its pre-3.0 Swagger-only counterpart) failed,
 * due to longstanding bugs in that parser.
 *
 * If a viable self-contained validation method appears, we should switch to it. Until then, this
 * appears to be the best we can do.
 *
 * TODO: Note that because this is not a self-contained test (requires web access), this should be
 * changed to an integration test once that capability exists in our build system.
 */
@Ignore
public class SwaggerValidationTest extends Assert {

    private static final String SWAGGER_RESOURCE = "/external/swagger.json";
    private static final String SWAGGER_VALIDATOR_URL = "http://online.swagger.io/validator/debug";
    private static final String ALTERNATE_SITE_URL = "http://www.google.com";

    private static String swaggerText = null;

    @BeforeClass
    public static void setup() throws IOException {
        InputStream swaggerIn = SwaggerValidationTest.class.getResourceAsStream(SWAGGER_RESOURCE);
        swaggerText = IOUtils.toString(swaggerIn, Charset.forName("UTF-8"));
    }

    @Ignore
    @Test
    public void testSwaggerIsValid() {
        validateSwagger(swaggerText);
    }

    /**
     * Perform the actual validation request and check the results using test assertions.
     *
     * This is separated from the actual validation test so that it can be reused in other
     * contexts.
     *
     * @param swaggerText The text of the swagger file to be validated
     */
    private void validateSwagger(String swaggerText) {
        HttpPost request = new HttpPost(SWAGGER_VALIDATOR_URL);
        StringEntity requestPayload = new StringEntity(swaggerText, "UTF-8");
        request.setEntity(requestPayload);
        request.setHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON);
        try (CloseableHttpResponse response = HttpClientBuilder.create().build().execute(request)) {
            assertEquals("Online validation request failed", HttpStatus.SC_OK, response.getStatusLine().getStatusCode());
            HttpEntity responsePayload = response.getEntity();
            final String text = IOUtils.toString(responsePayload.getContent(), Charset.forName("UTF-8"));
            JsonNode json = new ObjectMapper().readTree(text);
            assertTrue(collectValidationMessages(json), json.isObject() && json.size() == 0);
        } catch (IOException e) {
            // If this test fails intermittently because we can't succeed with the HTTP connection we'll let it go.
            // The risk of doing this is that the validation endpoint will change, so we'll here we'll fail if we
            // can't reach google!
            tryAlternateSite();
        }
    }

    /**
     * If validation request fails to elicit a response with a status code, we try a well-known
     * alternate site as well, and fail if that site succeeds.
     */
    private void tryAlternateSite() {
        HttpGet request = new HttpGet(ALTERNATE_SITE_URL);
        try (CloseableHttpResponse response = HttpClientBuilder.create().build().execute(request)) {
            assertEquals("Alternate site contacted but returned a failed response; check that alternate site is still active!",
                HttpStatus.SC_OK, response.getStatusLine().getStatusCode());
            fail("Swagger validation request failed, but an alternative site could be reached so unlikely to be a connectivity issue.");
        } catch (IOException e) {
            // We get here when our validation attempted failed, but so did an attempt to reach
            // our alternate site. This is strong evidence that the problem is with the validation
            // site, not with our swagger file. So we'll allow the test too pass.
        }
    }

    private String collectValidationMessages(JsonNode json) {
        JsonNode messages = json.path("messages");
        if (messages.isArray()) {
            ArrayNode array = (ArrayNode) messages;
            List<String> texts = new ArrayList<>();
            array.elements().forEachRemaining(elt -> {
                if (elt.isTextual()) {
                    texts.add(elt.asText());
                }
            });
            return "Swagger is invalid: \n    " + String.join("\n    ", texts);
        } else {
            return null;
        }
    }
}
