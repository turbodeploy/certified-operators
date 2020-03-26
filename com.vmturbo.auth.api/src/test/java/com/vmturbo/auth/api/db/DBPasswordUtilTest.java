package com.vmturbo.auth.api.db;

import static org.junit.Assert.assertEquals;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.method;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess;

import org.junit.Test;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.ResourceAccessException;

public class DBPasswordUtilTest {

    private final int port = 12345;
    private DBPasswordUtil passwordUtil = new DBPasswordUtil("auth", port, "", 1);
    private final String rootUri = "http://auth:" + port + DBPasswordUtil.SECURESTORAGE_PATH;
    private MockRestServiceServer mockServer =
        MockRestServiceServer.createServer(passwordUtil.getRestTemplate());

    @Test
    public void testGetSqlDbRootPasswordSuccess() throws Exception {
        mockServer.expect(requestTo(rootUri + DBPasswordUtil.SQL_DB_ROOT_PASSWORD_PATH))
            .andExpect(method(HttpMethod.GET))
            .andRespond(withSuccess("foo", MediaType.APPLICATION_JSON_UTF8));

        assertEquals("foo", passwordUtil.getSqlDbRootPassword());
    }

    @Test
    public void testGetSqlDbRootPasswordRetry() throws Exception {

        // Set up the 1st call to respond with an exception (unavailable yet)
        mockServer.expect(requestTo(rootUri + DBPasswordUtil.SQL_DB_ROOT_PASSWORD_PATH))
            .andExpect(method(HttpMethod.GET))
            .andRespond(request -> {
                throw new ResourceAccessException("unavailable");
            });

        // Set up the 2nd call to respond successful, with the  "baz" password
        mockServer.expect(requestTo(rootUri + DBPasswordUtil.SQL_DB_ROOT_PASSWORD_PATH))
            .andExpect(method(HttpMethod.GET))
            .andRespond(withSuccess("baz", MediaType.APPLICATION_JSON_UTF8));

        assertEquals("baz", passwordUtil.getSqlDbRootPassword());
    }

    @Test
    public void testGetArangoDbRootPassword() throws Exception {
        mockServer.expect(requestTo(rootUri + DBPasswordUtil.ARANGO_DB_ROOT_PASSWORD_PATH))
            .andExpect(method(HttpMethod.GET))
            .andRespond(withSuccess("bar", MediaType.APPLICATION_JSON_UTF8));

        assertEquals("bar", passwordUtil.getArangoDbRootPassword());
    }

    @Test
    public void testInfluxDbRootPassword() throws Exception {
        mockServer.expect(requestTo(rootUri + DBPasswordUtil.INFLUX_DB_ROOT_PASSWORD_PATH))
            .andExpect(method(HttpMethod.GET))
            .andRespond(withSuccess("baz", MediaType.APPLICATION_JSON_UTF8));

        assertEquals("baz", passwordUtil.getInfluxDbRootPassword());
    }
}