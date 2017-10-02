package com.vmturbo.repository;

import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.vmturbo.repository.exception.GraphDatabaseExceptions.GraphDatabaseException;
import com.vmturbo.repository.graph.driver.GraphDatabaseDriver;
import com.vmturbo.repository.graph.driver.GraphDatabaseDriverBuilder;


/**
 * Unit tests for {@link ComponentStartUpManager}.
 */
@RunWith(MockitoJUnitRunner.class)
public class ComponentStartUpManagerTest {

    private ComponentStartUpManager componentStartUpManagerTest;

    @Mock
    GraphDatabaseDriverBuilder mockGraphDatabaseDriverBuilder;

    @Mock
    GraphDatabaseDriver mockGraphDatabaseDriver;

    @Before
    public void setUp() {
        when(mockGraphDatabaseDriverBuilder.build("")).thenReturn(mockGraphDatabaseDriver);
        componentStartUpManagerTest = new ComponentStartUpManager(mockGraphDatabaseDriverBuilder);
    }

    @Test
    public void testServerUp() throws GraphDatabaseException {
        final int maxConnectRetryCount = 10;
        final int retryDelayInMilliSec = 100;
        when(mockGraphDatabaseDriver.isServerUp()).thenReturn(true);
        // The method throws an exception if the server is down and this test fails.
        // Else, the server is up.
        componentStartUpManagerTest.waitForServerUp(maxConnectRetryCount, retryDelayInMilliSec);
    }

    @Test(expected=GraphDatabaseException.class)
    public void testServerDown() throws GraphDatabaseException {
        final int maxConnectRetryCount = 3;
        final int retryDelayInMilliSec = 100;
        when(mockGraphDatabaseDriver.isServerUp()).thenThrow(GraphDatabaseException.class);
        componentStartUpManagerTest.waitForServerUp(maxConnectRetryCount, retryDelayInMilliSec);
    }

    @Test
    public void testServerDownAndThenUp() throws GraphDatabaseException {
        final int maxConnectRetryCount = 10;
        final int retryDelayInMilliSec = 100;
        when(mockGraphDatabaseDriver.isServerUp()).thenThrow(GraphDatabaseException.class).thenReturn(true);
        // The method throws an exception if the server is down and this test fails.
        // Else, the server is up.
        componentStartUpManagerTest.waitForServerUp(maxConnectRetryCount, retryDelayInMilliSec);
    }
}