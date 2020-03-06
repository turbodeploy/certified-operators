package com.vmturbo.repository.graph.operator;

import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.vmturbo.repository.dto.ServiceEntityRepoDTO;
import com.vmturbo.repository.graph.GraphDefinition;
import com.vmturbo.repository.graph.driver.GraphDatabaseDriver;

@RunWith(MockitoJUnitRunner.class)
public class TopologyEntityTopologyGraphCreatorTest {

    private TopologyGraphCreator topologyGraphCreatorTest;

    @Mock
    private GraphDatabaseDriver graphDatabaseBuilder;

    private GraphDefinition graphDefinition;

    private GraphCreatorFixture fixture;

    @Before
    public void setUp() {
        fixture = new GraphCreatorFixture();
        graphDefinition = fixture.getGraphDefinition();

        topologyGraphCreatorTest = new TopologyGraphCreator(
                graphDatabaseBuilder,
                graphDefinition,
                1);
    }

    @Test
    public void testUpdateTopologyToDb() throws Exception {
        List<ServiceEntityRepoDTO> ses = Arrays.asList(
                new ServiceEntityRepoDTO(), new ServiceEntityRepoDTO());
        topologyGraphCreatorTest.updateTopologyToDb(ses);

        // TODO: re-write the test.
    }
}