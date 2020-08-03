package com.vmturbo.repository.search;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.BDDMockito.given;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.verify;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import com.arangodb.ArangoCursor;
import com.arangodb.ArangoDB;
import com.arangodb.ArangoDatabase;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import javaslang.collection.List;
import javaslang.control.Either;
import javaslang.control.Try;

import com.vmturbo.common.protobuf.search.Search.PropertyFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.StringFilter;
import com.vmturbo.repository.dto.ServiceEntityRepoDTO;
import com.vmturbo.repository.graph.GraphDefinition;
import com.vmturbo.repository.graph.driver.ArangoDatabaseFactory;
import com.vmturbo.repository.graph.executor.GraphDBExecutor;
import com.vmturbo.repository.graph.operator.GraphCreatorFixture;
import com.vmturbo.repository.graph.parameter.GraphCmd.ServiceEntityMultiGet;
import com.vmturbo.repository.topology.TopologyID;
import com.vmturbo.repository.topology.TopologyID.TopologyType;

@RunWith(MockitoJUnitRunner.class)
public class SearchHandlerTest {

    private SearchHandler searchHandler;

    private final GraphCreatorFixture fixture = new GraphCreatorFixture();

    private final GraphDefinition graphDefinition = fixture.getGraphDefinition();

    private final AQLRepr repr1 = new AQLRepr(List.of(Filter.propertyFilter(
            PropertyFilter.newBuilder()
                    .setPropertyName("entityType")
                    .setStringFilter(StringFilter.newBuilder()
                            .setStringPropertyRegex("DataCenter")
                            .setCaseSensitive(true)
                            .build())
                    .build())));

    private final AQLRepr repr2 = new AQLRepr(List.of(
            Filter.traversalHopFilter(Filter.TraversalDirection.CONSUMER, 2)));

    private final AQLRepr repr3 = new AQLRepr(List.of(Filter.propertyFilter(
            PropertyFilter.newBuilder()
                    .setPropertyName("displayName")
                    .setStringFilter(StringFilter.newBuilder()
                            .setStringPropertyRegex("20")
                            .setCaseSensitive(true)
                            .build())
                    .build())));

    private final java.util.List<AQLRepr> reprs = Arrays.asList(repr1, repr2, repr3);

    private final String db = "db-1";

    private final GraphDBExecutor graphDBExecutor = Mockito.mock(GraphDBExecutor.class);

    @Mock
    private ArangoDatabaseFactory arangoDatabaseFactory;

    @Mock
    private ArangoDB arangoDB;

    @Mock
    private ArangoDatabase arangoDatabase;

    @Mock
    private ArangoCursor<Object> arangoCursor;

    @Before
    public void setUp() {
        searchHandler = new SearchHandler(graphDefinition,
                                          arangoDatabaseFactory,
                                          graphDBExecutor);

        given(arangoDatabaseFactory.getArangoDriver()).willReturn(arangoDB);
        given(arangoDB.db(db)).willReturn(arangoDatabase);
        given(arangoDatabase.query(any(), any(), any(), any())).willReturn(arangoCursor);
        given(arangoCursor.asListRemaining()).willReturn(
                Arrays.asList(graphDefinition.getServiceEntityVertex() + "/1", graphDefinition.getServiceEntityVertex() + "/2"));
    }

    @Test
    public void testSearchEntityOidsWithException() {
        given(arangoDB.db(any())).willThrow(new RuntimeException());

        final Either<Throwable, java.util.List<String>> result =
                        searchHandler.searchEntityOids(reprs, db, Optional.empty(), Collections.emptyList());

        assertTrue("The result should be an exception", result.isLeft());
    }

    @Test
    public void testSearchEntityOids() {
        searchHandler.searchEntityOids(reprs, db, Optional.empty(), Lists.newArrayList("1", "2", "3"));

        verify(arangoDB, atLeast(1)).db(db);

        ArgumentCaptor<String> queriesArg = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Map> bindVarsArg = ArgumentCaptor.forClass(Map.class);
        verify(arangoDatabase, atLeast(1)).query(queriesArg.capture(),
                bindVarsArg.capture(), any(), any());
        final String queriesToArangoDB = queriesArg.getAllValues().toString();
        final Object bindVarsObj = bindVarsArg.getAllValues().get(0).get("inputs");
        final ArrayList<String> bindVars = (ArrayList)(bindVarsObj);
        assertEquals(3L, bindVars.size());
        assertTrue(bindVars.stream().anyMatch(oid -> oid.equals("seVertexCollection/1")));
        assertTrue(bindVars.stream().anyMatch(oid -> oid.equals("seVertexCollection/2")));
        assertTrue(bindVars.stream().anyMatch(oid -> oid.equals("seVertexCollection/3")));
        // The queries should contain the strings for the property names and values
        // in the PropertyFilter instances
        shouldContain(queriesToArangoDB, "entityType");
        shouldContain(queriesToArangoDB, "DataCenter");
        shouldContain(queriesToArangoDB, "OUTBOUND"); // The traversal direction is 'CONSUMER'
        shouldContain(queriesToArangoDB, "LENGTH(p.edges) == 2"); // The number of hops is 2
        shouldContain(queriesToArangoDB, "displayName");
        // 20 is the regex value for displayName.
        shouldContain(queriesToArangoDB, "20");
        shouldContain(queriesToArangoDB, "._id"); // Return the OID
    }

    @Test
    public void testSearchEntitiesWithException() {
        given(arangoDB.db(any())).willThrow(new RuntimeException());

        final Either<Throwable, java.util.List<ServiceEntityRepoDTO>> result =
                        searchHandler.searchEntities(reprs, db, Optional.empty(), Collections.emptyList());

        assertTrue("The result should be an exception", result.isLeft());
    }

    @Test
    public void testSearchEntities() {
        searchHandler.searchEntities(reprs, db, Optional.empty(), Lists.newArrayList("1", "2", "3"));

        verify(arangoDB, atLeast(1)).db(db);

        ArgumentCaptor<String> queriesArg = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Map> bindVarsArg = ArgumentCaptor.forClass(Map.class);
        verify(arangoDatabase, atLeast(1)).query(queriesArg.capture(),
                bindVarsArg.capture(), any(), any());
        final String queriesToArangoDB = queriesArg.getAllValues().toString();
        final Object bindVarsObj = bindVarsArg.getAllValues().get(0).get("inputs");
        final ArrayList<String> bindVars = (ArrayList)(bindVarsObj);
        assertEquals(3L, bindVars.size());
        assertTrue(bindVars.stream().anyMatch(oid -> oid.equals("seVertexCollection/1")));
        assertTrue(bindVars.stream().anyMatch(oid -> oid.equals("seVertexCollection/2")));
        assertTrue(bindVars.stream().anyMatch(oid -> oid.equals("seVertexCollection/3")));

        // The queries should contain the strings for the property names and values
        // in the PropertyFilter instances
        shouldContain(queriesToArangoDB, "entityType");
        shouldContain(queriesToArangoDB, "DataCenter");
        shouldContain(queriesToArangoDB, "OUTBOUND"); // The traversal direction is 'CONSUMER'
        shouldContain(queriesToArangoDB, "LENGTH(p.edges) == 2"); // The number of hops is 2
        shouldContain(queriesToArangoDB, "displayName");
        // 20 is the regex value for displayName.
        shouldContain(queriesToArangoDB, "20");
        // Need to return the entity objects
        shouldContain(queriesToArangoDB, "uuid");
        shouldContain(queriesToArangoDB, "oid");
        shouldContain(queriesToArangoDB, "displayName");
        shouldContain(queriesToArangoDB, "state");
        shouldContain(queriesToArangoDB, "severity");
        shouldContain(queriesToArangoDB, "entityType");
        shouldContain(queriesToArangoDB, "FILTER e.type == \"CONSUMES\"");
    }

    @Test
    public void testGetEntitiesByOids() {
        final TopologyID topologyID =
            new TopologyID(123L, 456L, TopologyType.SOURCE);
        final Set<Long> entityOids = Sets.newHashSet(1L, 2L);
        final GraphDefinition graphDefinition = Mockito.mock(GraphDefinition.class);
        final String serviceEntityVertex = "serviceEntityVertex";
        Mockito.when(graphDefinition.getServiceEntityVertex()).thenReturn(serviceEntityVertex);
        final ServiceEntityRepoDTO serviceEntityOne = new ServiceEntityRepoDTO();
        serviceEntityOne.setUuid("1");
        final ServiceEntityRepoDTO serviceEntityTwo = new ServiceEntityRepoDTO();
        serviceEntityTwo.setUuid("2");
        final ArrayList<ServiceEntityRepoDTO> serviceEntityRepoDTOs =
                Lists.newArrayList(serviceEntityOne, serviceEntityTwo);
        Mockito.when(graphDBExecutor.executeServiceEntityMultiGetCmd(any(ServiceEntityMultiGet.class)))
                .thenReturn(Try.success(serviceEntityRepoDTOs));
        Either<Throwable, Collection<ServiceEntityRepoDTO>> results =
                searchHandler.getEntitiesByOids(entityOids, topologyID);
        assertTrue(results.isRight());
        Collection<ServiceEntityRepoDTO> serviceEntityRepoDTOCollection = results.get();
        assertTrue(serviceEntityRepoDTOCollection.size() == 2);
        assertTrue(serviceEntityRepoDTOCollection.stream()
                .anyMatch(serviceEntityRepoDTO -> serviceEntityRepoDTO.getUuid().equals("1")));
        assertTrue(serviceEntityRepoDTOCollection.stream()
                .anyMatch(serviceEntityRepoDTO -> serviceEntityRepoDTO.getUuid().equals("2")));
    }

    private void shouldContain(@Nonnull final String content,
                               @Nonnull final String subString) {
        assertTrue("The query " + content +" should contain a substring of " + subString,
                   content.contains(subString));
    }
}