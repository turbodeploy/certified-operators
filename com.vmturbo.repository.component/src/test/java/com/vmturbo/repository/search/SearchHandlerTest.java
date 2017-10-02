package com.vmturbo.repository.search;

import com.arangodb.ArangoCursor;
import com.arangodb.ArangoDB;
import com.arangodb.ArangoDatabase;
import com.vmturbo.repository.dto.ServiceEntityRepoDTO;
import com.vmturbo.repository.graph.GraphDefinition;
import com.vmturbo.repository.graph.driver.ArangoDatabaseFactory;
import com.vmturbo.repository.graph.operator.GraphCreatorFixture;

import javaslang.collection.List;
import javaslang.control.Either;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.Collection;

import javax.annotation.Nonnull;

import static org.junit.Assert.assertTrue;
import static org.mockito.BDDMockito.given;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.atLeast;

@RunWith(MockitoJUnitRunner.class)
public class SearchHandlerTest {

    private SearchHandler searchHandler;

    private final GraphCreatorFixture fixture = new GraphCreatorFixture();

    private final GraphDefinition graphDefinition = fixture.getGraphDefinition();


    private final AQLRepr repr1 = new AQLRepr(List.of(
            Filter.stringPropertyFilter("entityType", Filter.StringOperator.REGEX, "DataCenter")));

    private final AQLRepr repr2 = new AQLRepr(List.of(
            Filter.traversalHopFilter(Filter.TraversalDirection.CONSUMER, 2)));

    private final AQLRepr repr3 = new AQLRepr(List.of(
            Filter.stringPropertyFilter("displayName", Filter.StringOperator.REGEX, "20")));

    private final java.util.List<AQLRepr> reprs = Arrays.asList(repr1, repr2, repr3);

    private final String db = "db-1";

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
                                          arangoDatabaseFactory);

        given(arangoDatabaseFactory.getArangoDriver()).willReturn(arangoDB);
        given(arangoDB.db(db)).willReturn(arangoDatabase);
        given(arangoDatabase.query(any(), any(), any(), any())).willReturn(arangoCursor);
        given(arangoCursor.asListRemaining()).willReturn(Arrays.asList("1", "2"));
    }

    @Test
    public void testSearchEntityOidsWithException() {
        given(arangoDB.db(any())).willThrow(new RuntimeException());

        final Either<Throwable, Collection<String>> result =
                        searchHandler.searchEntityOids(reprs, db);

        assertTrue("The result should be an exception", result.isLeft());
    }

    @Test
    public void testSearchEntityOids() {
        searchHandler.searchEntityOids(reprs, db);

        verify(arangoDB, atLeast(1)).db(db);

        ArgumentCaptor<String> queriesArg = ArgumentCaptor.forClass(String.class);
        verify(arangoDatabase, atLeast(1)).query(queriesArg.capture(), any(), any(), any());
        final String queriesToArangoDB = queriesArg.getAllValues().toString();

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

        final Either<Throwable, Collection<ServiceEntityRepoDTO>> result =
                        searchHandler.searchEntities(reprs, db);

        assertTrue("The result should be an exception", result.isLeft());
    }

    @Test
    public void testSearchEntities() {
        searchHandler.searchEntities(reprs, db);

        verify(arangoDB, atLeast(1)).db(db);

        ArgumentCaptor<String> queriesArg = ArgumentCaptor.forClass(String.class);
        verify(arangoDatabase, atLeast(1)).query(queriesArg.capture(), any(), any(), any());
        final String queriesToArangoDB = queriesArg.getAllValues().toString();

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
    }

    private void shouldContain(@Nonnull final String content,
                               @Nonnull final String subString) {
        assertTrue("The query " + content +" should contain a substring of " + subString,
                   content.contains(subString));
    }
}