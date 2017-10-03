package com.vmturbo.group.persistent;

import static org.mockito.BDDMockito.given;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.mockito.Mockito;

import com.arangodb.ArangoCollection;
import com.arangodb.ArangoCursor;
import com.arangodb.ArangoDB;
import com.arangodb.ArangoDatabase;

public class ArangoDBFixtures {

    /**
     * Createa new {@link MockDatabase}.
     *
     * @param mockArangoDB The {@link ArangoDB} object, which should be a mock or a spy.
     * @param database The name of the database.
     * @return A {@link MockDatabase} containing an {@link ArangoDatabase} which will be returned
     *        by {@link ArangoDB#db(String)} with the provided database name. You can customize
     *        the behaviour of the {@link MockDatabase} by the various methods on it.
     */
    public static MockDatabase mockDatabase(final ArangoDB mockArangoDB, final String database) {
        return new MockDatabase(mockArangoDB, database);
    }

    /**
     * A wrapper around a mock Arango database that allows configuration of multiple
     * behaviours for a single {@link ArangoDatabase} object (e.g. return different things
     * for two different queries).
     */
    public static class MockDatabase {
        private ArangoDatabase arangoDatabase;
        private Map<String, ArangoCollection> collectionMap = new HashMap<>();

        private MockDatabase(final ArangoDB mockDb, final String database) {
            arangoDatabase = Mockito.mock(ArangoDatabase.class);
            given(mockDb.db(database)).willReturn(arangoDatabase);
        }

        public ArangoCollection createMockCollection(final String name) {
            return collectionMap.computeIfAbsent(name, n -> {
                final ArangoCollection mockCollection = Mockito.mock(ArangoCollection.class);
                given(arangoDatabase.collection(n)).willReturn(mockCollection);
                return mockCollection;
            });
        }

        public <R> MockDatabase givenGetDocumentWillReturn(final String collectionName,
                                                           final String key,
                                                           final Class<R> klass,
                                                           final R testData) {
            ArangoCollection collection = createMockCollection(collectionName);
            given(collection.getDocument(key, klass)).willReturn(testData);
            return this;
        }

        public <R> MockDatabase givenQueryWillReturn(final String query,
                                                     final Class<R> klass,
                                                     final R testData) {
            return givenQueryWillReturn(query, klass, Collections.singletonList(testData));
        }

        public <R> MockDatabase givenQueryWillReturn(final String query,
                                                     final Class<R> klass,
                                                     final List<R> testData) {
            @SuppressWarnings("unchecked")
            final ArangoCursor<R> mockCursor = Mockito.mock(ArangoCursor.class);

            when(mockCursor.asListRemaining()).thenReturn(testData);
            when(arangoDatabase.query(eq(query), anyMap(), eq(null), eq(klass)))
                    .thenReturn(mockCursor);

            return this;
        }

        public MockDatabase givenKeyExists(final String collectionName, final String key) {
            final ArangoCollection mockCollection = createMockCollection(collectionName);
            given(mockCollection.documentExists(key)).willReturn(true);
            return this;
        }

        public MockDatabase givenKeyDoesNotExist(final String collectionName, final String key) {
            final ArangoCollection mockCollection = createMockCollection(collectionName);
            given(mockCollection.documentExists(key)).willReturn(false);
            return this;
        }

        public MockDatabase givenInsertWillThrowException(final String collectionName,
                                                          final Throwable e) {
            final ArangoCollection mockCollection = createMockCollection(collectionName);
            given(mockCollection.insertDocument(any())).willThrow(e);
            return this;
        }

        public MockDatabase givenDeleteWillThrowException(final String collectionName,
                                                          final Throwable e) {
            final ArangoCollection mockCollection = createMockCollection(collectionName);
            given(mockCollection.deleteDocument(anyString())).willThrow(e);
            return this;
        }
    }
}
