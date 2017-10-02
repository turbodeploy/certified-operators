package com.vmturbo.group.persistent;

import static org.mockito.BDDMockito.given;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;

import java.util.Collections;

import org.mockito.Mockito;

import com.arangodb.ArangoCollection;
import com.arangodb.ArangoCursor;
import com.arangodb.ArangoDB;
import com.arangodb.ArangoDatabase;

public class ArangoDBFixtures {
    public static ArangoDatabase getMockDatabase(final ArangoDB mockArangoDB,
                                                 final String database) {
        final ArangoDatabase mockDatabase = Mockito.mock(ArangoDatabase.class);
        given(mockArangoDB.db(database)).willReturn(mockDatabase);

        return mockDatabase;
    }

    public static ArangoCollection getMockCollection(final ArangoDB mockArangoDB,
                                                     final String database,
                                                     final String collection) {
        final ArangoDatabase mockDatabase = getMockDatabase(mockArangoDB, database);
        final ArangoCollection mockCollection = Mockito.mock(ArangoCollection.class);

        given(mockDatabase.collection(collection)).willReturn(mockCollection);

        return mockCollection;
    }

    public static <R> void givenGetDocumentWillReturn(final ArangoDB mockArangoDB,
                                                      final String database,
                                                      final String collection,
                                                      final String key,
                                                      final Class<R> klass,
                                                      final R testData) {

        final ArangoCollection mockCollection = getMockCollection(mockArangoDB, database, collection);
        given(mockCollection.getDocument(key, klass)).willReturn(testData);
    }

    public static <R> ArangoDatabase givenQueryWillReturn(final ArangoDB mockArangoDB,
                                                final String database,
                                                final String query,
                                                final Class<R> klass,
                                                final R testData) {
        final ArangoDatabase mockDatabase = getMockDatabase(mockArangoDB, database);
        @SuppressWarnings("unchecked")
        final ArangoCursor<R> mockCursor = Mockito.mock(ArangoCursor.class);

        when(mockCursor.asListRemaining()).thenReturn(Collections.singletonList(testData));
        when(mockDatabase.query(eq(query), anyMap(), eq(null), eq(klass)))
            .thenReturn(mockCursor);

        return mockDatabase;
    }

    public static void givenKeyExists(final ArangoCollection mockCollection, final String key) {
        given(mockCollection.documentExists(key)).willReturn(true);
    }

    public static void givenKeyDoesNotExist(final ArangoCollection mockCollection, final String key) {
        given(mockCollection.documentExists(key)).willReturn(false);
    }

    public static void givenInsertWillThrowException(final ArangoCollection mockCollection,
                                                     final Throwable e) {
        given(mockCollection.insertDocument(any())).willThrow(e);
    }

    public static void givenDeleteWillThrowException(final ArangoCollection mockCollection, final Throwable e) {
        given(mockCollection.deleteDocument(anyString())).willThrow(e);
    }
}
