package com.vmturbo.history.db.bulk;

import static org.mockito.Matchers.anyCollectionOf;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

import java.util.HashMap;
import java.util.Map;

import org.jooq.Record;
import org.jooq.Table;
import org.mockito.stubbing.Answer;

/**
 * Class that creates mocks for {@link SimpleBulkLoaderFactory} that are backed by a {@link DbMock}
 * that will actively simulate limited DB operations.
 */
public class BulkLoaderMock {
    private final LoaderCache loaders = new LoaderCache();
    private final DbMock dbMock;

    /**
     * Create a new instance.
     *
     * @param dbMock the db mock that will simulate the database
     */
    public BulkLoaderMock(DbMock dbMock) {
        this.dbMock = dbMock;
    }

    /**
     * Create a new bulk loader factory mock.
     *
     * @return the new instance
     */
    @SuppressWarnings("unchecked")
    public SimpleBulkLoaderFactory getFactory() {
        SimpleBulkLoaderFactory mock = mock(SimpleBulkLoaderFactory.class);
        doAnswer(getLoader).when(mock).getLoader(isA(Table.class));
        return mock;
    }

    /**
     * Mock {@link Answer} that obtains a new mocked {@link BulkLoader} from this mocked factory.
     */
    private Answer<BulkLoader<?>> getLoader = invocation -> {
        Table<?> table = invocation.getArgumentAt(0, Table.class);
        return loaders.getLoader(table);
    };

    /**
     * Class to manage mock loaders delivered so far by this mock factory.
     */
    private class LoaderCache {
        private final Map<Table<?>, BulkLoader<?>> loaders = new HashMap<>();


        private <T extends Record> BulkLoader<T> getLoader(Table<T> table) throws InterruptedException {
            if (!loaders.containsKey(table)) {
                loaders.put(table, getMockLoader(table));
            }
            @SuppressWarnings("unchecked")
            final BulkLoader<T> loader = (BulkLoader<T>)loaders.get(table);
            return loader;
        }

        private <T extends Record> BulkLoader<T> getMockLoader(Table<T> table) throws InterruptedException {
            @SuppressWarnings("unchecked")
            BulkLoader<T> mock = mock(BulkLoader.class);
            doAnswer(dbMock.insert).when(mock).insert(isA(table.getRecordType()));
            @SuppressWarnings("unchecked")
            Class<T> tClass = (Class<T>)Record.class;
            doAnswer(dbMock.insert).when(mock).insertAll(anyCollectionOf(tClass));
            return mock;
        }
    }
}
