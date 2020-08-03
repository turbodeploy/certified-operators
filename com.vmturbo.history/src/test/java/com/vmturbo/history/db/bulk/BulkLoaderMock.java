package com.vmturbo.history.db.bulk;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.jooq.Record;
import org.jooq.Table;
import org.jooq.impl.DSL;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.vmturbo.history.db.VmtDbException;
import com.vmturbo.history.db.bulk.BulkInserterFactory.TableOperation;

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
        try {
            doAnswer(getTransientLoader).when(mock).getTransientLoader(
                    isA(Table.class), isA(TableOperation.class));
        } catch (SQLException | InstantiationException | VmtDbException | IllegalAccessException e) {
        }
        return mock;
    }

    /**
     * Mock {@link Answer} that obtains a new mocked {@link BulkLoader} from this mocked factory.
     */
    private Answer<BulkLoader<?>> getLoader = invocation -> {
        Table<?> table = invocation.getArgumentAt(0, Table.class);
        return loaders.getLoader(table);
    };

    private Answer<BulkLoader<?>> getTransientLoader = new Answer<BulkLoader<?>>() {
        @Override
        public BulkLoader<?> answer(final InvocationOnMock invocation) throws Throwable {
            Table<?> table = invocation.getArgumentAt(0, Table.class);
            final String transName = table.getName() + "_transient_" + System.nanoTime();
            final Table<?> transTable = mock(Table.class);
            when(transTable.getName()).thenReturn(transName);
            doAnswer(inv -> table.field(inv.getArgumentAt(0, String.class)))
                    .when(transTable).field(anyString());
            dbMock.setTableKeys(transTable, dbMock.getTableKeys(table));
            TableOperation tableOp = invocation.getArgumentAt(1, TableOperation.class);
            tableOp.execute(transTable);
            return loaders.getLoader(transTable);
        }
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
            doAnswer(dbMock.insert).when(mock).insert(any());
            doAnswer(dbMock.insert).when(mock).insertAll(any());
            doReturn(table).when(mock).getInTable();
            doReturn(table).when(mock).getOutTable();
            return mock;
        }
    }
}
