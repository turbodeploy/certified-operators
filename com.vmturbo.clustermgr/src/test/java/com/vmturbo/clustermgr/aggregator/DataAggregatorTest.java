package com.vmturbo.clustermgr.aggregator;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Calendar;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableList;

import com.vmturbo.proactivesupport.DataMetric;
import com.vmturbo.proactivesupport.DataMetricLOB;

/**
 * The DataAggregatorTest implements data aggregator tests.
 */
public class DataAggregatorTest {
    @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

    private static final String DATA = "Hello World!";

    private String dataAggregatorBasePath;

    @Before
    public void setup() throws IOException {
        File tempDir = tempFolder.newFolder();
        dataAggregatorBasePath = tempDir.getAbsolutePath();
    }

    @Test
    public void testBasePoint() {
        DataAggregator aggregator = new DataAggregator(dataAggregatorBasePath);
        Calendar calendar = Calendar.getInstance();
        String base = dataAggregatorBasePath + File.separator + calendar.get(Calendar.YEAR) + "_" +
                      calendar.get(Calendar.MONTH) + "_" +
                      calendar.get(Calendar.DAY_OF_MONTH) + "_" +
                      calendar.get(Calendar.HOUR_OF_DAY) + "_" +
                      calendar.get(Calendar.MINUTE);
        Assert.assertEquals(base, aggregator.getCurrentBase().getAbsolutePath());
    }

    @Test
    public void testPersistMessage() throws IOException {
        DataAggregator aggregator = new DataAggregator(dataAggregatorBasePath);
        aggregator.currentBase_ = new File(tempFolder.newFolder().getAbsolutePath());
        DataMetric msg = new TestDataCollectorMessage(false);
        aggregator.persistLOBMessage(msg);
        File[] files = aggregator.getCurrentBase().listFiles();
        Assert.assertNotNull(files);
        Assert.assertEquals(1, files.length);
        Assert.assertTrue(files[0].isFile());
        FileInputStream in = new FileInputStream(files[0]);
        byte[] data = new byte[in.available()];
        Assert.assertEquals(data.length, in.read(data));
        in.close();
        Assert.assertEquals(DATA, new String(data));
    }

    @Test
    public void testPersistMessageFailure() throws IOException {
        DataAggregator aggregator = new DataAggregator(dataAggregatorBasePath);
        aggregator.currentBase_ = new File(tempFolder.newFolder().getAbsolutePath());
        DataMetric msg = new TestDataCollectorMessage(true);
        aggregator.persistLOBMessage(msg);
        File[] files = aggregator.getCurrentBase().listFiles();
        Assert.assertNotNull(files);
        Assert.assertEquals(1, files.length);
        Assert.assertTrue(files[0].isFile());
        Assert.assertEquals(0, files[0].length());
    }

    @Test
    public void testCreateFileFailure() throws IOException {
        DataAggregator aggregator = new DataAggregator(dataAggregatorBasePath);
        aggregator.currentBase_ = new File("/dev/null");
        DataMetric msg = new TestDataCollectorMessage(true);
        aggregator.receiveLocalOffline(ImmutableList.of(msg));
        File msgFile = new File(aggregator.currentBase_ + "/" + msg.getName());
        Assert.assertFalse(msgFile.isFile());
    }

    @Test
    public void testReceiveOffline() throws IOException {
        // Use spy, as mock() will not invoke the underlying methods (from the method you invoked
        // directly)
        DataAggregator aggregator = spy(new DataAggregator(dataAggregatorBasePath));
        aggregator.currentBase_ = new File(tempFolder.newFolder().getAbsolutePath());
        // We will not be able to copy, as we are using the mocks for messages.
        doNothing().when(aggregator).persistLOBMessage((any()));
        aggregator.receiveLocalOffline(ImmutableList.of(Mockito.mock(DataMetric.class),
                                                        Mockito.mock(DataMetric.class)));
        verify(aggregator, times(2)).persistLOBMessage((any(DataMetric.class)));
    }

    private static class TestDataCollectorMessage extends DataMetricLOB {
        TestDataCollectorMessage(boolean throwOnRead) {
            super(MetricType.LOB, "test", new String[]{}, "help", Severity.INFO,
                  getStream(throwOnRead), true);
        }

        private static InputStream getStream(final boolean throwOnRead) {
            if (throwOnRead) {
                return new InputStream() {
                    @Override
                    public int read() throws IOException {
                        throw new IOException("Test read error");
                    }
                };
            }
            return new ByteArrayInputStream(DATA.getBytes());
        }
    }
}
