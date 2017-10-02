package com.vmturbo.clustermgr.collectors;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;

import com.vmturbo.clustermgr.ClusterMgrService;
import com.vmturbo.proactivesupport.DataMetric;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

/**
 * The LogDataCollectorTest implements the log data collector tests.
 */
public class LogDataCollectorTest {
    @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

    private File file;

    private byte[] data = new byte[100];

    @Before
    public void setUp() throws IOException {
        file = new File(tempFolder.newFolder().getAbsolutePath() + "/file.tmp");
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte)i;
        }

        FileOutputStream out = new FileOutputStream(file);
        out.write(data);
        out.close();
    }

    @Test
    public void testLogDataCollectorProperties() {
        DataMetricLogs dataMetricLogs =
                new DataMetricLogs(Mockito.mock(ClusterMgrService.class));
        Assert.assertFalse(dataMetricLogs.isUrgent());
        Assert.assertEquals("logs.tar.gz", dataMetricLogs.getName());
    }

    @Test
    public void testLogInputStreamOpenClose() throws Exception {
        DataMetricLogs.LogInputStream in = new DataMetricLogs.LogInputStream(file);
        in.close();
        Assert.assertFalse(file.exists());
    }

    @Test
    public void testDataIntegrity() throws Exception {
        DataMetricLogs.LogInputStream in = new DataMetricLogs.LogInputStream(file);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IOUtils.copy(in, out);
        IOUtils.closeQuietly(in);
        IOUtils.closeQuietly(out);
        int i = 0;
        for (byte b : out.toByteArray()) {
            Assert.assertEquals(b, data[i++]);
        }
    }

    @Test
    public void testCollect() throws Exception {
        DataMetricLogs metric = new DataMetricLogs(Mockito.mock(ClusterMgrService.class));
        InputStream msg = metric.getData();
        Assert.assertEquals(-1, msg.read());
        Assert.assertEquals(DataMetricLogs.LogInputStream.class, msg.getClass());
        Assert.assertEquals(DataMetric.Severity.INFO, metric.getSeverity());
        msg.close();
    }
}
