package com.vmturbo.components.common.diagnostics;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.zip.ZipOutputStream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.PropertyPlaceholderConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.vmturbo.components.common.IVmtComponent;
import com.vmturbo.components.common.OsCommandProcessRunner;
import com.vmturbo.components.common.OsProcessFactory;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:diagnostic-service-test.xml")
public class DiagnosticServiceTest {

    private final Logger logger = LogManager.getLogger();

    @Autowired
    private IVmtComponent theComponent;

    @Autowired
    DiagnosticService diagnosticService;

    @Autowired
    OsProcessFactory mockProcessFactory;

    @Autowired
    OsCommandProcessRunner mockCommandProcessRunner;

    @Autowired
    FileFolderZipper mockFileFolderZipper;

    @Test
    public void testDumpDiags() throws Exception {

        logger.info("Testing dump diags with temp folder {}",
                PropertyTestConfiguration.diagnosticTempDir);

        // Arrange
        ZipOutputStream mockZipOutputStream = Mockito.mock(ZipOutputStream.class);

        DiagnosticService svc = spy(diagnosticService);
        doReturn(true).when(svc).getSystemDiags();
        when(theComponent.getComponentName()).thenReturn("component-a");

        // Act
        svc.dumpSystemDiags(mockZipOutputStream);

        // Assert
        verify(mockFileFolderZipper).zipFilesInFolder(PropertyTestConfiguration.testInstanceId,
                mockZipOutputStream, Paths.get("/tmp/diags/system-data"));

        // dumpDiags should clean up the temporary folder.
        assertThat(Files.exists(Paths.get("/tmp/diags/system-data")), is(false));
    }
}

@Configuration
class PropertyTestConfiguration {
    static String diagnosticTempDir;

    static String testInstanceId="test-instance-id";

    @Bean
    public PropertyPlaceholderConfigurer propertyPlaceholderConfigurer() throws IOException {
        final Path tempDir = Files.createTempDirectory("diags");
        diagnosticTempDir = tempDir.toString();

        Properties properties = new Properties();
        properties.setProperty("instance_id", testInstanceId);

        final PropertyPlaceholderConfigurer ppc = new PropertyPlaceholderConfigurer();
        ppc.setProperties(properties);
        return ppc;
    }
}
