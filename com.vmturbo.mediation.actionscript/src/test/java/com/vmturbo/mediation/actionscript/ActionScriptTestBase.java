package com.vmturbo.mediation.actionscript;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.Writer;
import java.net.URLDecoder;
import java.nio.file.Files;
import java.security.GeneralSecurityException;
import java.util.regex.Pattern;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.util.FileUtils;
import org.apache.logging.log4j.core.util.IOUtils;
import org.apache.sshd.server.SshServer;
import org.junit.Assert;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.io.support.ResourcePatternResolver;

public class ActionScriptTestBase extends Assert {

    private static final Logger logger = LogManager.getLogger(ActionScriptTestBase.class);

    private SshServer apacheSshd = null;
    private static SystemSshdLauncher systemSshd = null;

    protected int startApacheSshd(@Nonnull File fileSystemRoot) throws IOException, GeneralSecurityException {
        this.apacheSshd = new ApacheSshdBuilder()
            .setupSftp()
            .setupSftp()
            .chroot(fileSystemRoot.toPath())
            .build();
        return apacheSshd.getPort();
    }

    protected void stopApacheSshd() throws IOException {
        apacheSshd.stop();
    }

    protected static int startSystemSshd(String authKey) throws IOException {
        systemSshd = new SystemSshdLauncher(authKey);
        systemSshd.launch();
        return systemSshd.getPort();
    }

    protected static void stopSystemSshd() {
        systemSshd.close();
    }
    /**
     * Copy the file structure rooted at "remoteFiles" in resources relative to this package, to
     * the TempFolder object.
     *
     * @throws IOException
     */
    protected static void copyResouresToFileTree(String resourceName, String clsOrPkgName, File root) throws IOException {
        ResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
        String prefix = clsOrPkgName.replaceAll("[.]", "/") + "/"+resourceName;
        logger.info("Copying test files to {}", root);
        for (Resource resource : resolver.getResources("classpath*:" + prefix + "/**/*")) {
            if (resource.exists() && !resource.getFile().isDirectory()) {
                String path = resource.getFile().toString();
                // we have some tests that use script paths with funky characters in them, and those
                // funky characters don't work in resource names. So we %-encode them like in URLs
                // in the resource name, but here we create the files with funky chars intact
                String encodedPath = path.substring(path.indexOf(prefix) + prefix.length());
                String realPath = URLDecoder.decode(encodedPath, "UTF-8");
                File dest = new File(root, realPath);
                FileUtils.makeParentDirs(dest);
                try (Reader in = new InputStreamReader(resource.getInputStream())) {
                    try (Writer out = new FileWriter(dest)) {
                        IOUtils.copy(in, out);
                    }
                }
            }
        }
    }

    protected static void setOwnerExecutable(File root, Pattern fileNamePattern) throws IOException {
        Files.walk(root.toPath())
            .map(p->p.toFile())
            .filter(f->f.isFile() && fileNamePattern.matcher(f.getName()).matches())
            .forEach(file -> {
                file.setExecutable(true, true);
            });
    }

    protected static void setAllExecutable(File root, Pattern fileNamePattern) throws IOException {
        Files.walk(root.toPath())
            .map(p->p.toFile())
            .filter(f->f.isFile() && fileNamePattern.matcher(f.getName()).matches())
            .forEach(file -> {
                file.setExecutable(true, false);
            });
    }
}

