package com.vmturbo.mediation.actionscript;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.Writer;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.util.FileCopyUtils;
import org.springframework.util.FileSystemUtils;
import org.springframework.util.SocketUtils;

public class SystemSshdLauncher {
    private static Logger logger = LogManager.getLogger(SystemSshdLauncher.class);

    private final String publicKey;
    private Process process;
    private File tempDir;
    private int port;

    public SystemSshdLauncher(String publicKey) {
        this.publicKey = publicKey;
    }


    public int launch() throws IOException {
        this.tempDir = Files.createTempDirectory(Paths.get(System.getProperty("user.home")), "testSshd").toFile();
        this.port = SocketUtils.findAvailableTcpPort();
        File hostKey = createHostKeyFile();
        File authKeys = createAuthKeysFile();
        File configFile = createSshdConfig(hostKey, authKeys);
        setPerms();
        logger.info("Running sshd on port {} in dir {}", port, tempDir);
        this.process = buildProcess(configFile);
        return port;
    }

    public int getPort() {
        return port;
    }

    public void close() {
        if (process != null) {
            logger.info("Killing sshd process");
            process.destroyForcibly();
            process = null;
        }
        if (tempDir != null && tempDir.exists()) {
            FileSystemUtils.deleteRecursively(tempDir);
        }
    }

    @Override
    protected void finalize() throws Throwable {
        if (process != null) {
            logger.warn("SystemSshdLauncher was not closed");
            close();
        }
    }

    private Process buildProcess(File configFile) throws IOException {
        ProcessBuilder builder = new ProcessBuilder("/usr/sbin/sshd", "-f", configFile.getAbsolutePath(), "-D", "-e")
            .inheritIO();
        Process proc = builder.start();
        boolean interrupted = false;
        do {
            try {
                Thread.sleep(1000L); // give the sshd little time to start before connections are requested
                interrupted = false;
            } catch (InterruptedException e) {
                interrupted = true; // try again if interrupted
            }
        } while (interrupted);
        return proc;
    }

    private File createHostKeyFile() throws IOException {
        final InputStream hostPrivateKeyStream = this.getClass().getResourceAsStream("sshd-host-key");
        File hostKeyFile = new File(tempDir, "sshd-host-key");
        hostKeyFile.getParentFile().mkdirs();
        FileCopyUtils.copy(hostPrivateKeyStream, new FileOutputStream(hostKeyFile));
        return hostKeyFile;
    }

    private File createAuthKeysFile() throws IOException {
        File authKeysFile = new File(tempDir, ".ssh/authorized_keys");
        authKeysFile.getParentFile().mkdirs();
        try (Writer out = new FileWriter(authKeysFile)) {
            out.write(publicKey);
        }
        return authKeysFile;
    }

    private File createSshdConfig(File hostKeyFile, File authKeysFile) throws IOException {
        File configFile = new File(tempDir, "sshd_config");
        configFile.getParentFile().mkdirs();
        try (PrintWriter out = new PrintWriter(new FileWriter(configFile))) {
            out.printf("Port %d\n", port);
            out.printf("HostKey %s\n", hostKeyFile.getAbsolutePath());
            out.printf("AuthorizedKeysFile %s\n", authKeysFile.getAbsolutePath());
            out.printf("PidFile %s/sshd.pid\n", tempDir.getAbsolutePath());
        }
        return configFile;
    }

    private static Set<PosixFilePermission> dirPerms = PosixFilePermissions.fromString("rwx------");
    private static Set<PosixFilePermission> filePerms = PosixFilePermissions.fromString("rw-------");

    private void setPerms() throws IOException {
        Files.walkFileTree(tempDir.toPath(), new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(final Path path, final BasicFileAttributes attrs) throws IOException {
                Files.setPosixFilePermissions(path, filePerms);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult preVisitDirectory(final Path dir, final BasicFileAttributes attrs) throws IOException {
                Files.setPosixFilePermissions(dir, dirPerms);
                return FileVisitResult.CONTINUE;
            }
        });
    }
}
