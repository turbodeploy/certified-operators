package com.vmturbo.mediation.client.bootstrap;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Collection;

/**
 * Mediation bootstrap has it's aim to launch the application (mediation client) with the class
 * loader separate from system class loader. To achieve this, the jar containing this class
 * should be the only jar in the class path of JVM on startup. During startup it create a secondary
 * class loader with the contents of lib/ directory.
 * Tha main value of this routine is to create a system class loader, not holding any project
 * dependencies in order to isolate the project's dependencies from SDK probe class loader.
 * System class loader -> SDK probe class loader
 * |
 * XL component class loader
 *
 * @see {@link com.vmturbo.platform.sdk.common.util.MultiparentClassLoader}
 */
public class MediationBootstrap {

    /**
     * Class that is an entry point to the mediation client.
     */
    private static final String MEDIATION_MAIN_CLASS =
            "com.vmturbo.mediation.client.MediationComponentMain";
    /**
     * Directory where mediation component's jars are located.
     */
    private static final String LIB_DIR = "lib";

    /**
     * Starts everytiong up.
     *
     * @param args arguments to be passed to mediation common.
     */
    public static void main(String[] args) {
        try {
            final ClassLoader appClassLoader = createAppClassLoader();
            Thread.currentThread().setContextClassLoader(appClassLoader);
            final Class<?> mainClass = getMainClass(appClassLoader);
            executeMainMethod(mainClass, args);
        } catch (BootstrapException e) {
            e.printStackTrace(System.err);
            System.exit(1);
        }
    }

    /**
     * Creates application's class loader from jars, contained in {@link #LIB_DIR}.
     *
     * @return class loader for application
     * @throws BootstrapException if class loader failed to instantiate
     */
    private static ClassLoader createAppClassLoader() throws BootstrapException {
        final File libDir = new File(LIB_DIR);
        final File[] contentsArray = libDir.listFiles();
        if (contentsArray == null) {
            System.err.println("Failed listing contents of directory " + libDir.getAbsolutePath());
            System.exit(1);
        }
        final Collection<URL> libraries = new ArrayList<>(contentsArray.length);
        for (File file : contentsArray) {
            if (!file.isFile()) {
                continue;
            }
            final URL url;
            try {
                url = file.toURI().toURL();
            } catch (MalformedURLException e) {
                throw new BootstrapException(
                        "Failed to convert file " + file.getAbsolutePath() + " to URL", e);
            }
            libraries.add(url);
        }
        final URLClassLoader classLoader =
                new URLClassLoader(libraries.toArray(new URL[libraries.size()]));
        return classLoader;
    }

    /**
     * Returns mediation client entry point class instance.
     *
     * @param classLoader application class loader, holding entry point's class
     * @return entry point class instance
     * @throws BootstrapException if class was not found.
     */
    private static Class<?> getMainClass(ClassLoader classLoader) throws BootstrapException {
        final Class<?> mainClass;
        try {
            mainClass = classLoader.loadClass(MEDIATION_MAIN_CLASS);
        } catch (ClassNotFoundException e) {
            throw new BootstrapException("Failed to load class " + MEDIATION_MAIN_CLASS, e);
        }
        return mainClass;
    }

    /**
     * Executes entry point (invoke it's main() method).
     *
     * @param mainClass class instance to invoke {@code main()} method of
     * @param args arguments to pass to main method
     * @throws BootstrapException if {@code main()} method is not found
     */
    private static void executeMainMethod(Class mainClass, String[] args)
            throws BootstrapException {
        final Method mainMethod;
        try {
            mainMethod = mainClass.getMethod("main", String[].class);
        } catch (NoSuchMethodException e) {
            throw new BootstrapException(
                    "Failed to locate main method of class " + mainClass.toString(), e);
        }
        try {
            mainMethod.invoke(null, (Object)args);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new BootstrapException(
                    "Failed to invoke main method on class " + MEDIATION_MAIN_CLASS, e);
        }
    }
}
