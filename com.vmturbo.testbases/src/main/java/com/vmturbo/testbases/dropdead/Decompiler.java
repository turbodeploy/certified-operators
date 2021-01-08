package com.vmturbo.testbases.dropdead;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import com.strobel.decompiler.DecompilerSettings;
import com.strobel.decompiler.PlainTextOutput;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.reflections.Reflections;
import org.reflections.scanners.SubTypesScanner;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;

/**
 * Class to discover and decompile class files for use in scanning for DB object uses.
 */
public class Decompiler {
    private static final Logger logger = LogManager.getLogger();

    private static final DecompilerSettings settings = createDecompilerSettings();

    private static DecompilerSettings createDecompilerSettings() {
        DecompilerSettings settings = new DecompilerSettings();
        settings.setForceExplicitImports(true);
        return settings;
    }

    private final Map<String, String> decompiledClasses = new HashMap<>();
    private final List<String> discoveredClassNames = new ArrayList<>();

    /**
     * Create a new instance and discover classes to be scanned for usages.
     *
     * <p>Each package spec is either a package name (empty intermediate packages are permitted) or
     * a package name prefixed with a bang (!). The former are used to filter the initial set of
     * discoviered classes to contain only classes whose full names begin with one of the given
     * packages. The latter (prefixed with bang) are then used to exclude classes that match in the
     * same fashion.</p>
     *
     * <p>In addition, non-bang packages are used to provide starter URLs for class discovery.</p>
     *
     * <p>Typically, you will include your source package(s) without bangs, and the package into
     * which your jOOQ-generated classes appear with a bang. (Failure to exclude jOOQ-generated
     * classes will pretty much ensure that all your DB objects will appear to be live.</p>
     *
     * @param packageSpecs list of packages to include and exclude (latter prefixed with bang (!)
     * @throws IOException if there's an error in class discovery
     */
    public Decompiler(final Collection<String> packageSpecs) throws IOException {
        discoverClasses(packageSpecs);
        if (logger.isDebugEnabled()) {
            discoveredClassNames.forEach(c -> logger.debug("Discovered class {}", c));
        }
    }

    private void discoverClasses(Collection<String> packageSpecs) {
        List<Predicate<String>> includeFilters = new ArrayList<>();
        List<Predicate<String>> excludeFilters = new ArrayList<>();
        ConfigurationBuilder builder = new ConfigurationBuilder()
                .setScanners(new SubTypesScanner(false))
                .addUrls(ClasspathHelper.forClass(Enum.class))
                .setExpandSuperTypes(false);
        for (final String pkg : packageSpecs) {
            if (pkg.startsWith("!")) {
                excludeFilters.add(s -> s.startsWith(pkg.substring(1)));
            } else {
                builder.addUrls(ClasspathHelper.forPackage(pkg));
                includeFilters.add(s -> s.startsWith(pkg));
            }
        }
        new Reflections(builder).getAllTypes().stream()
                .filter(includeFilters.stream().reduce(s -> false, Predicate::or))
                .filter(excludeFilters.stream().reduce(s -> false, Predicate::or).negate())
                .forEach(discoveredClassNames::add);
    }

    /**
     * Decompile the given class.
     *
     * <p>Decompilation results are cached so a given class will only actaully be decompiled once
     * if requested multiple times.</p>
     *
     * @param className fully qualified class name
     * @return decompiled class as Java source code
     */
    public String decompile(String className) {
        if (!decompiledClasses.containsKey(className)) {
            logger.debug("Decompiling class {}", className);
            try {
                final PlainTextOutput output = new PlainTextOutput();
                com.strobel.decompiler.Decompiler.decompile(className, output, settings);
                decompiledClasses.put(className, output.toString());
            } catch (Exception e) {
                logger.error("Failed to decompile class {}; using empty content", className, e);
                decompiledClasses.put(className, "");
            }
        }
        return decompiledClasses.get(className);
    }

    /**
     * Return names of all the discovered classes.
     *
     * @return names of discovered classes
     */
    public Collection<String> discoveredClassNames() {
        return discoveredClassNames;
    }
}
