package com.vmturbo.extractor.docgen;

import static java.util.Arrays.asList;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Collection;

import com.google.common.annotations.VisibleForTesting;

import joptsimple.AbstractOptionSpec;
import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

/**
 * Class to dump metadata information.
 *
 * <p>This is intended as a basis for generation of documentation that can be used by
 * internal developers as well as customers wishing to create custom reports.</p>
 */
public class DocGen {

    /**
     * Run the doc generator.
     *
     * @param args command line args - see {@link CmdLine} parser
     * @throws IOException if there are any I/O problems
     */
    public static void main(String[] args) throws IOException {
        new DocGen().gen(args);
    }

    @VisibleForTesting
    void gen(String... args) throws IOException {
        final CmdLine cmdLine;
        try {
            cmdLine = processArgs(args);
        } catch (OptionException e) {
            System.err.printf("Command line error: %s\n\n", e.getMessage());
            processArgs("-h").printHelp(System.err);
            return;
        }
        if (cmdLine.has(cmdLine.helpOption())) {
            cmdLine.printHelp(System.out);
        } else {
            getGenerator(cmdLine).generate();
        }
    }

    @VisibleForTesting
    Generator getGenerator(final CmdLine cmdLine) throws IOException {
        return new Generator(cmdLine);
    }

    @VisibleForTesting
    CmdLine processArgs(String... args) {
        return CmdLine.of(args);
    }


    /**
     * Command line parser for {@link DocGen}.
     */
    static class CmdLine {
        private final OptionParser parser;
        private final ArgumentAcceptingOptionSpec<File> docTreeOption;
        private final AbstractOptionSpec<Void> helpOption;
        private OptionSet opts;

        private CmdLine() {
            this.parser = new OptionParser();
            this.helpOption = parser.acceptsAll(asList("h", "help"), "print command line options")
                    .forHelp();
            this.docTreeOption = parser.accepts("doc-tree", "YAML file containing per-field documentation")
                    .requiredUnless(helpOption)
                    .withRequiredArg()
                    .ofType(File.class);
        }

        public static CmdLine of(final String... args) {
            CmdLine cmdLine = new CmdLine();
            cmdLine.parse(args);
            return cmdLine;
        }

        ArgumentAcceptingOptionSpec<File> docTreeOption() {
            return docTreeOption;
        }

        AbstractOptionSpec<Void> helpOption() {
            return helpOption;
        }

        void parse(String[] args) {
            this.opts = parser.parse(args);
        }

        void printHelp(final PrintStream out) throws IOException {
            parser.printHelpOn(out);
        }

        <T> T valueOf(final ArgumentAcceptingOptionSpec<T> option) {
            return opts.valueOf(option);
        }

        boolean has(final AbstractOptionSpec<?> option) {
            return opts.has(option);
        }

        boolean hasArgument(final ArgumentAcceptingOptionSpec<?> option) {
            return opts.hasArgument(option);
        }

        <T> Collection<T> valuesOf(final ArgumentAcceptingOptionSpec<T> option) {
            return opts.valuesOf(option);
        }
    }
}
