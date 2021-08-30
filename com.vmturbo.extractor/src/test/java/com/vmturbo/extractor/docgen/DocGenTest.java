package com.vmturbo.extractor.docgen;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isA;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyVararg;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.function.Supplier;

import joptsimple.AbstractOptionSpec;
import joptsimple.OptionException;

import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.extractor.docgen.DocGen.CmdLine;

/**
 * Test the {@link DocGen} class and its inner {@link CmdLine} class.
 */
public class DocGenTest {

    /**
     * Test that the command line correctly processes all options.
     */
    @Test
    public void testCommandLineParser() {
        // -h or --help should activate the help option, takes no arg
        CmdLine cmd = CmdLine.of("-h");
        assertThat(cmd.has(cmd.helpOption()), is(true));
        cmd = CmdLine.of("--help");
        assertThat(cmd.has(cmd.helpOption()), is(true));
        assertThat(cmd.has(cmd.docTreeOption()), is(false));

        // --doc-tree is present in all further cases, since it's required when --help is not used
        cmd = CmdLine.of("--doc-tree", "foo.yaml");
        assertThat(cmd.has(cmd.docTreeOption()), is(true));
        assertThat(cmd.has(cmd.helpOption()), is(false));

        assertThat(catchit(() -> CmdLine.of("--section", "--doc-tree", "foo.yaml")), isA(OptionException.class));
        assertThat(catchit(() -> CmdLine.of("--section", "EntityTable", "--section", "--doc-tree", "foo.yaml")),
                isA(OptionException.class));

        // --doc-tree option specifies a file that holds doc snippets, requires an argument
        // specifying the file path
        cmd = CmdLine.of("--doc-tree", "/a/b/c.yaml");
        assertThat(cmd.has(cmd.docTreeOption()), is(true));
        assertThat((cmd.hasArgument(cmd.docTreeOption())), is(true));
        assertThat(cmd.valueOf(cmd.docTreeOption()), is(new File("/a/b/c.yaml")));
        assertThat(catchit(() -> CmdLine.of("-doc-tree")), isA(OptionException.class));
    }

    /**
     * Test that if required --doc-tree option is missing, the parse fails. The option is only
     * required if --help is not present, which case is tested above.
     *
     * <p>We catch and inspect the exception to make sure it's complaining about the right thing,
     * which is why we don't use {@link Test#expected()} annotation for this test.</p>
     */
    @Test
    public void testThatMissingDocTreeOptionFails() {
        try {
            CmdLine.of(new String[0]);
            Assert.fail("Command line parse should have failed because --doc-tree is not present");
        } catch (OptionException e) {
            final String msg = e.getMessage();
            assertThat(msg, containsString("Missing required"));
            assertThat(msg, containsString("doc-tree"));
        }
    }

    /**
     * Test that --help action actually prints help.
     *
     * @throws IOException if there's a problem
     */
    @Test
    public void testHelpIsPrinted() throws IOException {
        CmdLine cmdLine = mock(CmdLine.class);
        DocGen docGen = spy(DocGen.class);
        doReturn(cmdLine).when(docGen).processArgs(anyVararg());
        doReturn(true).when(cmdLine).has(any(AbstractOptionSpec.class));
        docGen.gen("-h");
        verify(cmdLine).printHelp(any(PrintStream.class));
    }

    /**
     * Test that if --help option is absent, generation is invoked.
     *
     * @throws IOException if there's an issue
     */
    @Test
    public void testGeneratorIsInvoked() throws IOException {
        CmdLine cmdLine = mock(CmdLine.class);
        DocGen docGen = spy(DocGen.class);
        doReturn(cmdLine).when(docGen).processArgs(anyVararg());
        doReturn(false).when(cmdLine).has(any(AbstractOptionSpec.class));
        Generator generator = mock(Generator.class);
        doReturn(generator).when(docGen).getGenerator(cmdLine);
        docGen.gen();
        verify(generator).generate();
    }

    private Exception catchit(Supplier<?> code) {
        try {
            code.get();
            return null;
        } catch (Exception e) {
            return e;
        }
    }
}
