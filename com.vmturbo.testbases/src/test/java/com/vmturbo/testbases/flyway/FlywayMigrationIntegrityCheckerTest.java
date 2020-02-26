package com.vmturbo.testbases.flyway;

import static com.vmturbo.testbases.flyway.FlywayMigrationIntegrityChecker.MIGRATION_TEST_BASE_COMMIT;
import static com.vmturbo.testbases.flyway.Whitelist.ViolationType.CHANGE;
import static com.vmturbo.testbases.flyway.Whitelist.ViolationType.DELETE;
import static com.vmturbo.testbases.flyway.Whitelist.ViolationType.INSERT;
import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyString;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.Writer;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.common.base.Functions;
import com.google.common.collect.ImmutableList;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.lib.Constants;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.revwalk.RevTree;
import org.eclipse.jgit.treewalk.TreeWalk;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.io.support.ResourcePatternResolver;

/**
 * Test class for {@link FlywayMigrationIntegrityCheckerTest}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({RevCommit.class})
public class FlywayMigrationIntegrityCheckerTest {

    private static final String TREE_RESOURCES_ROOT = "trees/";
    private static final String WHITELIST_PROPERTIES_PATH_IN_MODULE =
            "com.vmturbo.testbases/src/test/resources" +
                    "/com/vmturbo/testbases/flyway/flywayMigrationWhitelist.yaml";
    private static final String EMPTY_BLOB_SIGNATURE = "e69de29bb2d1d6434b8b29ae775ad8c2e48c5391";

    /**
     * Temporary folder is used to create and test git repos.
     */
    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    /**
     * This allows us to maniuplate environment variables for testing the user-specified base
     * commit feature.
     */
    @Rule
    public EnvironmentVariables environmentVariables = new EnvironmentVariables();

    /**
     * Test that the normal constructor used in migration tests is able to cons a working
     * {@link org.eclipse.jgit.lib.Repository} object and do something simple with it.
     *
     * <p>Most of the rest of the tests here use a git repo builtin the temp folder.</p>
     *
     * @throws IOException     if there's a problem accessing git files
     * @throws GitAPIException if there's a JGit API problem
     */
    @Test
    public void testCanConnectToRealRepo() throws IOException, GitAPIException {
        final FlywayMigrationIntegrityChecker checker = new FlywayMigrationIntegrityChecker(
                "com.vmturbo.testbases",
                ImmutableList.of("src/test/resources"),
                ImmutableList.of("db/migration"),
                null,
                null);
        final RevTree tree = checker.repo.parseCommit(checker.repo.resolve(Constants.HEAD)).getTree();
        try (TreeWalk walk = TreeWalk.forPath(checker.repo, WHITELIST_PROPERTIES_PATH_IN_MODULE, tree)) {
            // If this succeeds it means JGit found a file for us in the current branch head.
            // It's our own file, so it should definitely be there.
            assertTrue(walk != null && walk.getPathString().equals(WHITELIST_PROPERTIES_PATH_IN_MODULE));
        }
        assertTrue(checker.whitelist.isEmpty());
    }

    /**
     * Same as prior {@link #testCanConnectToRealRepo()} but with a whitelist.
     *
     * @throws IOException     for git problems
     * @throws GitAPIException for JGit API problems
     */
    @Test
    public void testCanConnectToRealRepoWithWhitelist() throws IOException, GitAPIException {
        final FlywayMigrationIntegrityChecker checker = new FlywayMigrationIntegrityChecker(
                "com.vmturbo.testbases",
                ImmutableList.of("src/test/resources"),
                ImmutableList.of("db/migration"),
                getClass().getResource("flywayMigrationWhitelist.yaml"),
                null);
        final RevTree tree = checker.repo.parseCommit(checker.repo.resolve(Constants.HEAD)).getTree();
        try (TreeWalk walk = TreeWalk.forPath(checker.repo, WHITELIST_PROPERTIES_PATH_IN_MODULE, tree)) {
            // If this succeeds it means JGit found a file for us in the current branch head.
            // It's our own file, so it should definitely be there.
            assertTrue(walk != null && walk.getPathString().equals(WHITELIST_PROPERTIES_PATH_IN_MODULE));
        }
        assertFalse(checker.whitelist.isEmpty());
    }

    /**
     * Test that when we perform a valid migration evolution with checked-in git changes,
     * no violations are detected.
     *
     * @throws IOException     for git problems
     * @throws GitAPIException for JGit API problems
     */
    @Test
    public void testCleanNonViolatingScenario() throws IOException, GitAPIException {
        Git git = setupGit("proj");
        updateTree(git, "T1.ORIG");
        commitAll(git);
        updateTree(git, "T1.OK");
        commitAll(git);
        final FlywayMigrationIntegrityChecker checker = new FlywayMigrationIntegrityChecker(
                "proj", ImmutableList.of("src"), ImmutableList.of("db/migration"), null, null, git);
        assertTrue(checker.getAllViolations().isEmpty());
    }

    /**
     * Test that when we perform a valid migration evolution without checking in git changes,
     * no violations are detected.
     *
     * @throws IOException     for git problems
     * @throws GitAPIException for JGit API problems
     */
    @Test
    public void testDirtyNonViolatingScenario() throws IOException, GitAPIException {
        final Git git = setupGit("proj");
        updateTree(git, "T1.ORIG");
        commitAll(git);
        updateTree(git, "T1.OK");
        final FlywayMigrationIntegrityChecker checker = new FlywayMigrationIntegrityChecker(
                "proj", ImmutableList.of("src"), ImmutableList.of("db/migration"), null, null, git);
        assertTrue(checker.getAllViolations().isEmpty());
    }

    /**
     * Test that a scenario with a migration change detects a CHANGE violation.
     *
     * @throws IOException     for git problems
     * @throws GitAPIException for JGit API problems
     */
    @Test
    public void testChangeScenario() throws IOException, GitAPIException {
        final Git git = setupGit("proj");
        updateTree(git, "T1.ORIG");
        commitAll(git);
        updateTree(git, "T1.CHANGE");
        final FlywayMigrationIntegrityChecker checker = new FlywayMigrationIntegrityChecker(
                "proj", ImmutableList.of("src"), ImmutableList.of("db/migration"), null, null, git);
        final List<FlywayMigrationIntegrityException> changes = checker.getChangedMigrations();
        assertEquals(1, changes.size());
        assertTrue(changes.get(0).getMessage().contains("1.1"));
        FlywayMigrationIntegrityChecker.logger.info("Violation message: {}",
                changes.get(0).getMessage());
    }

    /**
     * Check that a whitelisted migration change does not produce a violation.
     *
     * <p>This test does not include signatures in the whitelist.</p>
     *
     * @throws IOException     for git problems
     * @throws GitAPIException for JGit API problems
     */
    @Test
    public void testChangeNoSignatureWhitelistedScenario() throws IOException, GitAPIException {
        final Git git = setupGit("proj");
        updateTree(git, "T1.ORIG");
        commitAll(git);
        updateTree(git, "T1.CHANGE");
        final FlywayMigrationIntegrityChecker checker = new FlywayMigrationIntegrityChecker(
                "proj", ImmutableList.of("src"), ImmutableList.of("db/migration"),
                new Whitelist.Builder().whitelist("1.1", CHANGE).build(), null, git);
        final List<FlywayMigrationIntegrityException> changes = checker.getChangedMigrations();
        assertEquals(0, changes.size());
    }

    /**
     * Check that a whitelisted change does not result in a violation.
     *
     * <p>In this case the whitelist is restricted to specific content signatures, and our prior
     * content is covered..</p>
     *
     * @throws IOException     for git problems
     * @throws GitAPIException for JGit API problems
     */
    @Test
    public void testChangeGoodSignatureWhitelistedScenario() throws IOException, GitAPIException {
        final Git git = setupGit("proj");
        updateTree(git, "T1.ORIG");
        commitAll(git);
        updateTree(git, "T1.CHANGE");
        final FlywayMigrationIntegrityChecker checker = new FlywayMigrationIntegrityChecker(
                "proj", ImmutableList.of("src"), ImmutableList.of("db/migration"),
                new Whitelist.Builder().whitelist("1.1", CHANGE, EMPTY_BLOB_SIGNATURE).build(),
                null, git);
        final List<FlywayMigrationIntegrityException> changes = checker.getChangedMigrations();
        assertEquals(0, changes.size());
    }

    /**
     * Test that a non-whitelisted change causes a violation.
     *
     * <p>In this case we have a whitelist entry that would cover the change if our prior content
     * had the correct signature, but it does not.</p>
     *
     * @throws IOException     for git problems
     * @throws GitAPIException for JGit API problems
     */
    @Test
    public void testChangeBadSignatureWhitelistedScenario() throws IOException, GitAPIException {
        final Git git = setupGit("proj");
        updateTree(git, "T1.ORIG");
        commitAll(git);
        updateTree(git, "T1.CHANGE");
        final FlywayMigrationIntegrityChecker checker = new FlywayMigrationIntegrityChecker(
                "proj", ImmutableList.of("src"), ImmutableList.of("db/migration"),
                new Whitelist.Builder().whitelist("1.1", CHANGE, "xxx").build(), null, git);
        final List<FlywayMigrationIntegrityException> changes = checker.getChangedMigrations();
        assertEquals(1, changes.size());
        assertTrue(changes.get(0).getMessage().contains("1.1"));
    }

    /**
     * Test that a change violation is detected despite renaming of the migration files.
     *
     * <p>We rename two migration files, but only one of them has changed content.</p>
     *
     * @throws IOException     for git problems
     * @throws GitAPIException for JGit API problems
     */
    @Test
    public void testChangeWithRenamedFiles() throws IOException, GitAPIException {
        final Git git = setupGit("proj");
        updateTree(git, "T1.ORIG");
        commitAll(git);
        updateTree(git, "T1.CHANGE_AND_RENAME");
        final FlywayMigrationIntegrityChecker checker = new FlywayMigrationIntegrityChecker(
                "proj", ImmutableList.of("src"), ImmutableList.of("db/migration"), null, null, git);
        final List<FlywayMigrationIntegrityException> changes = checker.getChangedMigrations();
        assertEquals(1, changes.size());
        assertTrue(changes.get(0).getMessage().contains("1.1"));
        assertTrue(changes.get(0).getMessage().contains("formerly"));
        // for additional info in the test log
        FlywayMigrationIntegrityChecker.logger.info("Violation message: {}",
                changes.get(0).getMessage());
    }

    /**
     * Test that colliding migration files yield violations.
     *
     * @throws IOException     for git problems
     * @throws GitAPIException for JGit API problems
     */
    @Test
    public void testCollideScenario() throws IOException, GitAPIException {
        final Git git = setupGit("proj");
        updateTree(git, "T1.ORIG");
        commitAll(git);
        updateTree(git, "T1.COLLIDE");
        final FlywayMigrationIntegrityChecker checker = new FlywayMigrationIntegrityChecker(
                "proj", ImmutableList.of("src"), ImmutableList.of("db/migration"), null, null, git);
        final List<FlywayMigrationIntegrityException> collisions = checker.getCollidingMigrations();
        assertEquals(1, collisions.size());
        assertTrue(collisions.get(0).getMessage().contains("1.1"));
        // for additional info in the test log
        FlywayMigrationIntegrityChecker.logger.info("Violation message: {}",
                collisions.get(0).getMessage());
    }

    /**
     * Check that change violations are not produced for versions that have collision violations.
     *
     * @throws IOException     for git problems
     * @throws GitAPIException for JGit API problems
     */
    @Test
    public void testCollisionsSuppressChangeViolations() throws IOException, GitAPIException {
        final Git git = setupGit("proj");
        updateTree(git, "T1.ORIG");
        commitAll(git);
        updateTree(git, "T1.COLLIDE");
        commitAll(git);
        // here we have collisions in working tree
        FlywayMigrationIntegrityChecker checker = new FlywayMigrationIntegrityChecker(
                "proj", ImmutableList.of("src"), ImmutableList.of("db/migration"), null, null, git);
        List<FlywayMigrationIntegrityException> changes = checker.getChangedMigrations();
        assertFalse(checker.baseMigrations.asMap().values().stream().anyMatch(v -> v.size() > 1));
        assertTrue(checker.currentMigrations.asMap().values().stream().anyMatch(v -> v.size() > 1));
        assertTrue(changes.isEmpty());
        // now get rid of that collision in working tree, but we'll have a collision in the base
        updateTree(git, "T1.DELETE");
        commitAll(git);
        checker = new FlywayMigrationIntegrityChecker(
                "proj", ImmutableList.of("src"), ImmutableList.of("db/migration"), null, null, git);
        changes = checker.getChangedMigrations();
        assertTrue(checker.baseMigrations.asMap().values().stream().anyMatch(v -> v.size() > 1));
        assertFalse(checker.currentMigrations.asMap().values().stream().anyMatch(v -> v.size() > 1));
        assertTrue(changes.isEmpty());
    }

    /**
     * Test that improper migration insert produces a violation.
     *
     * @throws IOException     for git problems
     * @throws GitAPIException for JGit API problems
     */
    @Test
    public void testInsertScenario() throws IOException, GitAPIException {
        final Git git = setupGit("proj");
        updateTree(git, "T1.ORIG");
        commitAll(git);
        updateTree(git, "T1.INSERT");
        final FlywayMigrationIntegrityChecker checker = new FlywayMigrationIntegrityChecker(
                "proj", ImmutableList.of("src"), ImmutableList.of("db/migration"), null, null, git);
        final List<FlywayMigrationIntegrityException> insertions = checker.getMigrationInsertions();
        assertEquals(1, insertions.size());
        assertTrue(insertions.get(0).getMessage().contains("1.1.1"));
        // for additional info in the test log
        FlywayMigrationIntegrityChecker.logger.info("Violation message: {}",
                insertions.get(0).getMessage());
    }

    /**
     * Test that a whitelisted insertion does not produce a violation.
     *
     * @throws IOException     for git problems
     * @throws GitAPIException for JGit API problems
     */
    @Test
    public void testWhitelistedInsertScenario() throws IOException, GitAPIException {
        final Git git = setupGit("proj");
        updateTree(git, "T1.ORIG");
        commitAll(git);
        updateTree(git, "T1.INSERT");
        final FlywayMigrationIntegrityChecker checker = new FlywayMigrationIntegrityChecker(
                "proj", ImmutableList.of("src"), ImmutableList.of("db/migration"),
                new Whitelist.Builder().whitelist("1.1.1", INSERT).build(), null, git);
        final List<FlywayMigrationIntegrityException> insertions = checker.getMigrationInsertions();
        assertEquals(0, insertions.size());
    }

    /**
     * Test that deleting a migration produces a violation.
     *
     * @throws IOException     for git problems
     * @throws GitAPIException for JGit API problems
     */
    @Test
    public void testDeletionScenario() throws IOException, GitAPIException {
        final Git git = setupGit("proj");
        updateTree(git, "T1.ORIG");
        commitAll(git);
        updateTree(git, "T1.DELETE");
        final FlywayMigrationIntegrityChecker checker = new FlywayMigrationIntegrityChecker(
                "proj", ImmutableList.of("src"), ImmutableList.of("db/migration"), null, null, git);
        final List<FlywayMigrationIntegrityException> deletions = checker.getMigrationDeletions();
        assertEquals(1, deletions.size());
        assertTrue(deletions.get(0).getMessage().contains("1.1"));
        FlywayMigrationIntegrityChecker.logger.info("Violation message: {}",
                deletions.get(0).getMessage());
    }

    /**
     * Test that a whitelisted deletion does not produce a violation.
     *
     * @throws IOException     for git problems
     * @throws GitAPIException for JGit API problems
     */
    @Test
    public void testWhitelistedDeletionScenario() throws IOException, GitAPIException {
        final Git git = setupGit("proj");
        updateTree(git, "T1.ORIG");
        commitAll(git);
        updateTree(git, "T1.DELETE");
        final FlywayMigrationIntegrityChecker checker = new FlywayMigrationIntegrityChecker(
                "proj", ImmutableList.of("src"), ImmutableList.of("db/migration"),
                new Whitelist.Builder().whitelist("1.1", DELETE).build(), null, git);
        final List<FlywayMigrationIntegrityException> deletions = checker.getMigrationDeletions();
        assertEquals(0, deletions.size());
    }

    /**
     * Test that a user-specified base commit supplied by system property is honored.
     *
     * @throws IOException     for git problems
     * @throws GitAPIException for JGit API problems
     */
    @Test
    public void testUserBaseWorksWithProperty() throws IOException, GitAPIException {
        final Git git = setupGit("proj");
        updateTree(git, "T1.ORIG");
        final RevCommit base = commitAll(git);
        updateTree(git, "T1.DELETE");
        commitAll(git);
        updateTree(git, "T1.OK");
        commitAll(git);
        try {
            System.setProperty(MIGRATION_TEST_BASE_COMMIT, base.getName());
            final FlywayMigrationIntegrityChecker checker = new FlywayMigrationIntegrityChecker(
                    "proj", ImmutableList.of("src"), ImmutableList.of("db/migration"), null, null, git);
            assertEquals(base, checker.baseCommit);
        } finally {
            System.clearProperty(MIGRATION_TEST_BASE_COMMIT);
        }
    }

    /**
     * Test that a user-supplied base commit supplied by environment variable is honored.
     *
     * @throws IOException     for git problems
     * @throws GitAPIException for JGit API problems
     */
    @Test
    public void testUserBaseWorksWithEnv() throws IOException, GitAPIException {
        final Git git = setupGit("proj");
        updateTree(git, "T1.ORIG");
        final RevCommit base = commitAll(git);
        updateTree(git, "T1.DELETE");
        commitAll(git);
        updateTree(git, "T1.OK");
        commitAll(git);
        environmentVariables.set(MIGRATION_TEST_BASE_COMMIT, base.getName());
        final FlywayMigrationIntegrityChecker checker = new FlywayMigrationIntegrityChecker(
                "proj", ImmutableList.of("src"), ImmutableList.of("db/migration"), null, null, git);
        assertEquals(base, checker.baseCommit);
    }

    /**
     * Test that in the absence of user-specified base or automated build scenario, the base
     * commit for a clean repo is HEAD^.
     *
     * @throws IOException     for git problems
     * @throws GitAPIException for JGit API problems
     */
    @Test
    public void testCleanBaseCommit() throws IOException, GitAPIException {
        final Git git = setupGit("proj");
        updateTree(git, "T1.ORIG");
        commitAll(git);
        updateTree(git, "T1.DELETE");
        final RevCommit base = commitAll(git);
        updateTree(git, "T1.OK");
        commitAll(git);
        final FlywayMigrationIntegrityChecker checker = new FlywayMigrationIntegrityChecker(
                "proj", ImmutableList.of("src"), ImmutableList.of("db/migration"), null, null, git);
        assertEquals(base, checker.baseCommit);
    }

    /**
     * Test that if a base branch exists (a branch named `xxx-base` when current branch is `xxx`),
     * the base branch HEAD is used as the base commit.
     *
     * @throws IOException     for git problems
     * @throws GitAPIException for JGit API problems
     */
    @Test
    public void testBranchNameBaseCommit() throws IOException, GitAPIException {
        final Git git = setupGit("proj");
        commitAll(git);
        git.branchCreate().setName("xxx-base").call();
        git.branchCreate().setName("xxx").call();
        git.checkout().setName("xxx").call();
        final Repository repo = git.getRepository();
        updateTree(git, "T1.ORIG");
        commitAll(git);
        updateTree(git, "T1.DELETE");
        commitAll(git);
        updateTree(git, "T1.OK");
        commitAll(git);
        final FlywayMigrationIntegrityChecker checker = new FlywayMigrationIntegrityChecker(
                "proj", ImmutableList.of("src"), ImmutableList.of("db/migration"), null, null, git);
        assertEquals(repo.parseCommit(repo.resolve("xxx-base")), checker.baseCommit);
    }

    /**
     * Test that in an automated CI build, the most recent matching ci-tagged commit is the base
     * commit.
     *
     * @throws IOException     for git problems
     * @throws GitAPIException for JGit API problems
     */
    @Test
    public void testPriorCiBuildTagBaseCommit() throws IOException, GitAPIException {
        final Git git = setupGit("xl-xyzzy-ci-build");
        commitAll(git);
        final Repository repo = git.getRepository();
        updateTree(git, "T1.ORIG");
        commitAll(git, "xl-xyzzy-ci-20200215101010000");
        updateTree(git, "T1.DELETE");
        commitAll(git, "xl-xyzzy-ci-20200216101010000");
        updateTree(git, "T1.OK");
        commitAll(git);
        final FlywayMigrationIntegrityChecker checker = new FlywayMigrationIntegrityChecker(
                "proj", ImmutableList.of("src"), ImmutableList.of("db/migration"), null, null, git);
        assertEquals(repo.parseCommit(repo.resolve("xl-xyzzy-ci-20200216101010000")),
                checker.baseCommit);
    }

    /**
     * Test that in an automated nightly build, the most recent matching nightly-tagged commit
     * is the base commit.
     *
     * @throws IOException     for git problems
     * @throws GitAPIException for JGit API problems
     */
    @Test
    public void testPrioNightlyBuildBaeCommit() throws IOException, GitAPIException {
        final Git git = setupGit("xl-xyzzy-nightly-build");
        commitAll(git);
        final Repository repo = git.getRepository();
        updateTree(git, "T1.ORIG");
        commitAll(git, "xl-xyzzy-nightly-20200215101010000");
        updateTree(git, "T1.DELETE");
        commitAll(git, "xl-xyzzy-nightly-20200216101010000");
        updateTree(git, "T1.OK");
        commitAll(git);
        final FlywayMigrationIntegrityChecker checker = new FlywayMigrationIntegrityChecker(
                "proj", ImmutableList.of("src"), ImmutableList.of("db/migration"), null, null, git);
        assertEquals(repo.parseCommit(repo.resolve("xl-xyzzy-nightly-20200216101010000")),
                checker.baseCommit);
    }

    /**
     * Test that in an automated CI build, the most recent matching ci-tagged commit is the base
     * commit.
     *
     * @throws IOException        for git problems
     * @throws GitAPIException    for JGit API problems
     * @throws URISyntaxException for GitlabApi problem
     */
    @Test
    public void testPriorCiBuildTagBaseCommitViaGitlab() throws IOException, GitAPIException, URISyntaxException {
        final GitlabMock gitlabMock = new GitlabMock();
        final Git git = setupGit("xl-xyzzy-ci-build");
        commitAll(git);
        final Repository repo = git.getRepository();
        updateTree(git, "T1.ORIG");
        gitlabMock.tagCommit(commitAll(git), "xl-xyzzy-ci-20200215101010000");
        updateTree(git, "T1.DELETE");
        gitlabMock.tagCommit(commitAll(git), "xl-xyzzy-ci-20200216101010000");
        updateTree(git, "T1.OK");
        commitAll(git);
        final FlywayMigrationIntegrityChecker checker = new FlywayMigrationIntegrityChecker(
                "proj", ImmutableList.of("src"), ImmutableList.of("db/migration"), null,
                gitlabMock.getMock(), git);
        assertEquals(gitlabMock.resolve("xl-xyzzy-ci-20200216101010000"), checker.baseCommit);
    }

    /**
     * Test that in an automated nightly build, the most recent matching nightly-tagged commit
     * is the base commit.
     *
     * @throws IOException        for git problems
     * @throws GitAPIException    for JGit API problems
     * @throws URISyntaxException for GitlabApi problem
     */
    @Test
    public void testPrioNightlyBuildBaeCommitViaGitlab() throws IOException, GitAPIException, URISyntaxException {
        final GitlabMock gitlabMock = new GitlabMock();
        final Git git = setupGit("xl-xyzzy-nightly-build");
        commitAll(git);
        final Repository repo = git.getRepository();
        updateTree(git, "T1.ORIG");
        gitlabMock.tagCommit(commitAll(git), "xl-xyzzy-nightly-20200215101010000");
        updateTree(git, "T1.DELETE");
        gitlabMock.tagCommit(commitAll(git), "xl-xyzzy-nightly-20200216101010000");
        updateTree(git, "T1.OK");
        commitAll(git);
        final FlywayMigrationIntegrityChecker checker = new FlywayMigrationIntegrityChecker(
                "proj", ImmutableList.of("src"), ImmutableList.of("db/migration"), null,
                gitlabMock.getMock(), git);
        assertEquals(gitlabMock.resolve("xl-xyzzy-nightly-20200216101010000"), checker.baseCommit);
    }

    /**
     * Test that by default, in a dirty working tree, the current branch HEAD is the base commit.
     *
     * @throws IOException     for git problems
     * @throws GitAPIException for JGit API problems
     */
    @Test
    public void testDirtyNoHeadBaseCommit() throws IOException, GitAPIException {
        final Git git = setupGit("proj");
        updateTree(git, "T1.ORIG");
        final FlywayMigrationIntegrityChecker checker = new FlywayMigrationIntegrityChecker(
                "proj", ImmutableList.of("src"), ImmutableList.of("db/migration"), null, null, git);
        assertNull(checker.baseCommit);
    }

    /**
     * Test that in various scenarios that yield a clean working tree without a HEAD or with
     * a HEAD that has no parent commit, the base commit is null, and there are no base
     * migrations.
     *
     * @throws IOException     for git problems
     * @throws GitAPIException for JGit API problems
     */
    @Test
    public void testCleanNoParentBaseCommit() throws IOException, GitAPIException {
        final Git git = setupGit("proj");
        FlywayMigrationIntegrityChecker checker = new FlywayMigrationIntegrityChecker(
                "proj", ImmutableList.of("src"), ImmutableList.of("db/migration"), null, null, git);
        assertNull(checker.baseCommit);
        assertTrue(checker.baseMigrations.isEmpty());
        updateTree(git, "T1.ORIG");
        checker = new FlywayMigrationIntegrityChecker(
                "proj", ImmutableList.of("src"), ImmutableList.of("db/migration"), null, null, git);
        assertNull(checker.baseCommit);
        assertTrue(checker.baseMigrations.isEmpty());
        commitAll(git);
        checker = new FlywayMigrationIntegrityChecker(
                "proj", ImmutableList.of("src"), ImmutableList.of("db/migration"), null, null, git);
        assertNull(checker.baseCommit);
        assertTrue(checker.baseMigrations.isEmpty());
    }

    /**
     * Test that version comparison works.
     */
    @Test
    public void testVersionComparison() {
        assertEquals(-1, FlywayMigrationIntegrityChecker.compareVersions("1.0", "1.1"));
        assertEquals(-1, FlywayMigrationIntegrityChecker.compareVersions("1.0.1", "1.1"));
        assertEquals(-1, FlywayMigrationIntegrityChecker.compareVersions("1.1", "1.1.1"));
        assertEquals(1, FlywayMigrationIntegrityChecker.compareVersions("1.1.1", "1.1"));
        assertEquals(0, FlywayMigrationIntegrityChecker.compareVersions("1.1", "1.1"));
        assertEquals(-1, FlywayMigrationIntegrityChecker.compareVersions("20200215101010000", "2020202015101010001"));
    }

    /**
     * Test that we throw an exception if we ever try to extract a version number from a filename
     * that does not match the migration filename pattern.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testCantGetVersionFromBadMigrationFileName() {
        FlywayMigrationIntegrityChecker.getMigrationVersion("no-version-here");
    }

    /**
     * Test an edge case of the migration directories list constructor.
     *
     * <p>This case will never occur, but it gets us to 100% code coverage.</p>
     */
    @Test
    public void testConcatDirsEmptyEdgeCase() {
        assertTrue(FlywayMigrationIntegrityChecker.concatDirs().isEmpty());
    }

    /**
     * Test that the ref-name trimmer works properly when handed something that doesn't look like
     * a ref name.
     */
    @Test
    public void testTrimRefNameNonRefEdgeCases() {
        assertEquals("xyzzy", FlywayMigrationIntegrityChecker.trimRefName("xyzzy"));
        assertEquals("xyzzy", FlywayMigrationIntegrityChecker.trimRefName("refs/xyzzy"));
    }

    /**
     * Test that adding advice to an unknown {@link FlywayMigrationIntegrityException} does nothing
     * if it's not of a type for which we actually have advice.
     */
    @Test
    public void testNoAdviceForUnrecognizedException() {
        FlywayMigrationIntegrityException e = new FlywayMigrationIntegrityException("oh no!") {
        };
        FlywayMigrationIntegrityChecker.addViolationAdvice(e);
        assertEquals("oh no!", e.getMessage());
    }

    /**
     * Test that in a build query, if the Gitlab API call to obtain tags fails, the test falls
     * back on the git HEAD parent as the base commit.
     *
     * @throws GitAPIException    if there's a problem with the JGit API
     * @throws IOException        if there's a problem with git files or with Gitlab API
     * @throws URISyntaxException if the Gitlab API request URI is malformed
     */
    @Test
    public void testBaseCommitIsParentWhenBuildTagsQueryFails() throws GitAPIException, IOException, URISyntaxException {
        final Git git = setupGit("xl-xyzzy-ci-build");
        final GitlabMock gitlabMock = new GitlabMock();
        commitAll(git);
        final Repository repo = git.getRepository();
        updateTree(git, "T1.ORIG");
        gitlabMock.tagCommit(commitAll(git), "xl-xyzzy-ci-20200215101010000");
        updateTree(git, "T1.DELETE");
        final RevCommit expectedBase = commitAll(git);
        updateTree(git, "T1.OK");
        commitAll(git);
        when(gitlabMock.getMock().getTags(anyString())).thenThrow(new IOException());
        final FlywayMigrationIntegrityChecker checker = new FlywayMigrationIntegrityChecker(
                "proj", ImmutableList.of("src"), ImmutableList.of("db/migration"), null,
                gitlabMock.getMock(), git);
        assertEquals(expectedBase, checker.baseCommit);
    }


    /**
     * Perform updates to the git working tree.
     *
     * <p>The tree is updated according to a specified resource tree. Files appearing in the
     * resource tree are copied to the working tree unless the filename begins with "-". In that
     * case, the files (with hyphens removed) are removed from the working tree.</p>
     *
     * <p>Working tree changes are not committed, but are added to the index.</p>
     *
     * @param git      JGit API access
     * @param treeName path name to desired working tree
     * @throws IOException     for git problems
     * @throws GitAPIException for JGit API problems
     */
    private void updateTree(Git git, String treeName) throws IOException, GitAPIException {
        ResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
        String prefix = getClass().getPackage().getName().replaceAll("[.]", "/") + "/" + (TREE_RESOURCES_ROOT + treeName);
        String resourceRootPath = resolver.getResource(prefix).getFile().getPath() + "/";
        for (Resource resource : resolver.getResources("classpath*:" + prefix + "/**/*")) {
            if (resource.exists() && !resource.getFile().isDirectory()) {
                String path = resource.getFile().getPath();
                if (path.startsWith(resourceRootPath)) {
                    path = path.substring(resourceRootPath.length());
                }
                final File dest = new File(git.getRepository().getWorkTree(), path);
                if (dest.getName().startsWith("-")) {
                    // file deletion
                    final File toDelete = new File(dest.getParentFile(), dest.getName().substring(1));
                    toDelete.delete();
                    git.rm().addFilepattern(new File(new File(path).getParentFile(), toDelete.getName()).getPath()).call();
                } else {
                    FileUtils.forceMkdirParent(dest);
                    try (Reader in = new InputStreamReader(resource.getInputStream());
                         Writer out = new FileWriter(dest)) {
                        IOUtils.copy(in, out);
                    }
                    git.add().addFilepattern(path).call();
                }
            }
        }
    }

    /**
     * Commit all working tree changes that have been added to the index, and optionally
     * apply tags to the resulting commit.
     *
     * @param git  JGit API access
     * @param tags tags to apply to the new commit
     * @return the new {@link RevCommit} object
     * @throws GitAPIException for JGit API problems
     */
    private RevCommit commitAll(Git git, String... tags) throws GitAPIException {
        final RevCommit commit = git.commit().setMessage("commit").call();
        for (String tag : tags) {
            git.tag().setName(tag).setObjectId(commit).call();
        }
        return commit;
    }

    /**
     * Create a new git repo at the given location.
     *
     * @param workingTreeName name of working tree, within which git directory will appear as <code>.git</code>.
     * @return JGit API access to new tree and repo
     * @throws GitAPIException for JGit API problems
     */
    private Git setupGit(final String workingTreeName) throws GitAPIException {
        // confusingly, this does not delete the temp directory, but only its contents
        tempFolder.delete();
        return Git.init().setDirectory(new File(tempFolder.getRoot(), workingTreeName)).call();
    }

    /**
     * Class to help in mocking GitlabApi.
     */
    private class GitlabMock {
        private GitlabApi gitlabApi = mock(GitlabApi.class);
        Map<String, RevCommit> tags = new HashMap<>();

        /**
         * Create a new instance and set up the mock to return correct tags.
         *
         * @throws IOException        won't happen in mock
         * @throws URISyntaxException won't happen in mock
         */
        GitlabMock() throws IOException, URISyntaxException {
            when(gitlabApi.getTags(anyString())).thenAnswer(new Answer<Map<String, String>>() {
                @Override
                public Map<String, String> answer(final InvocationOnMock invocation) throws Throwable {
                    String mustContain = (String)invocation.getArguments()[0];
                    return tags.keySet().stream()
                            .filter(tag -> tag.contains(mustContain))
                            .collect(Collectors.toMap(Functions.identity(), tag -> tags.get(tag).name()));
                }
            });
        }

        /**
         * Set up a tag for a given commit.
         *
         * @param commit the commit to tag
         * @param tag    the tag
         */
        public void tagCommit(RevCommit commit, String tag) {
            tags.put(tag, commit);
        }

        /**
         * Resolve a tag to a commit.
         *
         * @param tag the tag
         * @return the commit
         */
        public RevCommit resolve(final String tag) {
            return tags.get(tag);
        }

        /**
         * Get the GitlabApi mock.
         *
         * @return mock
         */
        public GitlabApi getMock() {
            return gitlabApi;
        }
    }
}
