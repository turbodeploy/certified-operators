package com.vmturbo.testbases.flyway;

import static com.vmturbo.testbases.flyway.Whitelist.ViolationType.CHANGE;
import static com.vmturbo.testbases.flyway.Whitelist.ViolationType.DELETE;
import static com.vmturbo.testbases.flyway.Whitelist.ViolationType.INSERT;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableListMultimap.Builder;
import com.google.common.collect.ListMultimap;

import org.apache.commons.lang3.tuple.Triple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.lib.Constants;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.ObjectInserter;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.storage.file.FileRepositoryBuilder;
import org.eclipse.jgit.treewalk.TreeWalk;
import org.eclipse.jgit.util.FileUtils;

import com.vmturbo.testbases.flyway.FlywayMigrationIntegrityException.FlywayChangedMigrationException;
import com.vmturbo.testbases.flyway.FlywayMigrationIntegrityException.FlywayCollidingMigrationsException;
import com.vmturbo.testbases.flyway.FlywayMigrationIntegrityException.FlywayDeletedMigrationException;
import com.vmturbo.testbases.flyway.FlywayMigrationIntegrityException.FlywayInsertedMigrationException;
import com.vmturbo.testbases.flyway.Whitelist.ViolationType;

/**
 * This class implements integrity checks for Flyway migrations defined in the current git
 * branch for a given module.
 *
 * <p>See https://vmturbo.atlassian.net/browse/OM-55314 for a detailed description of checks
 * performed on the current working tree.</p>
 */
public class FlywayMigrationIntegrityChecker {
    static final Logger logger = LogManager.getLogger();

    static final Pattern BUILD_NAME_WORKING_DIR_PATTERN = Pattern.compile("(.*-(ci|nightly))-build");
    static final Pattern MIGRATION_FILE_PATTERN =
            Pattern.compile("V(\\d+([._]\\d+)*)__.+\\.(?i:sql|java)");
    static final String MIGRATION_TEST_BASE_COMMIT = "MIGRATION_TEST_BASE_COMMIT";
    static final String BASE_BRANCH_SUFFIX = "-base";

    // advice included in violation messages to give the developer some clues as to how to proceed
    static final String CHANGE_ADVICE = "Once committed, a migration should not be changed.\n" +
            "If this migration has been committed, please consider creating a follow-on " +
            "migration with a higher version to achieve your needs.\n" +
            "For other options see the Migration Guide.";
    static final String COLLIDE_ADVICE = "Flyway will always fail if presented with multiple " +
            "migrations with the same version number.\n" +
            "If one of these is a new migration you are introducing, please choose a higher version " +
            "number for your new migration.\n" +
            "Please see the Migration Guide for more details.";
    static final String INSERTION_ADVICE = "Inserting a migration into an existing sequence is risky because it will " +
            "be ignored in installations that have previously applied higher-versioned migrations.\n" +
            "If this is acceptable, you can correct this build failure by white-listing the " +
            "insertion.\n" +
            "Please see the Migration Guide for more details.";
    static final String DELETION_ADVICE = "Flyway will fail to process migrations if a previously applied migration " +
            "is no longer present.\n" +
            "If this is absolutely necessary, you will need to whitelist the deletion and " +
            "create a callback to alter the migration history at existing installations.\n" +
            "See the Migration Guide for details.";

    final String relativeModuleDir;
    final List<String> relativeSouceDirs;
    final List<String> relativeMigrationLocations;
    final Git git;
    final Repository repo;
    final RevCommit baseCommit;
    final ListMultimap<String, String> currentMigrations;
    final ListMultimap<String, String> baseMigrations;
    final Whitelist whitelist;
    final ObjectInserter inserter;
    final GitlabApi gitlabApi;

    // tag name <-> git hash
    final BiMap<String, String> taggedCommitsFromGitlab = HashBiMap.create();

    /**
     * Create a new instance.
     *
     * @param relativeModuleDir          path, relative to git root, to module to be checked
     * @param relativeSouceDirs          paths to source directories relative to module root
     * @param relativeMigrationLocations paths to directories containing migration files, relative
     *                                   to source directories
     * @param whitelistPropertiesUrl     resource URL of properties file defining whitelist
     * @param gitlabApi                  for Gitlab API access, if needed
     * @throws IOException     for errors working with git files
     * @throws GitAPIException for errors using JGit APIs
     */
    public FlywayMigrationIntegrityChecker(
            @Nonnull String relativeModuleDir,
            @Nonnull List<String> relativeSouceDirs,
            @Nonnull List<String> relativeMigrationLocations,
            @Nullable URL whitelistPropertiesUrl,
            @Nullable GitlabApi gitlabApi)
            throws IOException, GitAPIException {
        this(relativeModuleDir,
                relativeSouceDirs,
                relativeMigrationLocations,
                whitelistPropertiesUrl != null
                        ? Whitelist.fromResource(whitelistPropertiesUrl) : null,
                gitlabApi,
                null);
    }

    /**
     * Create a new instance for testing.
     *
     * <p>This constructor allows objects created by the regular constructor to be supplied by
     * tests, as mocks or other specially constructed instances.</p>
     *
     * @param relativeModuleDir          path, relative to git root, to module to be checked
     * @param relativeSouceDirs          paths to source directories relative to module root
     * @param relativeMigrationLocations paths to directories containing migration files, relative
     *                                   to source directories
     * @param whitelist                  whitelist structure, or null for no whitelist
     * @param gitlabApi                  for Gitlab API access, if needed
     * @param git                        a {@link Git} object, or null to compute it from repo
     * @throws IOException     if there's a problem with git file access
     * @throws GitAPIException if there's a problem with the JGit api
     */
    FlywayMigrationIntegrityChecker(
            @Nonnull String relativeModuleDir,
            @Nonnull List<String> relativeSouceDirs,
            @Nonnull List<String> relativeMigrationLocations,
            @Nullable Whitelist whitelist,
            @Nullable GitlabApi gitlabApi,
            @Nullable Git git)
            throws IOException, GitAPIException {
        this.relativeModuleDir = relativeModuleDir;
        this.relativeSouceDirs = relativeSouceDirs;
        this.relativeMigrationLocations = relativeMigrationLocations;
        this.whitelist = whitelist != null ? whitelist : Whitelist.empty();
        this.repo = git != null ? git.getRepository()
                : new FileRepositoryBuilder().readEnvironment().findGitDir().build();
        this.gitlabApi = gitlabApi;
        this.git = git != null ? git : new Git(this.repo);
        this.baseCommit = computeBaseCommit();
        this.currentMigrations = getCurrentMigrations();
        this.baseMigrations = this.baseCommit != null ? getBaseMigrations()
                : mapMigrations(Collections.emptyList());
        this.inserter = this.repo.newObjectInserter();
        logger.info("Git Repository located at {}; checking migrations in {}",
                this.repo.getDirectory(), this.relativeModuleDir);
        logger.info("Checking current migrations against those in commit {}",
                describeCommit(this.baseCommit));
    }

    /**
     * Create a multimap of migration versions to migration files, given a list of migration file
     * paths.
     *
     * <p>The multimap will always produce keys in proper migration version order. Path entries
     * for a given version appear in the order they appear in the paths list.</p>
     *
     * @param migrations migration file paths
     * @return multimap linking each migration version to the paths to which it applies
     */
    static ListMultimap<String, String> mapMigrations(List<String> migrations) {
        final Builder<String, String> builder = ImmutableListMultimap.<String, String>builder()
                .orderKeysBy(FlywayMigrationIntegrityChecker::compareVersions);
        for (String migration : migrations) {
            final String version = getMigrationVersion(migration);
            builder.put(version, migration);
        }
        return builder.build();
    }

    /**
     * Compute a base commit to be used for migration checks.
     *
     * <p>Migrations in the working tree are compared with those from the base commit when
     * performing integrity checks.</p>
     *
     * @return base commit for integrity checks
     * @throws IOException     if there's a problem with git file access
     * @throws GitAPIException if there's a problem with the JGit API
     */
    private RevCommit computeBaseCommit() throws IOException, GitAPIException {
        // highest priority is a commit identifed by environment variable
        final String userChoice = getUserBaseCommit();
        if (userChoice != null) {
            return repo.parseCommit(repo.resolve(userChoice));
        }
        // next is, if current branch is named 'xxx', the head of a branch named 'xxx-base', if
        // that exists
        final String branch = repo.getBranch();
        final ObjectId baseBranch = repo.resolve(branch + BASE_BRANCH_SUFFIX);
        final RevCommit baseBranchCommit = baseBranch != null ? repo.parseCommit(baseBranch) : null;
        if (baseBranchCommit != null) {
            return baseBranchCommit;
        }
        final ObjectId headRef = repo.resolve(Constants.HEAD);
        final RevCommit head = headRef != null ? repo.parseCommit(headRef) : null;
        if (!git.status().call().isClean()) {
            // next, if working tree is dirty, use current branch head as base commit
            return head;
        } else {
            // next up, if our working directory name matches what we see in automated
            // builds, try to find the latest matching build tag
            final String buildName = getBuildName();
            final RevCommit taggedBuild = buildName != null ? getLatestTaggedBuild(buildName) : null;
            if (taggedBuild != null) {
                return taggedBuild;
            }
        }
        // If nothing else worked, use the (first) parent of current commit
        final RevCommit parent = head != null
                ? head.getParents().length > 0 ? head.getParent(0) : null
                : null;
        return parent != null ? repo.parseCommit(parent) : null;
    }

    /**
     * Get the user specified base commit.
     *
     * <p>The base commit can be specified via either an environment variable or a system property.</p>
     *
     * @return user supplied base commit specification, or null if none was specified
     */
    @Nullable
    private String getUserBaseCommit() {
        final String envVar = System.getenv(MIGRATION_TEST_BASE_COMMIT);
        if (envVar != null) {
            return envVar;
        }
        return System.getProperty(MIGRATION_TEST_BASE_COMMIT);
    }

    /**
     * Get the build name from the working tree directory name, if it matches the pattern
     * we see in our automated builds.
     *
     * @return build name, or null if this is not an automated build
     */
    @Nullable
    private String getBuildName() {
        final Matcher m = BUILD_NAME_WORKING_DIR_PATTERN.matcher(repo.getWorkTree().getName());
        return m.matches() ? m.group(1) : null;
    }

    /**
     * Find the latest tag associated with the given build name.
     *
     * <p>Build tags are named with the build name followed by a hyphen and a 17-digit numeric
     * timestamp. This ensures if we sort the matching tags in reverse alphabetical order, the
     * latest tag will appear first.
     *
     * @param buildName build name appearing at the front of every tag
     * @return latest tag for this build name, or null if none is found
     * @throws GitAPIException if there's a problem with the JGit API
     */
    @Nullable
    private RevCommit getLatestTaggedBuild(String buildName) throws GitAPIException {
        ObjectId commit = git.tagList().call().stream()
                .filter(tag -> tag.getName().startsWith("refs/tags/" + buildName + "-"))
                .max(Comparator.comparing(Ref::getName))
                .map(Ref::getObjectId)
                .orElse(null);
        if (commit == null && gitlabApi != null) {
            try {
                Map<String, String> tags = gitlabApi.getTags(buildName + "-");
                saveTagsFromGitlab(tags);
                commit = tags.keySet().stream()
                        // reverse alphabetical means reverse chronological cuz tags are
                        // identical up to beginning of timestamp
                        .sorted((a, b) -> b.compareTo(a))
                        .map(name -> {
                            try {
                                return repo.resolve(tags.get(name));
                            } catch (IOException e) {
                                logger.warn("Failed to resolve tag target {}:{}",
                                        name, tags.get(name), e);
                                return null;
                            }
                        })
                        .filter(Objects::nonNull)
                        .findFirst()
                        .orElse(null);
            } catch (URISyntaxException | IOException e) {
                logger.warn("Failed to retrieve current build tags for build {} from GitLab",
                        buildName, e);
            }
        }
        if (commit != null) {
            try {
                return repo.parseCommit(commit);
            } catch (IOException e) {
                logger.warn("Failed to resolve object-id {} to a commit", commit.getName());
            }
        } else {
            logger.warn("No tags found for build {}", buildName);
        }
        return null;
    }

    /**
     * Save tags retrieved from GitLab when local repo has no tags.
     *
     * @param tags map of tag name -> commit id
     */
    private void saveTagsFromGitlab(final Map<String, String> tags) {
        taggedCommitsFromGitlab.clear();
        taggedCommitsFromGitlab.putAll(tags);
    }


    /**
     * Get a structure identifying all migrations available in the current working tree.
     *
     * @return multimap linking migration versions to associated migration file paths
     * @throws IOException if there's an issue with git files
     */
    private ListMultimap<String, String> getCurrentMigrations() throws IOException {
        List<String> dirs = getMigrationDirs(repo.getWorkTree());
        final DirectoryLister directoryLister =
                d -> {
                    final File dir = new File(d);
                    return dir.isDirectory()
                            ? Stream.of(dir.listFiles()).filter(File::isFile).map(File::getAbsolutePath)
                            .collect(Collectors.toList())
                            : Collections.emptyList();
                };
        List<String> migrationFiles = findMigrations(dirs, directoryLister).stream()
                .map(f -> FileUtils.relativizeNativePath(repo.getWorkTree().getAbsolutePath(), f))
                .collect(Collectors.toList());
        return mapMigrations(migrationFiles);
    }

    /**
     * Get a structure identeifying all migrations available in the base commit tree.
     *
     * @return multimap linking migration versions to associated migration file paths
     * @throws IOException if there's an error working with git files
     */
    private ListMultimap<String, String> getBaseMigrations() throws IOException {
        List<String> dirs = getMigrationDirs(null);
        List<String> migrationFiles = findMigrations(dirs, this::getBaseMigrationFiles);
        return mapMigrations(migrationFiles);
    }

    /**
     * Get migration files available in the given root directory.
     *
     * <p>The search does not recurse into directories.</p>
     *
     * @param root File formed from root path to be searched
     * @return list of file paths for the discovered migration files
     * @throws IOException if there's a problem with git files
     */
    private List<String> getBaseMigrationFiles(String root) throws IOException {
        List<String> migrationFiles = new ArrayList<>();
        try (TreeWalk walk = TreeWalk.forPath(repo, platformAgnosticPath(root), baseCommit.getTree())) {
            if (walk != null) {
                walk.enterSubtree();
                while (walk.next()) {
                    // TODO (low priority) figure out how to filter out directories
                    String file = walk.getPathString();
                    if (isFlywayMigrationName(file)) {
                        migrationFiles.add(file);
                    }
                }
            }
        }
        return migrationFiles;
    }

    /**
     * JGit doesn't like paths with backslashes. This method replaces backslashes (on Windows)
     * with forward slashes, and should be used for any path strings given to JGit methods/objects.
     *
     * @param path The path, potentially with backslashes (on Windows).
     * @return The path, with backslashes replaced by forward slashes.
     */
    @Nonnull
    public static String platformAgnosticPath(String path) {
        return path.replaceAll("\\\\", "/");
    }

    /**
     * Construct a list of all the directories to be searched for migrations.
     *
     * <p>all combinations of source directories and migration locations, stemming from the
     * module root directory, are ceated.</p>
     *
     * <p>The <code>root</code> parameter is null when constructing paths for a git tree.</p>
     *
     * @param root file representing path to root directory, may be null
     * @return list of constructed paths
     */
    private List<String> getMigrationDirs(@Nullable File root) {
        return concatDirs(
                Collections.singletonList(new File(root, relativeModuleDir).getParent()),
                relativeSouceDirs,
                relativeMigrationLocations);
    }

    /**
     * Interface for functions passed to {@link #findMigrations(List, DirectoryLister)},
     * defined explicity to permit throwing {@link IOException}.
     */
    @FunctionalInterface
    private interface DirectoryLister {
        List<String> listFiles(String dir) throws IOException;
    }

    /**
     * Find all migration files appearing in the given directories.
     *
     * @param dirs            directories to search
     * @param directoryLister a function that delivers relative paths for all files appearing in]
     *                        a given directory
     * @return all discovered migration files
     * @throws IOException if there's a problem with git files
     */
    private List<String> findMigrations(List<String> dirs, DirectoryLister directoryLister)
            throws IOException {
        List<String> result = new ArrayList<>();
        for (String dir : dirs) {
            for (String file : directoryLister.listFiles(dir)) {
                if (isFlywayMigrationName(file)) {
                    result.add(file);
                }
            }
        }
        return result;
    }

    /**
     * Parse the migration version from a migration file path.
     *
     * <p>This should only be used with paths that are known to match the migration file
     * naming pattern.</p>
     *
     * @param path migration file path
     * @return migration version
     */
    static String getMigrationVersion(String path) {
        final Matcher m = MIGRATION_FILE_PATTERN.matcher(new File(path).getName());
        if (m.matches()) {
            return m.group(1).replace("_", ".");
        } else {
            throw new IllegalArgumentException(
                    String.format("File name does not match migration file pattern (%s): %s",
                            MIGRATION_FILE_PATTERN.pattern(), path));
        }
    }

    /**
     * Compare migration version strings according to Flyway version ordering.
     *
     * @param v1 version string
     * @param v2 version string
     * @return comparision result: -1 if v1 < v2, 0 if v1 = v2, and 1 if v1 > v2
     */
    static int compareVersions(String v1, String v2) {
        final String[] parts1 = v1.split("\\.");
        final String[] parts2 = v2.split("\\.");
        int i = 0;
        while (i < parts1.length && i < parts2.length) {
            final int cmp = Long.compare(Long.parseLong(parts1[i]), Long.parseLong(parts2[i]));
            if (cmp != 0) {
                return cmp;
            }
            i += 1;
        }
        return Integer.compare(parts1.length, parts2.length);
    }

    /**
     * Test if the given path looks like that of a flyway migration file.
     *
     * @param path file path
     * @return true if it appears to be a flyway migration file
     */
    private boolean isFlywayMigrationName(String path) {
        return MIGRATION_FILE_PATTERN.matcher(new File(path).getName()).matches();
    }

    /**
     * Describe the given commit, for logging purposes.
     *
     * @param commit the commit to describe
     * @return desciption, including SHA signature, branch name if it's a branch head, and all associated tags
     * @throws IOException     if there's a problem with git files
     * @throws GitAPIException if there's a problem with the JGit API
     */
    private String describeCommit(RevCommit commit) throws GitAPIException, IOException {
        if (commit == null) {
            return "(no commit)";
        }
        final String sha = commit.abbreviate(10).name();
        String branches = getBranches(commit).stream()
                .map(Ref::getName)
                .map(FlywayMigrationIntegrityChecker::trimRefName)
                .collect(Collectors.joining(", "));
        branches = branches.isEmpty() ? "" : "; branches: [" + branches + "]";
        List<String> tagList = getTags(commit).stream()
                .map(Ref::getName)
                .map(FlywayMigrationIntegrityChecker::trimRefName)
                .collect(Collectors.toList());
        if (tagList.isEmpty()) {
            String tag = taggedCommitsFromGitlab.inverse().get(commit.name());
            if (tag != null) {
                tagList.add(tag);
            }
        }
        final String tags = tagList.isEmpty() ? "" : "; tags: [" + String.join(", ", tagList) + "]";
        return sha + branches + tags;
    }

    /**
     * Get the branch names of which the given commit is current HEAD.
     *
     * @param commit commit to check
     * @return branch names, if any
     * @throws GitAPIException if there's a problem with the JGit API
     * @throws IOException     if there's a problem with git files
     */
    private List<Ref> getBranches(RevCommit commit) throws GitAPIException, IOException {
        List<Ref> branches = new ArrayList<>();
        for (Ref branch : git.branchList().call()) {
            if (repo.parseCommit(branch.getObjectId()).equals(commit)) {
                branches.add(branch);
            }
        }
        return branches;
    }

    /**
     * Retrieve all the tags associated with given commit.
     *
     * @param commit a git commit
     * @return list of {@link Ref} objects for all tags associated with the commit
     * @throws GitAPIException if there's a problem with the JGit API
     * @throws IOException     if there's a problem with git files
     */
    private List<Ref> getTags(RevCommit commit) throws GitAPIException, IOException {
        List<Ref> result = new ArrayList<>();
        for (Ref tag : git.tagList().call()) {
            if (repo.parseCommit(tag.getObjectId()).equals(commit)) {
                result.add(tag);
            }
        }
        return result;
    }

    /**
     * Remove the 'refs/xxx/' prefix in a ref name.
     *
     * @param refName full ref name
     * @return trimmed ref name
     */
    static String trimRefName(String refName) {
        if (refName.startsWith("refs/")) {
            String name = refName.substring("refs/".length());
            return name.contains("/") ? name.substring(name.indexOf('/') + 1) : name;
        } else {
            return refName;
        }
    }

    /**
     * Compute all distinct concatenations of file segments.
     *
     * <p>Each result includes a value for each segment, selected from a list of choices
     * for that segment.</p>
     *
     * @param segmentOptions choices for each segment
     * @return list of all possible concatenations
     */
    @SafeVarargs
    static List<String> concatDirs(List<String>... segmentOptions) {
        if (segmentOptions.length == 0) {
            return Collections.emptyList();
        } else {
            List<String> result = segmentOptions[0];
            for (int i = 1; i < segmentOptions.length; i++) {
                final List<String> nextSegOpts = segmentOptions[i];
                List<String> newResult = new ArrayList<>();
                for (String head : result) {
                    for (String tail : nextSegOpts) {
                        newResult.add(new File(head, tail).getPath());
                    }
                }
                result = newResult;
            }
            return result;
        }
    }

    /**
     * Find migrations where the migration file has chnaged between the base commit and the
     * current working tree.
     *
     * <p>Whitelisted versions are ommitted, but changes are logged.</p>
     *
     * @return list of exceptions for changed migrations
     * @throws IOException if there's a problem with git files
     */
    List<FlywayMigrationIntegrityException> getChangedMigrations() throws IOException {
        Set<String> commonVersions = new LinkedHashSet<>(currentMigrations.keySet());
        commonVersions.retainAll(baseMigrations.keySet());
        List<FlywayMigrationIntegrityException> changes = new ArrayList<>();
        for (String version : commonVersions) {
            checkChangedMigration(version)
                    .flatMap(triple -> checkWhitelist(
                            triple.getLeft(), CHANGE, triple.getMiddle(), triple.getRight()))
                    .ifPresent(e -> changes.add(addViolationAdvice(e)));
        }
        return changes;
    }

    /**
     * Check whether the migration file has changed for the given version, between the base
     * commit and the current working tree.
     *
     * @param version version to check
     * @return the version, the base signature and an exception, if a change is detected
     * @throws IOException if there's a problem with git files
     */
    private Optional<Triple<String, String, FlywayMigrationIntegrityException>>
    checkChangedMigration(String version)
            throws IOException {
        final List<String> currentPaths = currentMigrations.get(version);
        final List<String> basePaths = baseMigrations.get(version);
        if (currentPaths.size() > 1 || basePaths.size() > 1) {
            // Can't compare content when either current or base has more than one choice.
            // That will cause a separate collision error
            return Optional.empty();
        }
        final String currentPath = currentPaths.get(0);
        final String basePath = basePaths.get(0);
        byte[] currentContent = getCurrentContent(currentPath);
        byte[] baseContent = getBaseContent(basePath);
        if (Arrays.equals(currentContent, baseContent)) {
            return Optional.empty();
        } else {
            final String former = currentPath.equals(basePath) ? ""
                    : " (formerly at " + basePath + ")";
            final String msg = String.format("Migration %s at %s%s has changed.",
                    version, currentPath, former);
            final String baseSignature = getSignature(baseContent);
            return Optional.of(Triple.of(version, baseSignature,
                    new FlywayChangedMigrationException(msg)));
        }
    }

    /**
     * Obtaint the hash signature for the given file content as a git blob.
     *
     * @param content file content
     * @return git signature
     */
    String getSignature(byte[] content) {
        return inserter.idFor(Constants.OBJ_BLOB, content).getName();
    }

    /**
     * Get the content of a migration file in the current working tree.
     *
     * @param path path to the migration file
     * @return the migration file content
     * @throws IOException if there's a problem accessing the file
     */
    private byte[] getCurrentContent(String path) throws IOException {
        File file = new File(repo.getWorkTree(), path);
        return Files.readAllBytes(file.toPath());
    }

    /**
     * Get the content of a migration file in the base commit.
     *
     * @param path path to the migration file
     * @return the migration file content
     * @throws IOException if there's a problem with git files
     */
    private byte[] getBaseContent(String path) throws IOException {
        try (TreeWalk walk = TreeWalk.forPath(repo, path, baseCommit.getTree())) {
            return repo.open(walk.getObjectId(0)).getBytes();
        }
    }

    /**
     * Check for colliding migration files, i.e. multiple files for the same migration version.
     *
     * <p>Both the current working tree and the base commit are checked.</p>
     *
     * @return exceptions for all detected collisions
     */
    List<FlywayMigrationIntegrityException> getCollidingMigrations() {
        final Stream<FlywayMigrationIntegrityException> collisionsStream = Stream.concat(
                getCollidingMigrations(currentMigrations, "working tree").stream(),
                getCollidingMigrations(baseMigrations, "base migration").stream()
        );
        return collisionsStream.map(e -> addViolationAdvice(e))
                .collect(Collectors.toList());
    }

    /**
     * Locate colliding collisions in the given migrations multimap.
     *
     * @param migrations multimap of versions to migration file paths
     * @param tree       path of tree in which the migrations are located, solely for use in
     *                   exception messages
     * @return list of exceptions for all detected collisions
     */
    private List<FlywayMigrationIntegrityException> getCollidingMigrations(
            ListMultimap<String, String> migrations, String tree) {
        List<FlywayMigrationIntegrityException> collisions = new ArrayList<>();
        for (String version : migrations.keySet()) {
            if (migrations.get(version).size() > 1) {
                String msg = String.format("Multiple migrations for version %s in %s: %s",
                        version, tree, migrations.get(version), COLLIDE_ADVICE);
                // collisions cannot be whitelisted
                collisions.add(new FlywayCollidingMigrationsException(msg));
            }
        }
        return collisions;
    }

    /**
     * Get a list of all migrations improperly inserted by the working tree into the migration
     * sequence defined by the base commit.
     *
     * <p>Insertions for whitelisted versions are not returned, but they are logged.</p>
     *
     * @return list of exceptions for all discovered insertions
     */
    List<FlywayMigrationIntegrityException> getMigrationInsertions() {
        List<FlywayMigrationIntegrityException> insertions = new ArrayList<>();
        // note that because we created the multimap with ordered keys, this list will
        // be in ascending version order
        List<String> baseMigrationVersions = new ArrayList<>(baseMigrations.keySet());
        if (!baseMigrationVersions.isEmpty()) {
            String maxBaseMigrationVersion = baseMigrationVersions.get(baseMigrationVersions.size() - 1);
            for (String version : currentMigrations.keySet()) {
                if (!baseMigrations.containsKey(version)
                        && compareVersions(version, maxBaseMigrationVersion) < 0) {
                    String msg = String.format(
                            "New migration version %s (at %s) cannot be inserted into existing sequence " +
                                    "with higher versions (including up to %s)",
                            version, currentMigrations.get(version), maxBaseMigrationVersion);
                    checkWhitelist(version, INSERT, null, new FlywayInsertedMigrationException(msg))
                            .ifPresent(e -> insertions.add(addViolationAdvice(e)));
                }
            }
        }
        return insertions;
    }

    /**
     * Get a list of all migration deletions.
     *
     * <p>Deletions for whitelisted versions are not returned, but they are logged.</p>
     *
     * @return list of exceptions for all discovered deletions.
     */
    List<FlywayMigrationIntegrityException> getMigrationDeletions() {
        List<FlywayMigrationIntegrityException> deletions = new ArrayList<>();
        for (String version : baseMigrations.keySet()) {
            if (!currentMigrations.containsKey(version)) {
                String msg = String.format("Cannot delete migration version %s formerly at %s",
                        version, baseMigrations.get(version), DELETION_ADVICE);
                checkWhitelist(version, DELETE, null, new FlywayDeletedMigrationException(msg))
                        .ifPresent(e -> deletions.add(addViolationAdvice(e)));
            }
        }
        return deletions;
    }

    /**
     * Convenience method to obtain a single list of all types of violations.
     *
     * @return single list containing all detected migration violations that are not whitelisted
     * @throws IOException if there's a problem with git files
     */
    List<FlywayMigrationIntegrityException> getAllViolations() throws IOException {
        List<FlywayMigrationIntegrityException> result = new ArrayList<>();
        result.addAll(getChangedMigrations());
        result.addAll(getCollidingMigrations());
        result.addAll(getMigrationInsertions());
        result.addAll(getMigrationDeletions());
        return result;
    }

    /**
     * Given a potential migration integrity violation, see whether it is exempted in the
     * whitelist.
     *
     * <p>If the violation is exempted by whitelist, it is logged regardless. It just won't
     * cause a test failure.</p>
     *
     * @param version       migraiton version to check
     * @param violationType the {@link ViolationType} to check for
     * @param signature     content signature (null for non-CHANGE violations)
     * @param e             violation exception if not whitelisted
     * @return the violation exception, if not whitelisted
     */
    private Optional<FlywayMigrationIntegrityException> checkWhitelist(
            String version,
            ViolationType violationType,
            String signature,
            FlywayMigrationIntegrityException e) {
        if (whitelist.isWhitelisted(version, violationType, signature)) {
            logger.info("Whitelisted {}: {}", violationType, e.getMessage());
            return Optional.empty();
        } else {
            return Optional.of(e);
        }
    }

    /**
     * Add type-specific advice to the given {@link FlywayMigrationIntegrityException} and return
     * the exception.
     *
     * @param e exception to which advice should be added
     * @return the exception with advice added
     */
    static FlywayMigrationIntegrityException addViolationAdvice(
            final FlywayMigrationIntegrityException e) {
        String advice = e instanceof FlywayChangedMigrationException ? CHANGE_ADVICE
                : e instanceof FlywayCollidingMigrationsException ? COLLIDE_ADVICE
                : e instanceof FlywayInsertedMigrationException ? INSERTION_ADVICE
                : e instanceof FlywayDeletedMigrationException ? DELETION_ADVICE
                : null;
        if (advice != null) {
            e.setAdvice(advice);
        }
        return e;
    }
}
