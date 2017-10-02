Version-controlled git hooks.

hook-names contains all hooks for local repositories (since this isn't for remote repos).
Copied from: https://www.kernel.org/pub/software/scm/git/docs/githooks.html

Run init-hooks to copy all your current hooks to the personal/ directory and set up
the symlinks necessary to run the personal/ and common/ hooks.

To add a hook:

Use "$(hookname)-" as a prefix and put the hook into either personal/ or common/.
For example:
   pre-commit-spellcheck

How it works:
All git hooks call hook-wrapper, which calls the individual hooks in personal/ or
common/ with the expected prefix.
