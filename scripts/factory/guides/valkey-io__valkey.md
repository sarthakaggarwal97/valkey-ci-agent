# Fixing a Valkey issue from its test

Valkey's own guidelines for code and tests are at the end of this file, as the
default branch has them (`AGENTS.md`, `DEVELOPMENT_GUIDE.md`, `tests/README.md`
and `src/unit/README.md`). Read them: they are the authority on style and
conventions.

What follows first is the bot's own policy for a fix made from an issue, not
a summary of those files.

## The test is the specification

A test written from the issue fails on the working commit, every time code
ran it. The fix is judged by it: it must pass with the fix, for the right
reason, and the test itself may not be changed. The fix lives in `src/`: the
test is not the thing at fault here. Read the test and the issue together
before you read the code: the issue says what the server should do, the test
says how that is checked.

A test can cover less than the issue. Fix what the issue describes, not only
the line the test asserts: a fix that satisfies the assertion and leaves the
case the issue reports in another path is sent back by the review.

## The change

- Fix the cause, in the smallest coherent change, where the existing code
  handles the case next to it. Match the surrounding code; add no helper where
  one exists, no handler for a state that cannot happen, no flag or option the
  issue does not ask for.
- Keep it easy to backport: release branches take the same change.
- Never change `.github/`, and never change `deps/`: code under `deps/` is
  vendored and Valkey takes a change there only as an update from its project.
  If the fix has to be there, choose `refuse` and say which project needs it.
- A new command, a changed reply format or a breaking change is a decision
  for the project, not a fix. Choose `refuse` and say what needs deciding.
- You may add a test of your own where the fix needs one, in the same file as
  the test you were given or the existing file for the area, and never a test
  that only passes because of how it is run.
- Replication and persistence: a change to what a command does usually
  changes what it propagates and what is saved. Check `propagate`,
  `rewriteConfig`, `rdbSave`/`rdbLoad` and the AOF rewrite for the type you
  touched, and say in `validation` what you checked.
- Formatting: Valkey's CI runs `clang-format-18` on `src/*.c`, `*.h`, `*.cpp`
  and `*.hpp`. Code checks your change with it. Follow the file's style.
- A changed command definition under `src/commands/` must leave
  `make commands.def` with nothing to regenerate; include the regenerated
  `src/commands.def`.

## Comments and messages

A comment says what the code does or why, briefly. No narration of the fix,
no history ("previously", "now"), and no issue or pull request numbers. Write
the summary, root cause and reasoning as plain sentences for a maintainer, and
mention nobody.

## What code checks

With your change, code builds the server and runs the test, once and then in
a loop. Then it runs every test file the change touches in full, checks the
formatting and the generated files, and runs the test once more with the
server built under AddressSanitizer. A second session then reviews the change
with the issue and the test's output in front of it, and tries to find the
case it still gets wrong. Anything that fails comes back to you with the
output.
