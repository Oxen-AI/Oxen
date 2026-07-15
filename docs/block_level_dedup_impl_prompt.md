Read the following document for context on what we want to implement: docs/block_level_dedup_plan.md refer back to it as you are writing code if
  you have any questions. Start the implementation in phases, testing as you go. As you make changes: Start by writing unit tests, Make the
  smallest set of code changes needed to progress toward the goal, Write changes directly to the filesystem, Run tests, linters, or build checks,
  If checks fail, read the errors and fix them in the next iteration, Continue iterating until all checks and tests pass. Make sure the code you 
  write is isolated, modular, pragmatic, idomatic rust code, that is easy to review for a teammate. Make sure you would be proud of the pull 
  request and changes you are writing. Make all the changes on a branch, and commit changes as you get different phases implemented and all the 
  tests passing.