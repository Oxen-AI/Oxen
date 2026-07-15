# Claude vs GPT Plan

There have been two plans proposed for deduplication strategies within the core oxen version control system.

Read the following original prompt for context to what we are building: docs/block_level_dedup_prompt.md

Then look at two different plans that were made to tackle the problem:

1) docs/block_level_dedup_gpt_plan.md
2) docs/block_level_dedup_claude_plan.md

Compare and contrast the plans vs the original prompt, flagging any differences, if the plans agree, assume we are moving forward with the strategy. If they disagree, list the pros and cons of each strategy, and let's discuss further. Present me with each one of the discrepancies, pros and cons, and we will make a decision together.

One thing I missed in the original prompt was that we should use LMDB for any key-value storage that needs fast lookup, like the newer merkle tree implementation. We are using rocksdb in some places we should use LMDB in these plans. Update accordingly.

Write this agreed upon spec and context to a new file when we are finished: docs/block_level_dedup_plan.md

Let me know if you have any questions before getting started.