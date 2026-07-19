# Block level deduplication

Oxen is a version control system that specializes in handling large files (unlike git). It is mirrored after a lot of the patterns in git - so it is easy to learn for software engineers, but unlike git handles large files more efficiently. 

We want one of the core benefits of using oxen to be it's compression. Specifically of large files. Oxen can automatically detect different file types, and saves this information in the merkle tree already. Certain file types, like data frames or tabular data (csv, jsonl, parquet files), have nice properties for compression. For example if you are just labeling a dataset, adding new rows or columns, we should be able to smartly compress the file and deduplicate chunks of the file over time.

Hugging Face and xet have done good prior art, read these articles for context and to learn their solution.

https://huggingface.co/blog/from-files-to-chunks

https://huggingface.co/blog/from-chunks-to-blocks

We want our solution to be robust and extensible and customizable per file type. Each file type may have different properties that we can take advantage of during compression. 

Explore the existing codebase, including the merkle tree implementation, file system implementations (including S3 or other storage backends), the commit logic, and the fetching of file logic to get a basic understanding of the existing system. We should use LMDB for any key-value storage that needs fast lookup, like the newer merkle tree implementation

Then I want you to come up with a minimal viable change to enable "block level deduplication". Make sure the change is extensible, modular, and easy to understand. We want to set ourselves up for future contributors to add different compression implementations for files and chunks and blocks. The design should be idiomatic and pragmatic rust.

First research the problem, then explore the code, then quiz me until we come to a shared understanding. Interview me relentlessly about every aspect of this plan until we agree. Walk down each branch of the design tree, resolving dependencies between decisions one-by-one. For each question, provide your recommended answer.

Ask the questions one at a time.

If a question can be answered by exploring the codebase, explore the codebase instead.

Once we have a shared understanding and feel really good about the design, then write the design to a .md file in the docs/ directory with all the context needed, including questions and answers, so that we can refer to it during the implementation phase.

