ALGORITHM = fixed-size
CHUNK_SIZE = 8192
FILE = large_filler.csv
CARGO_RUN = cargo run
CARGO = cargo 
DEDUP_BUILD = "../../Oxen/target/debug/dedup"


.PHONY: test unpack pack clean clean-test

clean:
	$(CARGO) clean

test:
	$(CARGO_RUN) test --algorithm $(ALGORITHM) --chunk-size $(CHUNK_SIZE) --input-file $(FILE)

clean-test:
	$(CARGO) clean
	$(CARGO_RUN) test --algorithm $(ALGORITHM) --chunk-size $(CHUNK_SIZE) --input-file $(FILE)

unpack:
	$(CARGO_RUN) unpack --algorithm $(ALGORITHM) --chunk-size $(CHUNK_SIZE) --output-file $(FILE) --input-dir 4k

pack:
	$(CARGO_RUN) pack --algorithm $(ALGORITHM) --chunk-size $(CHUNK_SIZE) --file $(FILE) --output-dir 4k

test-oxen:
	$(DEDUP_BUILD) test-oxen --algorithm $(ALGORITHM) --chunk-size $(CHUNK_SIZE) --input-file $(FILE)