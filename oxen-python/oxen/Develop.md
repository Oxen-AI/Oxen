# 🐂 + 🐍 Dev Guide

How to contribute to Oxen Python Interface


## Build and Run

From the root of this repo, navigate to the oxen directory

```bash
cd oxen
```

Install requirements

```bash
pip install -r requirements.txt
```

Then use `maturin` to build the Rust native module and deploy it in the current virtualenv.

```bash
maturin develop
```

For intel builds if it cannot find the c++ headers

```
env CPLUS_INCLUDE_PATH=/Library/Developer/CommandLineTools/usr/include/c++/v1/ maturin develop
```

## Run Tests

```bash
pytest -s tests/
```

## Code Quality

### Formatting

```bash
black .
```

### Linting

```bash
ruff .
```

## Adding Modules 🦀 -> 🐍

1. Create a test file in `tests/` that imports the module and tests it.
2. Implement or extend the rust wrappers in `src/py_*.rs`
3. Make sure the rust module is linked `src/lib.rs`
4. Add the module to `python/oxen/__init__.py`
5. If there is another python layer that needs to be added, or python docs that we want to generate, use `python/oxen/repo.py` as a reference for how it can interact with the rust modules.
6. Run the tests end to end, to make sure you didn't leave out any steps.

## Documentation

```
brew install sphinx-doc
```

```
cd oxen/docs
make install
```
