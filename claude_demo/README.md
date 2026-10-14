# Even Checker

A small command-line app (`even_checker.py`) that asks for an integer and tells you
whether it is even or odd. Anything that isn't an integer (e.g. `3.5`, `abc`, or a blank
line) is rejected, and you're asked again.

## Requirements

- Python 3 and [pipenv](https://pipenv.pypa.io/)
- No third-party packages; the app uses only the Python standard library.

## Setup (pipenv)

This repo uses **one** virtual environment, defined by the `Pipfile` at the repo root
(`ds3022/`). Create or activate it from the root, not from inside `claude_demo/`:

```bash
cd ds3022                # repo root, where the Pipfile lives
pip install pipenv       # only if pipenv isn't installed yet
pipenv install           # creates the virtualenv and installs dependencies
```

## Usage

From the repo root, either activate the shell:

```bash
pipenv shell
python claude_demo/even_checker.py
```

or run it in one step:

```bash
pipenv run python claude_demo/even_checker.py
```

Example session:

```
Enter an integer: abc
'abc' is not an integer. Please try again.
Enter an integer: 3.5
'3.5' is not an integer. Please try again.
Enter an integer: 42
42 is even.
```

Press `Ctrl+C` or `Ctrl+D` to quit without entering a number.

## Logging

Every accepted input, rejected input, and result is written to
`claude_demo/even_checker.log`. The log is generated output, so don't commit it.
