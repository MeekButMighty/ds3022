"""Prompt the user for an integer and report whether it is even."""
import logging
import sys
from pathlib import Path

# Log to a local file alongside the script
logging.basicConfig(
    filename=Path(__file__).with_suffix(".log"),
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)


def get_integer(prompt="Enter an integer: "):
    """Re-prompt until the user enters a valid integer (rejects floats, text, blanks)."""
    while True:
        raw = input(prompt).strip()
        try:
            value = int(raw)  # int() rejects "3.5", "abc", and ""
            log.info("Accepted input: %r", raw)
            return value
        except ValueError:
            log.warning("Rejected non-integer input: %r", raw)
            print(f"'{raw}' is not an integer. Please try again.")


def is_even(n):
    """Return True if n is even."""
    return n % 2 == 0


def main():
    try:
        n = get_integer()
        result = "even" if is_even(n) else "odd"
        print(f"{n} is {result}.")
        log.info("%d evaluated as %s", n, result)
    except (KeyboardInterrupt, EOFError):
        # Handle Ctrl+C / Ctrl+D gracefully
        print("\nExiting.")
        log.info("User exited before entering a valid integer")
        sys.exit(1)
    except Exception:
        log.exception("Unexpected error")
        raise


if __name__ == "__main__":
    main()
