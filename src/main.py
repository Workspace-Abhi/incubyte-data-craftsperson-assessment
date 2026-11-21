"""Backward-compatibility shim for main CLI."""
try:
    from skypoints.cli import main
except ImportError:
    from src.skypoints.cli import main

if __name__ == "__main__":
    main()
