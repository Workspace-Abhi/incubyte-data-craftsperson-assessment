"""Backward-compatibility shim for pipeline."""
try:
    from skypoints.pipeline import *
except ImportError:
    from src.skypoints.pipeline import *
