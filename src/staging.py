"""Backward-compatibility shim for staging."""
try:
    from skypoints.transform.staging import *
except ImportError:
    from src.skypoints.transform.staging import *
