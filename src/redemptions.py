"""Backward-compatibility shim for redemptions."""
try:
    from skypoints.redemptions.engine import *
except ImportError:
    from src.skypoints.redemptions.engine import *
