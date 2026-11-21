"""Backward-compatibility shim for validators."""
try:
    from skypoints.validation.validators import *
except ImportError:
    from src.skypoints.validation.validators import *
