"""Backward-compatibility shim for models."""
try:
    from skypoints.models import *
except ImportError:
    from src.skypoints.models import *
