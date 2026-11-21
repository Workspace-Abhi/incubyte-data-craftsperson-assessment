"""Backward-compatibility shim for country_router."""
try:
    from skypoints.transform.country_router import *
except ImportError:
    from src.skypoints.transform.country_router import *
