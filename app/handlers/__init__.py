"""Internal EventBus → delivery bridges (not coordinator Consumers)."""

from app.handlers.price_update import register_event_handlers

__all__ = ["register_event_handlers"]
