"""
Mapa $type → configuración de handler.

* `stream_name` / `event_name`  — Nombres declarados en streams.py
* `attribute_creator`           — Función que construye el dict de atributos
* `stream_id` / `event_id`      — Rellenados dinámicamente por main_bluesky.py
"""

from __future__ import annotations

from .attributes import (
    create_bluesky_post_attributes,
    create_bluesky_like_attributes,
    create_bluesky_repost_attributes,
    create_bluesky_profile_attributes,
    create_bluesky_follow_attributes,
    create_bluesky_block_attributes,
)

BLUESKY_EVENT_HANDLERS_CONFIG: dict[str, dict[str, object]] = {
    # --------------------------------------------------------------------- #
    "app.bsky.feed.post": {
        "stream_name":       "BlueskyEvents",
        "event_name":        "CreatePost",
        "attribute_creator": create_bluesky_post_attributes,
        "stream_id":         None,   # ← se completa al arrancar
        "event_id":          None,
    },
    # --------------------------------------------------------------------- #
    "app.bsky.feed.like": {
        "stream_name":       "BlueskyEvents",
        "event_name":        "CreateLike",
        "attribute_creator": create_bluesky_like_attributes,
        "stream_id":         None,
        "event_id":          None,
    },
    # --------------------------------------------------------------------- #
    "app.bsky.feed.repost": {
        "stream_name":       "BlueskyEvents",
        "event_name":        "CreateRepost",
        "attribute_creator": create_bluesky_repost_attributes,
        "stream_id":         None,
        "event_id":          None,
    },
    # --------------------------------------------------------------------- #
    "app.bsky.actor.profile": {
        "stream_name":       "BlueskyEvents",
        "event_name":        "UpdateProfile",
        "attribute_creator": create_bluesky_profile_attributes,
        "stream_id":         None,
        "event_id":          None,
    },
    # --------------------------------------------------------------------- #
    "app.bsky.graph.follow": {
        "stream_name":       "BlueskyEvents",
        "event_name":        "CreateFollow",
        "attribute_creator": create_bluesky_follow_attributes,
        "stream_id":         None,
        "event_id":          None,
    },
    # --------------------------------------------------------------------- #
    "app.bsky.graph.block": {
        "stream_name":       "BlueskyEvents",
        "event_name":        "CreateBlock",
        "attribute_creator": create_bluesky_block_attributes,
        "stream_id":         None,
        "event_id":          None,
    },
}