# streams.py

"""
Declaraciones CREATE STREAM que se envían al motor CORE+.

Se define un único stream consolidado (BlueskyEvents) que agrupa
los eventos de Bluesky para simplificar las queries y la gestión.
Cada evento (CreatePost, CreateLike, etc.) define los atributos
exactamente en el mismo orden y tipo que produce `attributes.py`.
"""

# ──────────────────────────────────────────────────────────
# Stream único para todos los eventos de Bluesky
# ──────────────────────────────────────────────────────────
BLUESKY_EVENTS_STREAM_DECLARATION = """
CREATE STREAM BlueskyEvents {
    /* ... Eventos CreatePost, CreateLike, CreateRepost sin cambios ... */
    EVENT CreatePost {
        uri                 : string,
        commit_cid          : string,
        repo                : string,
        seq                 : int,
        commit_time         : double,
        record_text         : string,
        record_created_at   : primary_time,
        langs               : string,
        reply_root_uri      : string,
        reply_parent_uri    : string,
        embed_type          : string,
        embed_image_count   : int,
        embed_external_uri  : string,
        embed_record_uri    : string
    },
    EVENT CreateLike {
        uri                 : string,
        commit_cid          : string,
        repo                : string,
        seq                 : int,
        commit_time         : double,
        record_created_at   : primary_time,
        subject_uri         : string,
        subject_cid         : string
    },
    EVENT CreateRepost {
        uri                 : string,
        commit_cid          : string,
        repo                : string,
        seq                 : int,
        commit_time         : double,
        record_created_at   : primary_time,
        subject_uri         : string,
        subject_cid         : string
    },
    
    /* --- EVENTO CORREGIDO --- */
    EVENT UpdateProfile {
        repo                : string,         /* DID del perfil actualizado    */
        commit_cid          : string,
        seq                 : int,
        commit_time         : double,
        profile_text        : string          /* displayName y description unidos por '|||' */
    },
    EVENT CreateFollow {
        repo                : string,         /* DID de quien sigue            */
        commit_cid          : string,
        seq                 : int,
        commit_time         : double,
        record_created_at   : primary_time,
        subject_did         : string          /* DID de a quien se sigue       */
    },
    EVENT CreateBlock {
        repo                : string,         /* DID de quien bloquea          */
        commit_cid          : string,
        seq                 : int,
        commit_time         : double,
        record_created_at   : primary_time,
        subject_did         : string          /* DID de a quien se bloquea     */
    }
}
"""