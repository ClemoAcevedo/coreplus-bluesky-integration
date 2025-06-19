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
    EVENT CreatePost {
        /* Identificación básica del post ------------------------------------ */
        uri                 : string,
        commit_cid          : string,
        repo                : string,
        seq                 : int,

        /* Timestamps -------------------------------------------------------- */
        commit_time         : double,         /* ns epoch (from commit op)     */
        record_text         : string,         /* cuerpo del post; puede ser '' */
        record_created_at   : primary_time,   /* ns epoch – SOLO primary_time  */

        /* Metadatos opcionales --------------------------------------------- */
        langs               : string,
        reply_root_uri      : string,
        reply_parent_uri    : string,

        /* Embed ------------------------------------------------------------- */
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
        commit_time         : double,         /* ns epoch (commit op)          */
        record_created_at   : primary_time,   /* ns epoch – SOLO primary_time  */
        subject_uri         : string,
        subject_cid         : string
    },
    EVENT CreateRepost {
        uri                 : string,
        commit_cid          : string,
        repo                : string,
        seq                 : int,
        commit_time         : double,         /* ns epoch (commit op)          */
        record_created_at   : primary_time,   /* ns epoch – SOLO primary_time  */
        subject_uri         : string,
        subject_cid         : string
    }
}
"""

