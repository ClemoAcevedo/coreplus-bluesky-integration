"""
Declaraciones CREATE STREAM que se envían al motor CORE+.

Cada stream expone **un único evento** (CreatePost / CreateLike / CreateRepost)
y define los atributos exactamente en el mismo orden y tipo que produce
`attributes.py`.  
El ﬁchero es importado por `main_bluesky.py`, que lee las constantes y
las envía al servidor CORE+ en la fase de “bootstrap”.
"""

# ──────────────────────────────────────────────────────────
# BlueskyPosts
# ──────────────────────────────────────────────────────────
BLUESKY_POST_STREAM_DECLARATION = """
CREATE STREAM BlueskyPosts {
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
    }
}
"""

# ──────────────────────────────────────────────────────────
# BlueskyLikes
# ──────────────────────────────────────────────────────────
BLUESKY_LIKE_STREAM_DECLARATION = """
CREATE STREAM BlueskyLikes {
    EVENT CreateLike {
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

# ──────────────────────────────────────────────────────────
# BlueskyReposts
# ──────────────────────────────────────────────────────────
BLUESKY_REPOST_STREAM_DECLARATION = """
CREATE STREAM BlueskyReposts {
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
