# CORE/test/test_bluesky_event_parser.py
import unittest
import sys
import os
import json
import re   # usado sólo para el banner de resumen

# ───────────────────────────────────────────────────────────────
#  Pathing: añadimos la raíz del repo (…/CORE) al sys.path
# ───────────────────────────────────────────────────────────────
sys.path.insert(
    0,
    os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "..")
    ),
)

from bluesky.bluesky_event_parser import (
    parse_event_attributes,
    STREAM_ATTRIBUTE_DEFINITIONS,
)

# ───────────────────────────────────────────────────────────────
#  Utilidades de ayuda / pretty-print
# ───────────────────────────────────────────────────────────────
def pretty_print_parse_result(test_name, raw, parsed, errors):
    print(f"\n--- {test_name} ---")
    print(f"Input : {raw!r}\n")
    print("Parsed:")
    print(json.dumps(parsed, indent=2))
    if errors:
        print("\nErrors:")
        for e in errors:
            print(" •", e)
    else:
        print("\n(No errors)")
    print("-" * 40)


# Placeholder CIDs válidos
VALID_B_CID = "b" + "a" * 58                      # 59 chars
VALID_Z_CID = "zdpuAnBiHHaSXYXsyWD3CDggpkh83iQvQkA4UZSSbXT4pKuty"  # 49 chars


# ══════════════════════════════════════════════════════════════
#  Tests
# ══════════════════════════════════════════════════════════════
class TestBlueskyEventParser(unittest.TestCase):
    def test_problematic_create_post_from_log(self):
        """Caso real que fallaba en producción (doble espacio + ‘0’ suelto)."""
        evt = "BlueskyEvents.CreatePost"
        raw = (
            "at://did:plc:akkzm62zogz3r3y7hbdrdj52/app.bsky.feed.post/3lqbiwtbfs52p "
            f"{VALID_Z_CID} did:plc:akkzm62zogz3r3y7hbdrdj52 9692709225 "
            "1748480004763000064.000000  946684800000000000         0   "
        )

        exp = {
            "uri": "at://did:plc:akkzm62zogz3r3y7hbdrdj52/app.bsky.feed.post/3lqbiwtbfs52p",
            "commit_cid": VALID_Z_CID,
            "repo": "did:plc:akkzm62zogz3r3y7hbdrdj52",
            "seq": 9692709225,
            "commit_time": 1748480004763000064.0,
            "record_text": "",
            "record_created_at": 946684800000000000,
            "langs": "",
            "reply_root_uri": "",
            "reply_parent_uri": "",
            "embed_type": "",
            "embed_image_count": 0,
            "embed_external_uri": "",
            "embed_record_uri": "",
        }

        parsed, errs = parse_event_attributes(evt, raw)
        self.assertEqual(errs, [], f"Unexpected errors: {errs!r}")
        self.assertEqual(parsed, exp)

    def test_create_post_full_no_text(self):
        evt = "BlueskyEvents.CreatePost"
        raw = (
            "at://did:plc:user/app.bsky.feed.post/id1 "
            f"{VALID_B_CID} did:plc:user 12345 1678886400.123456 "
            "1678886400000000000 en,es-MX "
            "at://did:plc:root/app.bsky.feed.post/rootid "
            "at://did:plc:parent/app.bsky.feed.post/parentid "
            "app.bsky.embed.images 2 http://example.com/external "
            "at://did:plc:record/app.bsky.feed.post/recordid"
        )
        exp = {
            "uri": "at://did:plc:user/app.bsky.feed.post/id1",
            "commit_cid": VALID_B_CID,
            "repo": "did:plc:user",
            "seq": 12345,
            "commit_time": 1678886400.123456,
            "record_text": "",
            "record_created_at": 1678886400000000000,
            "langs": "en,es-MX",
            "reply_root_uri": "at://did:plc:root/app.bsky.feed.post/rootid",
            "reply_parent_uri": "at://did:plc:parent/app.bsky.feed.post/parentid",
            "embed_type": "app.bsky.embed.images",
            "embed_image_count": 2,
            "embed_external_uri": "http://example.com/external",
            "embed_record_uri": "at://did:plc:record/app.bsky.feed.post/recordid",
        }

        parsed, errs = parse_event_attributes(evt, raw)
        self.assertEqual(errs, [])
        self.assertEqual(parsed, exp)

    def test_create_post_with_text(self):
        evt = "BlueskyEvents.CreatePost"
        raw = (
            "at://did:plc:userx/app.bsky.feed.post/id2 "
            f"{VALID_B_CID} did:plc:userx 54321 1679000000.654321 "
            "This is the record text with multiple tokens. "
            "1679000000000000000 fr  app.bsky.embed.external 0 "
            "https://external.link/page "
        )
        exp = {
            "uri": "at://did:plc:userx/app.bsky.feed.post/id2",
            "commit_cid": VALID_B_CID,
            "repo": "did:plc:userx",
            "seq": 54321,
            "commit_time": 1679000000.654321,
            "record_text": "This is the record text with multiple tokens.",
            "record_created_at": 1679000000000000000,
            "langs": "fr",
            "reply_root_uri": "",
            "reply_parent_uri": "",
            "embed_type": "app.bsky.embed.external",
            "embed_image_count": 0,
            "embed_external_uri": "https://external.link/page",
            "embed_record_uri": "",
        }

        parsed, errs = parse_event_attributes(evt, raw)
        self.assertEqual(errs, [])
        self.assertEqual(parsed, exp)

    def test_create_post_all_optionals_empty(self):
        evt = "BlueskyEvents.CreatePost"
        raw = (
            "at://did:plc:usery/app.bsky.feed.post/id3 "
            f"{VALID_B_CID} did:plc:usery 98765 1680000000.000000 "
            "1680000000000000000          0  "
        )
        exp = {
            "uri": "at://did:plc:usery/app.bsky.feed.post/id3",
            "commit_cid": VALID_B_CID,
            "repo": "did:plc:usery",
            "seq": 98765,
            "commit_time": 1680000000.000000,
            "record_text": "",
            "record_created_at": 1680000000000000000,
            "langs": "",
            "reply_root_uri": "",
            "reply_parent_uri": "",
            "embed_type": "",
            "embed_image_count": 0,
            "embed_external_uri": "",
            "embed_record_uri": "",
        }

        parsed, errs = parse_event_attributes(evt, raw)
        self.assertEqual(errs, [])
        self.assertEqual(parsed, exp)

    def test_create_like_basic(self):
        evt = "BlueskyEvents.CreateLike"
        raw = (
            "at://did:plc:user1/app.bsky.feed.like/lk1 "
            f"{VALID_B_CID} did:plc:user1 12345 1710000000.123456 "
            "1710000000000000000 at://did:plc:user2/app.bsky.feed.post/pst1 "
            f"{VALID_B_CID+'a'}"
        )
        exp = {
            "uri": "at://did:plc:user1/app.bsky.feed.like/lk1",
            "commit_cid": VALID_B_CID,
            "repo": "did:plc:user1",
            "seq": 12345,
            "commit_time": 1710000000.123456,
            "record_created_at": 1710000000000000000,
            "subject_uri": "at://did:plc:user2/app.bsky.feed.post/pst1",
            "subject_cid": VALID_B_CID + "a",
        }

        parsed, errs = parse_event_attributes(evt, raw)
        self.assertEqual(errs, [])
        self.assertEqual(parsed, exp)

    def test_create_like_optional_subject_cid_empty(self):
        evt = "BlueskyEvents.CreateLike"
        raw = (
            "at://did:plc:user1/app.bsky.feed.like/lk2 "
            f"{VALID_B_CID} did:plc:user1 12346 1710000001.000000 "
            "1710000001000000000 at://did:plc:user2/app.bsky.feed.post/pst2"
        )
        exp = {
            "uri": "at://did:plc:user1/app.bsky.feed.like/lk2",
            "commit_cid": VALID_B_CID,
            "repo": "did:plc:user1",
            "seq": 12346,
            "commit_time": 1710000001.000000,
            "record_created_at": 1710000001000000000,
            "subject_uri": "at://did:plc:user2/app.bsky.feed.post/pst2",
            "subject_cid": "",
        }

        parsed, errs = parse_event_attributes(evt, raw)
        self.assertEqual(errs, [])
        self.assertEqual(parsed, exp)

    def test_create_repost_basic(self):
        evt = "BlueskyEvents.CreateRepost"
        raw = (
            "at://did:plc:user1/app.bsky.feed.repost/rp1 "
            f"{VALID_B_CID} did:plc:user1 67890 1720000000.654321 "
            "1720000000000000000 at://did:plc:user3/app.bsky.feed.post/pst2 "
            f"{VALID_B_CID+'b'}"
        )
        exp = {
            "uri": "at://did:plc:user1/app.bsky.feed.repost/rp1",
            "commit_cid": VALID_B_CID,
            "repo": "did:plc:user1",
            "seq": 67890,
            "commit_time": 1720000000.654321,
            "record_created_at": 1720000000000000000,
            "subject_uri": "at://did:plc:user3/app.bsky.feed.post/pst2",
            "subject_cid": VALID_B_CID + "b",
        }

        parsed, errs = parse_event_attributes(evt, raw)
        self.assertEqual(errs, [])
        self.assertEqual(parsed, exp)

    def test_create_post_text_with_numbers_and_hyphens(self):
        evt = "BlueskyEvents.CreatePost"
        raw = (
            "at://did:plc:test/app.bsky.feed.post/textnum "
            f"{VALID_B_CID} did:plc:test 1001 1679000000.000000 "
            "Text with numbers 12345 and date like 2023-03-15 and "
            "at://mention/app.bsky.actor.profile/did:plc:fake but not a real token here. "
            "1679000001000000000 en  0  "
        )
        exp = {
            "uri": "at://did:plc:test/app.bsky.feed.post/textnum",
            "commit_cid": VALID_B_CID,
            "repo": "did:plc:test",
            "seq": 1001,
            "commit_time": 1679000000.0,
            "record_text": (
                "Text with numbers 12345 and date like 2023-03-15 and "
                "at://mention/app.bsky.actor.profile/did:plc:fake but not a real token here."
            ),
            "record_created_at": 1679000001000000000,
            "langs": "en",
            "reply_root_uri": "",
            "reply_parent_uri": "",
            "embed_type": "",
            "embed_image_count": 0,
            "embed_external_uri": "",
            "embed_record_uri": "",
        }

        parsed, errs = parse_event_attributes(evt, raw)
        self.assertEqual(errs, [])
        self.assertEqual(parsed, exp)

    def test_unparseable_remainder(self):
        evt = "BlueskyEvents.CreateLike"
        raw = (
            "at://did:plc:user1/app.bsky.feed.like/lk1 "
            f"{VALID_B_CID} did:plc:user1 12345 1710000000.123456 "
            "1710000000000000000 at://did:plc:user2/app.bsky.feed.post/pst1 "
            f"{VALID_B_CID+'a'} EXTRA_UNPARSED_CONTENT"
        )

        parsed, errs = parse_event_attributes(evt, raw)
        self.assertTrue(
            any("Extra unparsed content" in e for e in errs),
            "Expected 'Extra unparsed content' error",
        )
        self.assertEqual(parsed.get("_UNPARSED_REMAINDER_"), "EXTRA_UNPARSED_CONTENT")

    def test_missing_required_field_middle(self):
        """
        Falta el campo 'repo'; aceptamos que parser deje '', pero debe reportar error.
        """
        evt = "BlueskyEvents.CreateLike"
        raw = (
            "at://did:plc:user1/app.bsky.feed.like/lk1 "
            f"{VALID_B_CID} 12345 1710000000.123456 "
            "1710000000000000000 at://did:plc:user2/app.bsky.feed.post/pst1 "
            f"{VALID_B_CID+'a'}"
        )

        parsed, errs = parse_event_attributes(evt, raw)

        self.assertTrue(
            any("Pattern for 'repo' did not match" in e for e in errs),
            "Expected 'repo' pattern error",
        )
        # El valor puede ser cadena vacía (fallback) o el placeholder de error,
        # dependiendo del modo estricto en parse_event_attributes:
        self.assertIn(parsed.get("repo", ""), ("", "<ERROR:MISSING_OR_NO_MATCH_REPO>"))


# ══════════════════════════════════════════════════════════════
#  Test-suite de edge-cases reales (logs)
# ══════════════════════════════════════════════════════════════
class TestBlueskyEventParserEdgeCases(unittest.TestCase):
    def test_create_post_binary_text_no_langs_trailing_zero(self):
        """
        Caso tomado del log: record_text binario y '0' suelto al final;
        esperaba parsers sin resto ni errores.
        """
        evt = "BlueskyEvents.CreatePost"
        raw = (
            "at://did:plc:fcnbisw7xl6lmtcnvioocffz/app.bsky.feed.post/3lqbs5wgrmp23 "
            "zdpuAkSUKCEP6hfXeJNk4D9FiXhoWpEbPzDkmwPyCjsk8CMJD "
            "did:plc:fcnbisw7xl6lmtcnvioocffz 9696788217 1748489906736999936.000000 "
            "0110001001100011011001000011000100110101001101010011010000110001"
            "0110000101100010001100100110010000110000001100010011000101100110"
            "0011011100110001011000010011000101100001001101000110011001100010"
            "0110010101100100001100100110011000110001001101110011010000110001 "
            "1748489896000000000 0"
        )

        parsed, errs = parse_event_attributes(evt, raw)

        self.assertEqual(errs, [], f"Expected 0 errors, got {errs!r}")
        self.assertEqual(parsed["embed_image_count"], 0)
        self.assertEqual(parsed["langs"], "")
        self.assertEqual(parsed["embed_type"], "")
        self.assertEqual(parsed["embed_external_uri"], "")
        self.assertEqual(parsed["embed_record_uri"], "")


# ══════════════════════════════════════════════════════════════
#  Runner
# ══════════════════════════════════════════════════════════════
if __name__ == "__main__":
    # Resumen breve de los schemas (para diagnóstico rápido)
    print("STREAM_ATTRIBUTE_DEFINITIONS (resumen):")
    for etype, attrs in STREAM_ATTRIBUTE_DEFINITIONS.items():
        print(" •", etype)
        for ad in attrs:
            d = {
                k: (v.pattern if isinstance(v, re.Pattern) else v)
                for k, v in ad.items()
                if k != "_compiled_regex"
            }
            print("    ", d)
    print("\nRunning tests…\n" + "=" * 60 + "\n")

    unittest.main(verbosity=2)