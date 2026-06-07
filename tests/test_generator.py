import random
from datetime import date


def _inputs(seed="42", d="2026-06-07"):
    rng = random.Random(f"{seed}:{d}")
    cards = [f"HASH{i:060d}" for i in range(50)]
    merchants = [(f"M{i:04d}", f"{5000 + i:04d}") for i in range(20)]
    return rng, date.fromisoformat(d), cards, merchants


def test_auth_rows_deterministic_and_count(genmod):
    rng, d, cards, merch = _inputs()
    a1 = genmod._gen_auth_rows(rng, d, 500, cards, merch)
    rng2, d2, cards2, merch2 = _inputs()
    a2 = genmod._gen_auth_rows(rng2, d2, 500, cards2, merch2)
    assert [r for r, _ in a1] == [r for r, _ in a2]          # identical given same seed/date
    assert sum(1 for _, is_dup in a1 if not is_dup) == 500    # 500 originals (+ any dups)


def test_decline_rate_in_range(genmod):
    rng, d, cards, merch = _inputs()
    rows = genmod._gen_auth_rows(rng, d, 500, cards, merch)
    declines = sum(1 for r, _ in rows if r["response_code"] != "00")
    assert 10 <= declines <= 60   # ~5% with slack


def test_posts_only_for_approved_non_duplicate(genmod):
    rng, d, cards, merch = _inputs()
    auth = genmod._gen_auth_rows(rng, d, 500, cards, merch)
    posts = genmod._gen_post_rows(rng, d, auth)
    approved_ids = {r["txn_id"] for r, is_dup in auth if not is_dup and r["response_code"] == "00"}
    assert posts, "expected some postings"
    for p in posts:
        assert p["txn_id"] in approved_ids   # never post a decline or a duplicate


def test_card_hashes_come_from_reference_pool(genmod):
    rng, d, cards, merch = _inputs()
    rows = genmod._gen_auth_rows(rng, d, 500, cards, merch)
    pool = set(cards)
    assert all(r["card_number_hash"] in pool for r, _ in rows)
