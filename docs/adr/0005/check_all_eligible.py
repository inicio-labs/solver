"""Decide each maker on ALL its eligible orders of the pair (newest side wins), then fill the
windows in one walk that skips sit-outs. No refill rule, no repeat. Checks the invariants on
every book and compares with the window-only rule."""
import itertools, random
from check_rule import maker_seqs, two_max, window_algorithm

def decisions(sells, buys, seq):
    kept = {}
    for m in {o for o in sells + buys if o != "U"}:
        mine = [("S", r) for r, o in enumerate(sells) if o == m] + [("B", r) for r, o in enumerate(buys) if o == m]
        mine.sort(key=lambda k: seq[k])
        keep = two_max([k[0] for k in mine])
        kept[m] = {mine[i] for i in keep}
    return kept

def walk(sells, buys, W, seq):
    kept = decisions(sells, buys, seq)
    window, skipped = {"S": [], "B": []}, {"S": set(), "B": set()}
    for side, lst in (("S", sells), ("B", buys)):
        for r, o in enumerate(lst):
            if len(window[side]) == W:
                break
            if o != "U" and (side, r) not in kept[o]:
                skipped[side].add(r)
                continue
            window[side].append(r)
    return window, skipped, kept

def check(sells, buys, W, seq):
    window, skipped, kept = walk(sells, buys, W, seq)
    ms = {sells[r] for r in window["S"] if sells[r] != "U"}; mb = {buys[r] for r in window["B"] if buys[r] != "U"}
    assert not (ms & mb), "maker on both sides"
    for side, lst in (("S", sells), ("B", buys)):
        assert all(lst[r] != "U" for r in skipped[side]), "user skipped"
        for r in window[side]:
            if lst[r] != "U":
                assert (side, r) in kept[lst[r]], "a non-kept maker order admitted"
        last = max(window[side]) if window[side] else -1
        assert all(r in window[side] or r in skipped[side] for r in range(last + 1)), "order passed over"
    for m, k in kept.items():
        newest = max((key for key in seq if (sells if key[0] == "S" else buys)[key[1]] == m), key=lambda key: seq[key])
        assert newest in k, "maker's newest eligible order not kept"
    return window

def books_exhaustive():
    for n in (3, 4):
        owners = "UMN" if n == 3 else "UM"
        for sells in itertools.product(owners, repeat=n):
            for buys in itertools.product(owners, repeat=n):
                for seq in maker_seqs(sells, buys):
                    for W in (1, 2, 3):
                        yield sells, buys, W, seq

def books_random(trials, seed=5):
    rng = random.Random(seed)
    for _ in range(trials):
        sells = tuple(rng.choice("UMNP") for _ in range(rng.randint(0, 9)))
        buys = tuple(rng.choice("UMNP") for _ in range(rng.randint(0, 9)))
        keys = [("S", r) for r, o in enumerate(sells) if o != "U"] + [("B", r) for r, o in enumerate(buys) if o != "U"]
        order = list(range(len(keys))); rng.shuffle(order)
        yield sells, buys, rng.randint(1, 5), {k: order[i] for i, k in enumerate(keys)}

for label, it in (("exhaustive (3/side two makers, 4/side one maker, W=1..3)", books_exhaustive()),
                  ("random (up to 9/side, three makers, W=1..5)", books_random(200000))):
    n = against_intent = empty = 0
    for sells, buys, W, seq in it:
        n += 1
        check(sells, buys, W, seq)
        kept_all = decisions(sells, buys, seq)
        wwin = window_algorithm(sells, buys, W, seq)[0]
        bad = any(o != "U" and (s, r) not in kept_all[o] for s, l in (("S", sells), ("B", buys)) for r in wwin[s] for o in [l[r]])
        against_intent += bad
        w = walk(sells, buys, W, seq)[0]
        empty += any(not w[s] and any(o == "U" for o in l) for s, l in (("S", sells), ("B", buys)))
    print(f"{label}: {n} books: PASS (no maker on both sides, users never skipped, newest order kept)")
    print(f"   window-only rule would trade a maker order AGAINST the maker's newest intent in {against_intent} books")
    print(f"   all-eligible rule leaves a side empty while users wait in {empty} books")

sells, buys = ("M",), ("M", "M")
seq = {("B", 0): 10, ("S", 0): 11, ("B", 1): 12}
print("Example B1 #10, S1 #11, B2 #12, window 1:",
      "window-only admits", window_algorithm(sells, buys, 1, seq)[0], "| all-eligible admits", walk(sells, buys, 1, seq)[0])
s2 = ("M",) * 100 + ("U",) * 50; b2 = ("M",) * 100 + ("U",) * 50
q2 = {("S", r): 2 * r + 1 for r in range(100)}; q2.update({("B", r): 2 * r for r in range(100)})
w2 = walk(s2, b2, 100, q2)[0]
print(f"Crowding (100 crossed sells + 100 crossed buys, 50 users each side): sell window {len(w2['S'])}, buy window {len(w2['B'])} ({sum(b2[r]=='U' for r in w2['B'])} users)")
