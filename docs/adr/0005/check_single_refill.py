"""Compare three window variants: loop (refill + repeat), single refill that skips any maker
already on the other side, and no refill. Checks the safety properties on every book and
measures the cost of each simplification."""
import itertools, random
from check_rule import maker_seqs, window_algorithm

def decide(mine, seq):
    nb = max(seq[o] for o in mine if o[0] == "B"); ns = max(seq[o] for o in mine if o[0] == "S")
    win, cut = ("B", ns) if nb > ns else ("S", nb)
    return {o for o in mine if o[0] == win and seq[o] > cut}

def variant(sells, buys, W, seq, refill):
    sides = {"S": sells, "B": buys}
    window = {s: list(range(min(W, len(sides[s])))) for s in sides}
    pos = {s: len(window[s]) for s in sides}
    skipped = {s: set() for s in sides}
    ms = {sells[r] for r in window["S"] if sells[r] != "U"}; mb = {buys[r] for r in window["B"] if buys[r] != "U"}
    decided = {}
    for m in sorted(ms & mb):
        mine = [("S", r) for r in window["S"] if sells[r] == m] + [("B", r) for r in window["B"] if buys[r] == m]
        kept = decide(mine, seq); decided[m] = kept
        for side, r in mine:
            if (side, r) not in kept:
                window[side].remove(r); skipped[side].add(r)
    if refill:
        for side, other in (("S", "B"), ("B", "S")):
            lst = sides[side]
            while len(window[side]) < W and pos[side] < len(lst):
                r = pos[side]; pos[side] += 1; o = lst[r]
                if o != "U" and (o in decided or o in {sides[other][x] for x in window[other]}):
                    skipped[side].add(r); continue
                window[side].append(r)
    return window, decided, skipped

def check(sells, buys, W, seq, refill):
    window, decided, skipped = variant(sells, buys, W, seq, refill)
    sides = {"S": sells, "B": buys}
    ms = {sells[r] for r in window["S"] if sells[r] != "U"}; mb = {buys[r] for r in window["B"] if buys[r] != "U"}
    assert not (ms & mb), "maker on both sides"
    for side, lst in sides.items():
        assert all(lst[r] != "U" for r in skipped[side]), "user skipped"
    for m, kept in decided.items():
        assert kept and all(o[1] in window[o[0]] for o in kept)
    return sum(len(v) for v in window.values())

def compare(cases_iter):
    n = fewer_single = fewer_none = 0; empty_side_none = empty_side_single = 0
    for sells, buys, W, seq in cases_iter:
        n += 1
        full = sum(len(v) for v in window_algorithm(sells, buys, W, seq)[0].values())
        s1 = check(sells, buys, W, seq, True); s0 = check(sells, buys, W, seq, False)
        fewer_single += s1 < full; fewer_none += s0 < full
        w1 = variant(sells, buys, W, seq, True)[0]; w0 = variant(sells, buys, W, seq, False)[0]
        has_users = {s: any(o == "U" for o in l) for s, l in (("S", sells), ("B", buys))}
        empty_side_single += any(not w1[s] and has_users[s] for s in w1)
        empty_side_none += any(not w0[s] and has_users[s] for s in w0)
    return n, fewer_single, fewer_none, empty_side_single, empty_side_none

def exhaustive(per_side, owners, windows):
    for sells in itertools.product(owners, repeat=per_side):
        for buys in itertools.product(owners, repeat=per_side):
            for seq in maker_seqs(sells, buys):
                for W in windows:
                    yield sells, buys, W, seq

def rand(trials, seed=11):
    rng = random.Random(seed)
    for _ in range(trials):
        sells = tuple(rng.choice("UMNP") for _ in range(rng.randint(0, 9)))
        buys = tuple(rng.choice("UMNP") for _ in range(rng.randint(0, 9)))
        keys = [("S", r) for r, o in enumerate(sells) if o != "U"] + [("B", r) for r, o in enumerate(buys) if o != "U"]
        order = list(range(len(keys))); rng.shuffle(order)
        yield sells, buys, rng.randint(1, 5), {k: order[i] for i, k in enumerate(keys)}

def attack(W=100, users=50):
    sells = ("M",) * W + ("U",) * users; buys = ("M",) * W + ("U",) * users
    seq = {("S", r): 2 * r + 1 for r in range(W)}; seq.update({("B", r): 2 * r for r in range(W)})
    for name, refill in (("single refill", True), ("no refill", False)):
        w = variant(sells, buys, W, seq, refill)[0]
        print(f"  attack, {name}: sell window {len(w['S'])} orders, buy window {len(w['B'])} orders"
              f" ({sum(buys[r] == 'U' for r in w['B'])} users)")

for label, it in (("exhaustive 3/side, makers M,N + users, W=1..3", exhaustive(3, "UMN", (1, 2, 3))),
                  ("random up to 9/side, 3 makers, W=1..5", rand(200000))):
    n, f1, f0, e1, e0 = compare(it)
    print(f"{label}: {n} cases, both variants safe (no maker on both sides, users never skipped)")
    print(f"  single refill: fewer orders than the loop in {f1} cases; a side left empty despite waiting users in {e1}")
    print(f"  no refill:     fewer orders than the loop in {f0} cases; a side left empty despite waiting users in {e0}")
attack()
