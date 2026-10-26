"""Part A: exhaustive checks of the self-trade decision rule and the window refill.

A1. For every sequence of maker orders (sides in submission order) up to MAX_LEN,
    the two-maximum decision equals the literal submission-order replay, and the
    kept set is non-empty, one-sided, contains the newest order, and is exactly
    the final run of same-side orders.
A2. For every small book (owners per rank, per-maker submission orders) and window
    size, the window algorithm from ADR 0005 terminates and leaves no maker with
    orders on both sides; it never skips a user order; every crossed maker keeps
    at least one order; the windows are the first W non-skipped orders by rank.
"""
import itertools
import random
import sys

MAX_LEN = 16


def replay(sides):
    live = []
    for i, s in enumerate(sides):
        if not live or sides[live[0]] == s:
            live.append(i)
        else:
            live = [i]
    return set(live)


def two_max(sides):
    buys = [i for i, s in enumerate(sides) if s == "B"]
    sells = [i for i, s in enumerate(sides) if s == "S"]
    if not buys or not sells:
        return set(range(len(sides)))
    nb, ns = max(buys), max(sells)
    win, cut = ("B", ns) if nb > ns else ("S", nb)
    return {i for i, s in enumerate(sides) if s == win and i > cut}


def check_a1():
    cases = 0
    for n in range(1, MAX_LEN + 1):
        for combo in itertools.product("BS", repeat=n):
            cases += 1
            kept, rep = two_max(combo), replay(combo)
            assert kept == rep, (combo, kept, rep)
            assert kept, combo
            assert len({combo[i] for i in kept}) == 1, combo
            assert n - 1 in kept, combo
            start = min(kept)
            assert kept == set(range(start, n)), combo
            assert all(combo[i] == combo[n - 1] for i in kept), combo
            assert start == 0 or combo[start - 1] != combo[n - 1], combo
    return cases


def window_algorithm(sells, buys, W, seq):
    """sells/buys: owners by rank ('U' = user, else a maker id). seq[(side, rank)] =
    the maker's submission sequence. Returns (windows, decided, skipped, passes)."""
    sides = {"S": sells, "B": buys}
    window = {s: [] for s in sides}
    pos = {s: 0 for s in sides}
    decided = {}
    skipped = {s: set() for s in sides}

    def fill(side):
        lst = sides[side]
        while len(window[side]) < W and pos[side] < len(lst):
            r = pos[side]
            pos[side] += 1
            owner = lst[r]
            if owner != "U" and owner in decided:
                skipped[side].add(r)
                continue
            window[side].append(r)

    fill("S")
    fill("B")
    passes = 0
    while True:
        makers_s = {sells[r] for r in window["S"] if sells[r] != "U"}
        makers_b = {buys[r] for r in window["B"] if buys[r] != "U"}
        crossed = sorted((makers_s & makers_b) - set(decided))
        if not crossed:
            break
        passes += 1
        assert passes <= 10, "no termination"
        for m in crossed:
            mine = [("S", r) for r in window["S"] if sells[r] == m] + [("B", r) for r in window["B"] if buys[r] == m]
            nb = max(seq[o] for o in mine if o[0] == "B")
            ns = max(seq[o] for o in mine if o[0] == "S")
            win, cut = ("B", ns) if nb > ns else ("S", nb)
            kept = {o for o in mine if o[0] == win and seq[o] > cut}
            decided[m] = kept
            for side, r in mine:
                if (side, r) not in kept:
                    window[side].remove(r)
                    skipped[side].add(r)
        fill("S")
        fill("B")
    return window, decided, skipped, passes


def check_window(sells, buys, W, seq):
    window, decided, skipped, passes = window_algorithm(sells, buys, W, seq)
    sides = {"S": sells, "B": buys}
    ms = {sells[r] for r in window["S"] if sells[r] != "U"}
    mb = {buys[r] for r in window["B"] if buys[r] != "U"}
    assert not (ms & mb), ("maker on both sides", sells, buys, W, seq, window)
    for side, lst in sides.items():
        assert len(window[side]) <= W
        assert all(lst[r] != "U" for r in skipped[side]), "user skipped"
        assert all(lst[r] in decided for r in skipped[side]), "skip without decision"
        last = max(window[side]) if window[side] else -1
        for r in range(last + 1):
            assert r in window[side] or r in skipped[side], ("order passed over", side, r)
        if len(window[side]) < W:
            for r in range(len(lst)):
                assert r in window[side] or r in skipped[side], ("window short", side, r)
    for m, kept in decided.items():
        assert kept, "crossed maker lost every order"
        assert all(o[1] in window[o[0]] for o in kept), "kept order not in window"
    return passes


def maker_seqs(sells, buys):
    """Every assignment of submission order within each maker (makers are independent)."""
    orders = [("S", r, o) for r, o in enumerate(sells) if o != "U"] + [("B", r, o) for r, o in enumerate(buys) if o != "U"]
    by_maker = {}
    for side, r, o in orders:
        by_maker.setdefault(o, []).append((side, r))
    makers = sorted(by_maker)
    for perms in itertools.product(*(itertools.permutations(by_maker[m]) for m in makers)):
        seq = {}
        for perm in perms:
            for i, key in enumerate(perm):
                seq[key] = i
        yield seq


def check_a2(per_side, windows, owners):
    cases = 0
    multi_pass = 0
    for sells in itertools.product(owners, repeat=per_side):
        for buys in itertools.product(owners, repeat=per_side):
            for seq in maker_seqs(sells, buys):
                for W in windows:
                    cases += 1
                    if check_window(sells, buys, W, seq) > 1:
                        multi_pass += 1
    return cases, multi_pass


def check_a2_random(trials, seed=7):
    rng = random.Random(seed)
    multi = 0
    for _ in range(trials):
        n_s, n_b = rng.randint(0, 9), rng.randint(0, 9)
        owners = ["U", "M", "N", "P"]
        sells = tuple(rng.choice(owners) for _ in range(n_s))
        buys = tuple(rng.choice(owners) for _ in range(n_b))
        keys = [("S", r) for r, o in enumerate(sells) if o != "U"] + [("B", r) for r, o in enumerate(buys) if o != "U"]
        order = list(range(len(keys)))
        rng.shuffle(order)
        seq = {k: order[i] for i, k in enumerate(keys)}
        if check_window(sells, buys, rng.randint(1, 5), seq) > 1:
            multi += 1
    return multi


if __name__ == "__main__":
    print("A1 two-max == replay + kept properties:", check_a1(), "sequences, all lengths 1..%d: PASS" % MAX_LEN)
    c, mp = check_a2(3, (1, 2, 3), ("U", "M", "N"))
    print("A2 exhaustive windows (3 orders/side, makers M,N + users, W=1..3):", c, "cases: PASS;", mp, "needed a second pass")
    c, mp = check_a2(4, (1, 2, 3), ("U", "M"))
    print("A2 exhaustive windows (4 orders/side, maker M + users, W=1..3):", c, "cases: PASS;", mp, "needed a second pass")
    print("A2 random windows (up to 9/side, 3 makers, W=1..5): 200000 cases: PASS;", check_a2_random(200000), "needed a second pass")
    sys.exit(0)
