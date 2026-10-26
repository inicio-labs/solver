"""Part B: explicit-state model check of the order life cycle in ADR 0005.

Orders: B1 (maker M, buy, older), S1 (maker M, sell, newer), U1 (user buy).
Every interleaving of these events is explored (each at most the stated number of times):
  rounds (2), proof outcomes (transient failure, failing group held, pause/retry, ok),
  reservation commit or rollback, chain confirm or discard, a crash anywhere (1),
  maker cancels of B1 and of S1 (1 each), U1 spent elsewhere (1).
Database writes and the matcher message they publish happen together (the ordered stream).

Invariants, checked in every reachable state:
  I1  B1 stopped by the rule only if a reservation committed while B1 was live
  I2  at most one CANCELLED event for B1, present iff the rule stopped B1
  I3  SettlementPending(S1) comes before CANCELLED(B1)
  I4  CANCELLED(B1) never follows the maker's own cancel of B1
  I5  the matcher never treats a non-live order as active
  I6  no settlement fills two orders of the same maker on opposite sides
and in every state where the executor holds nothing:
  I7  the matcher holds nothing (no leak)
  I8  every live order is active in the matcher (nothing lost)
Mutations re-run the check with one design rule broken; each must be caught.
"""
from collections import deque
import sys

ORDERS = ("B1", "S1", "U1", "S1r")
LINEAGE = {"B1": "B1", "S1": "S1", "S1r": "S1", "U1": "U1"}


def initial():
    return dict(
        db=dict(B1="active", S1="active", U1="active", S1r="none"),
        stopped=frozenset(), stp_stopped=False, events=(),
        mt=dict(B1="active", S1="active", U1="active", S1r="absent"),
        phase="idle", inputs=(), cancels=(), pending_cancelled=(), committed=False,
        rounds=0, crash=0, mc_b1=0, mc_s1=0, spend=0, pause=0, hold=0, transient=0,
        maker_first=False, stp_commit=False,
    )


def freeze(s):
    return tuple((k, tuple(sorted(v.items())) if isinstance(v, dict) else v) for k, v in sorted(s.items()))


def thaw(t):
    return {k: dict(v) if k in ("db", "mt") else v for k, v in t}


def live(s, o):
    return s["db"][o] == "active" and LINEAGE[o] not in s["stopped"]


def give_back(s, mut):
    held = list(s["inputs"]) + ([] if "giveback_omits_sitouts" in mut else list(s["cancels"]))
    for o in held:
        if s["db"][o] == "spent":
            s["mt"][o] = "absent"
        elif live(s, o):
            s["mt"][o] = "active"
    s.update(phase="idle", inputs=(), cancels=())


def transitions(s, mut):
    out = []

    def step(label, f):
        n = thaw(freeze(s))
        f(n)
        out.append((label, n))

    if s["phase"] == "idle" and s["rounds"] < 2:
        def rnd(n):
            n["rounds"] += 1
            a = {o for o in ORDERS if n["mt"][o] == "active"}
            crossed = "B1" in a and ("S1" in a or "S1r" in a)
            seller = "S1" if "S1" in a else ("S1r" if "S1r" in a else None)
            if seller and "U1" in a:
                n["inputs"] = (seller, "U1")
                n["cancels"] = ("B1",) if crossed else ()
                n["committed"] = False
                for o in n["inputs"] + n["cancels"]:
                    n["mt"][o] = "held"
                n["phase"] = "batch"
                if "cancel_at_selection" in mut and crossed:
                    n["stopped"] = n["stopped"] | {"B1"}
                    n["stp_stopped"] = True
                    n["events"] = n["events"] + (("CANCELLED", "B1"),)
        step("round", rnd)

    if s["phase"] == "batch":
        if s["transient"] < 1:
            step("proof fails (transient)", lambda n: (n.update(transient=n["transient"] + 1), give_back(n, mut)))
        if s["hold"] < 1:
            step("group keeps failing: held", lambda n: n.update(phase="held", hold=n["hold"] + 1))
        if s["pause"] < 1:
            step("executor paused", lambda n: n.update(phase="paused", pause=n["pause"] + 1))

        def reserve(n):
            if all(live(n, o) for o in n["inputs"]):
                seller = n["inputs"][0]
                for o in n["inputs"]:
                    n["db"][o] = "reserved"
                n["events"] = n["events"] + (("SettlementPending", seller),)
                cancelled = []
                for c in n["cancels"]:
                    if live(n, c) or "no_live_check" in mut:
                        n["stopped"] = n["stopped"] | {LINEAGE[c]}
                        n["stp_stopped"] = True
                        n["stp_commit"] = True
                        n["events"] = n["events"] + (("CANCELLED", c),)
                        cancelled.append(c)
                n["pending_cancelled"] = tuple(cancelled) if "remember_stopped_only" in mut else tuple(n["cancels"])
                n["committed"] = True
                n["phase"] = "pending"
            else:
                give_back(n, mut)
        step("reserve", reserve)
    if s["phase"] == "held":
        step("hold over: give back", lambda n: give_back(n, mut))
    if s["phase"] == "paused":
        step("verified: retry", lambda n: n.update(phase="batch"))

    if s["phase"] == "pending":
        def finish(n, confirmed):
            seller, user = n["inputs"]
            if confirmed:
                n["db"][seller] = "consumed"
                n["db"][user] = "consumed"
                n["mt"][seller] = "absent"
                n["mt"][user] = "absent"
                n["db"]["S1r"] = "active"
                n["mt"]["S1r"] = "active" if live(n, "S1r") else "absent"
            else:
                for o in (seller, user):
                    if n["db"][o] == "reserved":
                        n["db"][o] = "active"
                    n["mt"][o] = "active" if live(n, o) else "absent"
            n["events"] = n["events"] + (("SettlementResolved", seller),)
            if "no_removed_on_finish" not in mut:
                for c in n["pending_cancelled"]:
                    n["mt"][c] = "absent"
            n.update(phase="idle", inputs=(), cancels=(), pending_cancelled=())
        if s["db"]["U1"] != "spent":
            step("chain confirms", lambda n: finish(n, True))
        step("chain rejects / expires", lambda n: finish(n, False))

    if s["crash"] < 1:
        def crash(n):
            n["crash"] += 1
            for o in ORDERS:
                n["mt"][o] = "active" if live(n, o) else "absent"
            if n["phase"] == "pending":
                for o in n["inputs"]:
                    if n["db"][o] == "reserved":
                        n["mt"][o] = "absent"
                n["pending_cancelled"] = ()
            else:
                n.update(phase="idle", inputs=(), cancels=())
        step("crash + restart", crash)

    if s["mc_b1"] < 1:
        def cancel_b1(n):
            n["mc_b1"] += 1
            if not n["stp_stopped"]:
                n["maker_first"] = True
            n["stopped"] = n["stopped"] | {"B1"}
            n["mt"]["B1"] = "absent"
        step("maker cancels B1", cancel_b1)
    if s["mc_s1"] < 1:
        def cancel_s1(n):
            n["mc_s1"] += 1
            n["stopped"] = n["stopped"] | {"S1"}
            for o in ("S1", "S1r"):
                n["mt"][o] = "absent"
        step("maker cancels S1", cancel_s1)
    if s["spend"] < 1 and s["db"]["U1"] in ("active", "reserved"):
        def spend(n):
            n["spend"] += 1
            n["db"]["U1"] = "spent"
            n["mt"]["U1"] = "absent"
        step("U1 spent elsewhere", spend)
    return out


def violations(s):
    v = []
    ev = s["events"]
    cancels = [i for i, e in enumerate(ev) if e == ("CANCELLED", "B1")]
    if s["stp_stopped"] and not s["stp_commit"]:
        v.append("I1 B1 stopped by the rule without a committed reservation")
    if len(cancels) > 1 or (len(cancels) == 1) != s["stp_stopped"]:
        v.append("I2 CANCELLED(B1) count does not match the rule's stop")
    if cancels:
        pend = [i for i, e in enumerate(ev) if e[0] == "SettlementPending"]
        if not pend or pend[0] > cancels[0]:
            v.append("I3 CANCELLED before SettlementPending")
    if cancels and s["maker_first"]:
        v.append("I4 CANCELLED after the maker's own cancel")
    if "B1" in s["inputs"] and any(o in s["inputs"] for o in ("S1", "S1r")):
        v.append("I6 a settlement fills maker M on both sides")
    for o in ORDERS:
        if s["mt"][o] == "active" and not live(s, o):
            v.append(f"I5 matcher has non-live {o} active")
    if s["phase"] in ("idle",):
        for o in ORDERS:
            if s["mt"][o] == "held":
                v.append(f"I7 matcher still holds {o} with nothing in flight")
            if live(s, o) and s["mt"][o] != "active":
                v.append(f"I8 live {o} missing from the matcher")
    return v


def explore(mut=frozenset()):
    start = initial()
    seen = {freeze(start): None}
    parent = {}
    q = deque([start])
    while q:
        s = q.popleft()
        bad = violations(s)
        if bad:
            trace, k = [], freeze(s)
            while parent.get(k):
                k, label = parent[k]
                trace.append(label)
            return len(seen), bad, list(reversed(trace))
        for label, n in transitions(s, mut):
            k = freeze(n)
            if k not in seen:
                seen[k] = 1
                parent[k] = (freeze(s), label)
                q.append(n)
    return len(seen), [], []


if __name__ == "__main__":
    states, bad, trace = explore()
    print(f"Correct design: {states} reachable states explored, violations: {len(bad)}")
    if bad:
        print("  ", bad, "\n   trace:", " → ".join(trace))
        sys.exit(1)
    states, bad, trace = explore(frozenset(["remember_stopped_only"]))
    print(f"Variant 'executor remembers only the cancels the reservation stopped': {states} states, violations: {len(bad)}")
    for mut, meaning in [
        ("no_removed_on_finish", "confirm/discard message omits the cancelled sit-outs"),
        ("cancel_at_selection", "cancel when the matcher selects, not at reservation"),
        ("giveback_omits_sitouts", "give-back returns only the inputs"),
        ("no_live_check", "reservation cancels without the live check"),
    ]:
        states, bad, trace = explore(frozenset([mut]))
        status = "CAUGHT" if bad else "MISSED"
        print(f"Mutation '{meaning}': {status}: {bad[0] if bad else ''}")
        if bad:
            print("   shortest trace:", " → ".join(trace))
