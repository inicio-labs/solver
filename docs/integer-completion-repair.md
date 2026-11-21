# Optional integer completion repair

`engine.clearing_integer_completion_repair = false` is the default. Existing
configuration files omit the field safely. Enabling it permits one bounded
neighboring allocation after the original maximum-volume plan is insolvent.
At a distinct bounded walk price, the legacy plan must produce an execution
batch before repair can upgrade it at that price. A walk skip or error falls
through to mid once. At terminal mid, repair may also recover an eligible
legacy skip against a zero reference. Source errors retain their legacy path.

For example, at price1.5, a note offering5quote for3base against a maker
10base/14quote currently receives2base and leaves2quote/1base. The repair can
pay that maker5quote to release3base and complete the user. The maker keeps
7base/9quote in its remainder. The final2/1 tail can also complete against8/11.
The repaired pair pays only assets released by its own admitted notes.

The first post-resource-trim anchor, price, ordering and admitted set stay fixed.
Only one already-positive donor changes. For missing release `d`, offered `O`,
current requested minimum `R` and old release `r`, derive
`T = ceil((r+d)*R/O)`. Try the smallest comparison allocation reaching T;
canonical full completion uses the existing maximum/full-price/capped-fee
formula and must still pay **exactly T**. A canonical jump above T is rejected.
There is no unconditional donor-maximum candidate, second donor, new input,
alternate price/anchor, subsidy or recursive search.

Every candidate preserves all legacy participants' comparison, payment,
release and completion; equals the existing priority allocator at its new
target; meets current minimums and asset bounds; funds every nominal protocol
fee and preserves legacy realized fees; and has exact independent pair funding.
The winner passes the existing execution-batch conversion, PSWAP model,
executor preparation/request and indivisible-group cap guards before acceptance.
The pure preflight uses an input sender as a structural consumer identity;
the executor repeats preparation with its real account and performs its usual
admission/reservation/fee-headroom checks. Preflight is not VM or node proof.

The policy deliberately allows departure from `S=floor(P*B)`, spending some
rounding surplus. It ranks greatest buyer comparison total, least coupling
deviation, greatest payment vector then release vector (canonical sells then
buys), lowest donor index and smallest donor comparison. It preserves the
source allocator's fixed-target priority, rather than claiming global optimality.

Limits are200 post-trim orders,100 donor-side orders, configured interval cap
50000 and100 candidate attempts,20000 speculative execution evaluations and10000
priority visits. Maps are reused; no per-candidate rebuild occurs. The execution budget reserves the anchor regeneration and up to three
threshold evaluations plus a full candidate vector per donor. A100-donor
maximum-size search may therefore hit the work cap and fall back; the final
selected-winner request preflight is separately bounded. Incomplete/capped search, invalid anchor, no winner or failed winner
validation returns the exact legacy result. Individual invalid candidates are
rejected while enumeration continues. The limits do not establish a latency SLO.

The donor-payment increase bound `ceil(d*R/O)` is relative to the insolvent
anchor, **not total economics versus the shaved fallback**. Tokenwise anchor,
fallback and candidate payments, releases, fee targets, fees and residuals are
logged; anchor residuals can be negative. A token quantum can be economically
material. This policy does not guarantee filling every feasible trade or every
historical remainder. The124+14/139 regression reconstructs actual public note
arithmetic; its final counterparty is explicitly synthetic.

Aggregate quote estimates remain approximate. The same-book regression has an
earlier1000/995 order requiring600 requested units, a later200/199 probe and
opposite500/499 liquidity. Actual clearing skips the unavailable minimum and
completes the probe, while the aggregate estimator says none, in both directions.
This patch changes no HTTP endpoints, wait budgets or estimator API. Shared
order-level quote preview and wallet/monitor migration are separate work.

Keep the flag off until unit/property, DB/private-maker/recovery, MockChain/VM,
local-node and worst-shape performance gates pass on the attested build/config.
Then use controlled shadow comparison and an owner-supervised pair/cohort.
Stop for any funding, payment/fee, priority, STP/cancel/reservation/reconciliation
regression or work/latency overrun. Disable the flag for **new planning**; let
reserved/submitted work reconcile normally. Disabling cannot undo committed
fills. Keep the prior verified binary/config available for rollback; no schema
migration or reservation behavior change is involved.
