# PSWAP settlement wedge — root-cause report (attachment-stripped store copy)

**Status:** root cause identified and evidenced end-to-end. Not yet fixed in production.
**Component:** Miden solver `executor` + `miden-client` input-note store (PSWAP notes).
**Severity:** High — settlement is fully wedged; the executor retries the same batch every ~5s and never makes progress.

---

## 1. TL;DR

The solver's executor cannot settle its batch. Every attempt fails the Miden transaction kernel with **`InputNoteNotInBlock`**.

The note the solver *hands the kernel is correct* (it carries its PSWAP attachment). But the executor's **miden-client note store holds a corrupted copy of that same note with its attachments stripped** (empty). miden-client prefers the stored (authenticated) copy over the provided one. Because **a PSWAP note's `NoteId` commits to its attachments**, the attachment-stripped body hashes to a **different, non-existent `NoteId`**, which then fails to validate against the real note's on-chain inclusion proof → `InputNoteNotInBlock`.

Two notes are affected, both **created by the solver itself** (sender = solver account). The corrupt copies sit only in the executor's client store; the solver's own order-book copy and the on-chain note are both fine.

---

## 2. Environment

- Network: Miden **devnet** (`rpc.devnet.miden.io`), forkchoice devnet, current tip ~359,650.
- Solver account: `AccountIdV1 { suffix: 1029722055273328640, prefix: 9065804163806743057 }`.
- Affected blocks: **354450** and **356150**.
- Executor client store: `solver_executor.sqlite3`.
- miden-client: `vaibhav/pswap` branch (PSWAP feature), `miden-protocol`/`miden-standards` 0.15.x.

---

## 3. Symptom

Executor loops every ~5s producing a 6-note batch and failing:

```
ERROR solver::executor::executor: batch execution failed
  error=submit failed (tx error, classified): transaction execution failed notes=6
  consumed_count=0 refed_count=6
```

`consumed_count=0` means the post-failure nullifier check finds **none** of the notes spent — so the existing cleanup (which only prunes nullified notes) never removes the offender, and the batch repeats forever.

---

## 4. The exact kernel error (verbatim, full chain)

```
transaction execution failed
TransactionExecutorError(
    InvalidTransactionInputs(
        InputNoteNotInBlock(
            NoteId(Word([18357760816833035238, 16705090006100391018,
                         11217406515388485723, 5843166100726472839])),
            BlockNumber(356150),
        ),
    ),
)
  caused by: failed to create transaction inputs
  caused by: input note with id
             0xe6fbea0633dec3fe6ab48a6ada66d4e75b10e21acb3bac9b87e067359c181751
             was not created in block 356150
```

Raised in `miden-protocol` `validate_is_in_block` (`transaction/inputs/mod.rs`), which verifies
`note.id()` against the committing block's note-root via the inclusion proof. The rejected id
`0xe6fbea06…` **does not exist on-chain** (see §6).

---

## 5. Root cause

For each note in the batch the executor logs (a) the note it is consuming (rebuilt from its own
order-book raw bytes) and (b) the copy in its miden-client store, and compares them:

| note | consuming (order book) | store copy | match |
|---|---|---|---|
| `0xc021f600…08da6a4d` (blk 356150 / idx 1) | `attachments_count = 1` ✅ | `attachments_count = 0` ❌ | **ATTACHMENTS MISMATCH** |
| `0x858deb25…5623e167` (blk 354450 / idx 0) | `attachments_count = 1` ✅ | `attachments_count = 0` ❌ | **ATTACHMENTS MISMATCH** |

Both are PSWAP-scheme attachments (`NoteAttachmentScheme(3)`), sender = the solver account, i.e.
**solver-created** payback/remainder notes. The store copies are `Committed` with a **valid,
canonical inclusion proof** (verified against the live node, §6) but an **empty `attachments`
body**, while their metadata still commits to the *real* attachments. They are internally
inconsistent rows.

The four other batch notes (mirror counters `0xf2e17f16…`, `0xe0cb4c1e…`, `0x83444f94…`,
`0xfdae52eb…`) are **not in the store** → consumed unauthenticated → fine.

---

## 6. On-chain verification (live node)

`rpc.Api/GetNotesById` for the two affected notes:

- `0xc021f600…` **exists** on-chain at **block 356150, index 1**, with an inclusion proof
  **byte-identical** to the one stored locally, and **carries its 36-byte attachment**.
- `0x858deb25…` **exists** at **block 354450, index 0**, same story.
- The rejected ids `0xe6fbea06…` and `0x152a30ec…` (see §7) **do not exist on-chain at all.**

`GetBlockHeaderByNumber` for 354450 / 356150: the `note_root` returned by the node is
**byte-identical** to the `block_note_root` stored in the local `Committed` records → **no reorg**,
the proofs are anchored to canonical blocks. The problem is purely the note **body**, not the proof
and not the chain.

---

## 7. Mechanism (why the kernel rejects)

1. A PSWAP note's `NoteId` **commits to its attachments** (via the metadata attachments-commitment).
   So the *same* details with vs. without attachments yield **different** `NoteId`s.
2. The on-chain note `0xc021f600…` was committed **with** its attachment.
3. The executor store holds `0xc021f600…` (keyed by its details) but with an **empty** attachment
   body. That stripped body hashes to **`0xe6fbea06…`** (= `c021f600` minus its attachment).
   (`858deb25` minus its attachment = `0x152a30ec…`; the live error alternates between the two.)
4. miden-client's `submit_new_transaction` builds the authenticated input note **from the stored
   record**, not the note the solver passed in (`build_input_notes` prefers a store record whose
   `is_authenticated()`).
5. The kernel recomputes `note.id()` from that empty-attachment body → `0xe6fbea06…`, then runs the
   inclusion proof (which is for the real `0xc021f600…` at 356150/idx 1).
   `e6fbea06 ≠ c021f600` → `InputNoteNotInBlock(e6fbea06, 356150)`.

End to end: **correct note in the order book → attachment-stripped copy in the executor's client
store → kernel re-hashes the stripped body to a phantom id → rejects it against the real note's
proof.**

---

## 8. What was ruled out (with evidence)

- **Reorg / stale proof** — node `note_root` == stored `block_note_root` for both blocks (§6). Not a reorg.
- **PSWAP fill-math / on-chain assert** — the error is `InputNoteNotInBlock` (input validation), not a MASM `assert.err`.
- **Solver note (de)serialization** — `Note::write_into`/`read_from` *do* serialize attachments; the order-book copy is correct (`attachments_count = 1`). Not the solver's raw-bytes round-trip.
- **The standard maker flow in miden-client** — the existing end-to-end test `pswap_chain_tracking_test` asserts payback/remainder attachments survive a re-sync `INSERT OR REPLACE` and **passes**. So the *standard* observer+screener path preserves attachments.

So the corruption is **specific to how these rows entered the executor's store**, not a general client bug.

---

## 9. Open question for the team (the one unproven link)

**How did the executor's client store come to hold these two notes as `Committed` with an empty
attachment body, when the on-chain note and the order-book copy both have the attachment?**

Established facts that bound the search:
- The executor's miden-client sync is **tagless** (it discovers no notes via tags), so these rows
  did **not** come from normal tag-driven sync of a third party's note.
- The notes are **solver-created** (sender = solver). They enter the executor's store via the
  executor's **own transaction processing** (it creates payback/remainder as outputs, applies its
  own tx optimistically without pulling chain state, and later consumes the remainder).
- The store keys input notes by **`details_commitment` (recipient + assets) — which excludes
  attachments**. We proved (unit test, see §11) that a full-row `INSERT OR REPLACE` of an existing
  note with an empty attachment set **silently wipes** the attachments. So *any* write path that
  re-inserts the row with `NoteAttachments::empty()` corrupts it.

Most likely candidates to audit (in priority order):
1. The **output→input** transition for the solver's own created notes (does the expected-output /
   committed-input record carry the attachment bytes, or only the metadata commitment?).
2. Any `InputNoteRecord` built with `NoteAttachments::empty()` for a note whose metadata commits to
   attachments (e.g. a state transition that "just gained its metadata" → routed to the full-row
   `batch_insert_input_notes` rather than the attachment-preserving state `UPDATE`).
3. Whether this was produced by a **pre-0.15.3 binary** (before the `AssetCallbackFlag` / attachment
   hardening fixes) and the corrupt rows simply persist in the live DB.

---

## 10. Fix recommendations

**Immediate unblock (operational):**
- Consume these notes **unauthenticated** in the executor (don't let miden-client substitute the
  stored copy) — the order-book note is correct, so the kernel/node then sees the right body. **Or**
  purge the two corrupt rows (`c021f600`, `858deb25`) from `solver_executor.sqlite3` and redeploy
  from current `vaibhav/pswap`.
- Also extend the executor's post-failure cleanup: `InputNoteNotInBlock` /
  `InputNoteBlockNotInPartialBlockchain` should mark the order terminal, not silently re-feed it
  (today only nullified notes are pruned, so the offender loops forever).

**Proper fix (miden-client):**
- Never let a full-row `INSERT OR REPLACE` overwrite an existing note's `attachments` with an empty
  set — route existing notes through the attachments-preserving `UPDATE`, or `COALESCE`/merge the
  attachment column. (The store keys by `details_commitment`, which excludes attachments, so the
  attachment column must be protected explicitly.)
- Audit the output→input / commit transition for solver-created notes to ensure the attachment
  bytes are carried, not just the metadata commitment.

---

## 11. How to reproduce / verify

- **Store-level (proves the clobber):** `crates/sqlite-store/src/note/tests.rs ::
  full_row_reinsert_clobbers_attachments` — store a note with 1 attachment, re-insert the same
  note (same `details_commitment`) with an empty set, read back → attachments are **0** (clobbered).
- **End-to-end (standard flow is fine):** `crates/testing/miden-client-tests/src/tests.rs ::
  pswap_chain_tracking_test` (public + private) — asserts payback/remainder attachments survive a
  re-sync `INSERT OR REPLACE`; **passes** today.
- **Live evidence:** the executor now logs a full per-note diagnostic on failure
  (`log_batch_consume_diagnostics` in `crates/solver/src/executor/executor.rs`): id, details
  commitment, serial, nullifier (+ on-chain consumed check), attachments (count + content), assets,
  offered/requested, the store copy, the consumed-vs-store attachments match, and the complete VM
  error. Re-run / tail `journalctl -u miden-solver` to see it.

---

## 12. Key code references

- Rejection site: `miden-protocol` `transaction/inputs/mod.rs` → `validate_is_in_block` (note.id vs proof).
- Error enum: `miden-protocol` `errors/mod.rs` → `TransactionInputError::InputNoteNotInBlock(NoteId, BlockNumber)`
  — *"input note with id {0} was not created in block {1}"*.
- Store-vs-provided selection: `miden-client` `transaction/mod.rs` (`get_input_notes` →
  `retain(is_authenticated)` → `build_input_notes`) and `transaction/request/builder.rs`
  (`input_notes` — present-proof ⇒ authenticated, else unauthenticated).
- Full-row clobber: `sqlite-store` `note/mod.rs` → `apply_note_updates_tx` (`Insert`/`InsertCommitted`
  → `batch_insert_input_notes` = `INSERT OR REPLACE`; `Update` = state-only, preserves attachments).
- PSWAP reconstruction (correct): `miden-standards` `note/pswap.rs` → `payback_note` / `remainder_note`
  (both attach via `Note::with_attachments(...)`).
- Note serialization (preserves attachments): `miden-protocol` `note/mod.rs` →
  `impl Serializable for Note` writes `attachments`.
```
