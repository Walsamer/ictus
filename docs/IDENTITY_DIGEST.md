# The identity digest (protocol v1)

The digest that binds an identity to immutable content is **part of the
interface between Ictus and its consumers**, not workflow policy and not Work
Order lifecycle logic. Ictus **produces and owns** it: this document and
`contracts/digest-vectors.json` are the contract. A consumer (Tactus, or any
future controller) **implements it themselves** and asserts our published
vectors.

There is deliberately **no shared runtime library**. Ictus and its consumers
stay independently implementable; what they share is this versioned contract.
Duplicated implementations are not a defect to be removed — an implementation
that is *checked against the same artefact* is the design.

| | Owner |
| --- | --- |
| The algorithm, the digested field sets, the vectors | Ictus (`docs/IDENTITY_DIGEST.md`, `contracts/digest-vectors.json`) |
| Implementation in Ictus | Rust `ictus_core::digest`; Python `ictus_dagster.submission.intent_digest` |
| Implementation in a consumer | the consumer |
| Proof they agree | `crates/core/tests/digest_vectors.rs`, `tests/python/test_digest_vectors.py` |

```text
Ictus ──produces──▶ contracts/digest-vectors.json ◀──asserts── consumer
        (owns the algorithm and the vectors)      (implements it independently)
```

---

## 1. The canonical form

The digest covers the **canonical JSON** of a payload: CPython's

```python
json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
```

encoded as UTF-8. Concretely:

- **Object keys are sorted by Unicode code point**, ascending. Code-point order
  is what `sorted(dict)` and a UTF-8 byte comparison both produce; it is *not*
  UTF-16 code-unit order (an astral key such as `U+1F600` sorts after `U+FFFD`,
  not before it). Two objects with the same pairs in a different insertion order
  are the same canonical bytes.
- **No whitespace.** Items are separated by `,` and a key from its value by `:`.
- **Every character outside `0x20..=0x7e` is escaped** as `\uXXXX` with
  lowercase hex. That covers control characters (`\u0000`), `DEL` (`\u007f`),
  accented and CJK text (`\u00e9`, `\u65e5`), and astral characters, which become
  a **surrogate pair** (`\ud83d\ude00`). The seven short escapes
  `\" \\ \b \f \n \r \t` are used where JSON defines them, and `/` is never
  escaped.
- **Numbers** are written in CPython's `repr` form: an integer as its decimal
  digits; a double as the shortest decimal string that round-trips, choosing
  among equally short spellings the one closest to the exact value (ties to the
  even last digit) — that is what CPython's `dtoa` and `ryu` both produce, and
  it is *not* interchangeable with any other shortest-round-trip formatter:
  `-847472097840887.2` also has the equally short spelling
  `-847472097840887.3`, and two implementations picking differently would digest
  different bytes for one value. The string is written positionally when the
  scientific exponent `e` (value = `d.ddd × 10^e`) satisfies `-4 <= e < 16`, and
  in exponent form with a sign and at least two digits otherwise: `1.0`, `0.1`,
  `0.0001` (e = -4), `1e-05` (e = -5), `1e+16` (e = 16), `1e+300`,
  `1.7976931348623157e+308`, `-0.0`. `1` and `1.0` are **different payloads**
  and digest differently.
- **Literals**: `null`, `true`, `false`; the empty object `{}` and the empty
  array `[]` are written as such.

The hash is the **lowercase hex SHA-256** of those UTF-8 bytes.

### The number domain — fail closed, never approximate

A digest is computed only over values every implementation can represent
exactly:

- integers in **−2^63 … 2^64−1** inclusive (a JSON integer);
- finite IEEE-754 doubles, written as above;
- strings, booleans, `null`, arrays, and objects whose keys are strings.

Anything else is **outside the protocol** and must be refused with a typed error
naming the offending field, never hashed as the nearest representable value:

| Outside the domain | Why it must fail closed |
| --- | --- |
| an integer below −2^63 or above 2^64−1 | one parser keeps it exact, another rounds it to a double; the two sides would digest different payloads, and the digest would look valid |
| a non-finite number (`NaN`, `Infinity`, a literal that overflows to infinity) | it has no canonical JSON form |
| a number literal that only *rounds* to its double | the text was written with digits the value cannot hold, so a consumer holding the same text cannot tell which digits mattered |
| a value that cannot be written back exactly | the writer would be inventing bytes |

**A number literal is in the contract only if it is exactly the value it
denotes.** The literal must equal, as a number, the shortest round-trip form of
the double it parses to: `0.10`, `1e-05` and `1E5` are their values and pass,
while a literal with more significant digits than the double holds only *rounds*
— 25 significant digits for a value that needs 17 — and is refused with a reason
of its own, `number-would-round`. The comparison is numeric, not textual, so a
spelling that denotes the same value is never a refusal, and a rounded value is
never accepted as a substitute for the number that was written. A literal that
overflows to infinity is `non-finite`, not a rounding case.

Both implementations therefore enforce the domain *before* hashing. Three
things can only be judged on the **document text**: an integer a parser cannot
hold, a literal that only rounds, and the `NaN`, `Infinity` and `-Infinity`
spellings, which a permissive parser accepts as numbers. So each ingress judges
the text before a value exists — Rust scans it, Python parses it with hooks that
refuse a literal — and reports the JSON path of the offending number (e.g.
`$.facts[0].value`), rather than handing the document to a parser whose dialect
is wider than ours. Once a document is parsed, the digest writer still refuses
an out-of-domain integer or a non-finite float it is handed; what it can no
longer judge is a literal's spelling, which only the text had. A producer that
needs a number outside the domain encodes it as a string.

Duplicate object keys are also outside the protocol: the canonical form is
defined over the **parsed** object, and parsers disagree about which duplicate
survives. A producer must not emit them.

### Which fields are digested

For every subject the digest covers **the complete serialized contract object**,
exactly as Ictus emits it. A consumer digests the payload **it received** —
adding, removing, defaulting or reordering a field is what makes two
implementations disagree.

| Subject | Digested | Deliberately not digested |
| --- | --- | --- |
| **StateSnapshot** (content digest, `snapshot.digest`) | the whole `StateSnapshot`: `schema_version`, `snapshot_id`, `timestamp`, `domain`, `subject`, `facts` (each `key` + open `value`), `capabilities`, `constraints` | the digest itself (no self-reference); a digest supplied by a producer — Ictus recomputes its own over the snapshot it received and never trusts an input digest |
| **DecisionProposal** (`validation.proposal_digest`, repeated in `intent.policy_context.proposal_digest`) | the whole `DecisionProposal` as serialized: `schema_version`, `proposal_id`, `decision`, `subject`, `capability`, `route`, `arguments`, `provider`, `reason`, `evidence`, `proposed_at` | absent optional fields: the wire form omits `capability`, `route` and `reason` when they are absent and `arguments` when it is empty, so they are **not** digested either — do not materialize them. Also excluded: the envelope that binds the digest (`validation`, `approvals`, `grants`, `policy`, `expiry`), so an approval or a verdict can change without changing the proposal binding |
| **ExecutionIntent** (the submission identity digest, `intent_digest`) | the whole intent payload as submitted: `schema_version`, `intent_id`, `capability`, `target`, `arguments`, `policy_context`, `selected_route`, `requested_by` | every execution-backend fact: the Dagster run id, run tags, `ictus.submitted_at`, the submission protocol version and the whole receipt (`intent_digest` itself, `run_id`, `state`, the result). Those describe the *attempt*, not the semantic intent — two attempts at the same intent must reconcile to one run |

The envelope chain is deliberate: `validation.proposal_digest` and
`intent.policy_context.proposal_digest` are the *same* digest, so a consumer can
prove the intent it holds is the one the envelope validated. A `MODIFY`
revalidation recomputes it over the modified proposal, so a copied digest from
the pre-modification payload fails closed.

### Normalization: digest what you received

Ictus serializes the contract (optional fields absent, collections empty or
absent as the schema says) and digests **that** object. A consumer must digest
the payload it was handed without re-adding fields it considers defaulted. The
vector file makes this testable: each `payload` is exactly the object that is
digested, and the Rust conformance test asserts that deserializing and
re-serializing it changes nothing.

---

## 2. What a consumer must do, and must not do

The digested cases are the easy half. Reproducing them proves a consumer can
compute the digest; what protects a binding is that it **refuses** exactly where
this protocol refuses — a value produced where we produce a refusal makes an
unverified proposal look validated. A conforming consumer:

- **must** refuse every number outside the domain — an integer outside
  −2^63 … 2^64−1, a non-finite value, a literal that only rounds to its double —
  **at the text ingress**, and the values it can still judge **in the digest
  writer**, with a typed error naming the rule and the JSON path (e.g.
  `number-would-round at $.arguments.limit`), and **must not** rely on a
  permissive parser to do it: Python's `json.loads` accepts `NaN`, `Infinity`
  and `-Infinity` unless `parse_constant` refuses them, and this protocol's
  numbers are JSON numbers, not that dialect;
- **must not** hash a value it had to round, reject or spell itself: no `%g`, no
  `float.hex`, no re-rendering a `Decimal`. The digits come from the language's
  shortest round-trip form, which the vectors pin, because a formatter that
  merely round-trips can still pick a different spelling of the same double;
- **must** fail closed on an unknown `schema_version` and on **every case the
  vectors mark as a refusal** — a refusal is an answer, not a gap to fill with
  the nearest representable value;
- **must** assert `contracts/digest-vectors.json` **including its `accepts` and
  `rejections`**: passing the digested cases alone is not conformance, and an
  implementation that digests what the protocol refuses passes the easy half and
  fails the half that protects a binding.

---

## 3. Pseudocode

A stranger can implement this in twenty lines:

```python
import hashlib, json

def digest(payload):
    check_domain(payload, "$")          # refuse, naming the field: see §1
    text = canonical(payload)
    return hashlib.sha256(text.encode("utf-8")).hexdigest()

def parse_document(document):       # the ingress: the literal rule of §1, see §2
    refused = []
    def refuse(literal, reason):    # a marker, so the path can still be named
        refused.append(Refused(literal, reason))    # after the parser is done
        return refused[-1]
    payload = json.loads(
        document,
        parse_constant=lambda text: refuse(text, "non-finite"),
        parse_int=lambda text: check_integer(int(text), text, refuse),
        parse_float=lambda text: check_literal(float(text), text, refuse),
    )
    raise_if_refused(payload, refused)   # DomainError(reason, path of the marker)
    return payload

def check_literal(value, literal, refuse):   # the same value, or it would round
    if not isfinite(value):              return refuse(literal, "non-finite")
    if Decimal(literal) != Decimal(repr(value)):
        return refuse(literal, "number-would-round")
    return value

def canonical(value):
    if value is None:            return "null"
    if value is True:            return "true"
    if value is False:           return "false"
    if isinstance(value, int):   return str(value)          # bool checked first
    if isinstance(value, float): return float_repr(value)    # CPython repr
    if isinstance(value, str):   return escape(value)        # \uXXXX outside 0x20..0x7e
    if isinstance(value, list):   return "[" + ",".join(canonical(v) for v in value) + "]"
    return "{" + ",".join(escape(k) + ":" + canonical(value[k])
                          for k in sorted(value)) + "}"     # by code point

def check_domain(value, path):      # raises on: integer outside −2^63..2^64−1,
    ...                             # non-finite float, non-string key, other type
```

`json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)`
*is* `canonical` in CPython, which is why it is the reference implementation:
the vectors in `contracts/digest-vectors.json` were computed with it and
`hashlib.sha256`, never with the code under test.

---

## 4. Versioning

`schema_version` of this protocol is **1** (`contracts/digest-vectors.json`
carries it; Rust exposes `ictus_core::DIGEST_SCHEMA_VERSION`, Python
`ictus_dagster.submission.DIGEST_SCHEMA_VERSION`).

- Any change to the **algorithm**, to the **digested field set** of a subject,
  or to the **vectors** requires a new `schema_version`. A changed algorithm is
  a new protocol, not an edit. Adding a case that pins a refusal or an
  acceptance which no version-1 digest moves is not one: the number domain, the
  canonical form and every published digest stay where they are.
- A consumer **pins the version** it implements and **fails closed** on an
  unknown one: it asserts its vectors for exactly that version and refuses to
  compute or accept a digest of a version it does not implement.
- Ictus emits **one** version at a time. There is no dual-algorithm mode and no
  fallback: a second live algorithm is what this contract exists to prevent.
- The number domain, the escaping rules and the normalization rule are part of
  the version, not implementation detail.

---

## 5. The failure mode this prevents

The same digest used to be implemented with three different bodies. Measured
before v1 was published:

| Implementation | Body | Agreed with the canonical form |
| --- | --- | --- |
| Python `intent_digest`, Tactus `canonical_digest` | `json.dumps(..., sort_keys=True, separators=(",", ":"), ensure_ascii=True)` | yes |
| Rust `content_digest` | `serde_json::to_vec` of the value form | for ASCII content only |
| Rust `proposal_digest` | `serde_json::to_vec` of the **struct**, i.e. declaration order | **never** — no real proposal has alphabetically ordered fields |

The consequences are not visible inside either repository:

- a consumer implementing the canonical form could **not verify a single
  proposal binding**, so every `validation.proposal_digest` it checked looked
  forged;
- a snapshot carrying a non-ASCII fact (`"héllo 😀"`), a double below `1e-4`
  (`0.00001` vs `1e-05`) or a double with more than 15 significant digits
  digested differently in Rust and Python;
- the failure surfaces as **duplicate execution** (the consumer concludes the
  binding is new and submits again) or as a **silently rejected binding** (an
  authorized decision is refused), neither of which any single repository's
  tests can see.

v1 fixes this by making one artefact authoritative and asserting it from every
implementation. It also fixes the number parsing that made the same literal a
different double on each side: `serde_json` is built with `float_roundtrip`
workspace-wide, so a literal such as `9007199254740993.0` parses to the same
double in Rust and in CPython. That feature also affects literal parsing
elsewhere in the workspace — every float literal is now correctly rounded. It is
agreement on the *parsed* value, not a licence for the text: that literal does
not denote the double it parses to, so the ingress refuses it (§1) instead of
digesting an approximation of it.

### What changed when v1 was published

Digest values moved exactly where the implementations disagreed. Recorded here
so a reader of our history can see it rather than discover it:

- **every proposal digest changed** (declaration order → sorted keys). The two
  recorded values in the examples moved, and each envelope now binds a proposal
  payload that is checked in beside it:
  - `examples/decision-envelope.aborted.json`:
    `713ca5ab36ccefba79b69d8bfe5971ae618355d28de6653c91a525172dc1314c` →
    `9df8d66d65af56098c18a9061783aa95192cbf3c86ab5c09441c1fc5339ee84e`
    (`examples/decision-proposal.abort.json`);
  - `examples/decision-envelope.escalated.json`:
    `c9d5099ba1534b1b3a847e2b269771236737f2010c451e7459e9e345c4b4063b` →
    `9ee156c0dd452092f88a9af5fb1729db844efa352db1d9bb901912e1da69e2dc`
    (`examples/decision-proposal.escalate.json`; its proposal id became
    `proposal:recovery:obs-0002` so the two examples stay distinct).
- **snapshot digests are unchanged** for ASCII content with ordinary numbers —
  including the pinned vector in `crates/core/src/snapshot.rs`
  (`38a4835e…`). They move only for content the old implementation escaped or
  rounded differently.
- **intent digests are unchanged**: Python already implemented the canonical
  form, so no accepted intent payload hashes differently.

---

## 6. Conformance

`contracts/digest-vectors.json` (schema `contracts/digest-vectors.schema.json`)
is the artefact to pin, and **all of it** is asserted, not just the digests:

- `cases` — the digested ones. Each names a `digests` subject and carries the
  exact `payload` and the expected lowercase hex `sha256`.
- `accepts` — documents the ingress **must let through**. Each carries the raw
  `document` and the `sha256` the payload parsed from it must digest to, so a
  guard that refuses a legitimate spelling (`0.10`, `1e-05`) is caught as
  loudly as one that accepts what the protocol refuses.
- `rejections` — documents the ingress **must refuse**. Each carries `expect:
  "refusal"`, the raw `document`, the `reason` that must be reported
  (`number-out-of-domain`, `non-finite`, `number-would-round`) and the `path` the
  error must name. Asserting that *something* failed is not conformance: the
  rule and the path are part of the case, because they are what tells a
  consumer's operator which number to fix.

A `payload` is an object and a `document` is text, and only the text is judged
by the literal rule: how this file happens to spell a number inside a `payload`
is not a spelling any implementation has to accept.

| | Asserts |
| --- | --- |
| `crates/core/tests/digest_vectors.rs` | every snapshot case through `StateSnapshot::content_digest`, every proposal case through `ictus_core::digest::proposal_digest`, every intent case through the canonical writer, every acceptance through `check_number_domain` and the writer, every rejection through `check_number_domain` with the file's `reason` and `path`; and that each payload is the contract's serialized form |
| `tests/python/test_digest_vectors.py` | every case through `ictus_dagster.submission.intent_digest`, every acceptance and rejection through `parse_digestable_document` with the file's `reason` and `path`, the file against its JSON Schema, and the example envelope ↔ proposal bindings |

The vector set covers: key ordering (including non-alphabetical insertion order
for all three subjects, and code-point ordering against UTF-16 ordering),
nesting, non-ASCII and astral strings, escapes and control characters
(including `\u007f`), integers versus doubles and the whole float battery
(including `9007199254740993.0`, `0.00001`, `5e-324`, `1.7976931348623157e308`),
the shortest-spelling tie (`-847472097840887.2`), `null`/`true`/`false`, empty
object and empty array, and duplicate-looking keys (case-differing,
case-fold-equal, NFC versus NFD). On the ingress side it covers both domain
edges as integers, the overflow of a literal to infinity in both directions, the
`NaN`/`Infinity`/`-Infinity` spellings, and a literal that only rounds — for a
value with a bare double and for an integral one — beside a document whose
`0.10`, `1e-05` and `1E5` spellings must be accepted.

Beyond the published cases, `crates/core/src/digest.rs` also compares its
formatter against CPython's `repr` for a generated corpus of 240 doubles
(subnormals, the extremes, tie cases and pseudo-random bit patterns), so a
formatter that merely round-trips but spells a value differently fails there
rather than in a consumer.

A consumer adding its own conformance test only needs the vector file and this
document — no Ictus code, and no agreement from us about a library.
