# SCALA broadcast-over-Connect spike — findings

**Status:** exploratory spike. Not for merge, not compile-verified (no build run per spike rules).
**Branch:** `broadcast-connect-scala-spike` (off master `0c97b7d6`).
**Question:** can the Python-only "broadcast variables over Spark Connect" v1 (SPARK-51705) be
extended to the **Scala** UDF path (`ScalarScalaUDF`), which the Vizio "Cooker" anchor case needs
because its broadcast is consumed inside a **Scala** UDF (`selectCandidatesScala`)?

**Bottom line (recommendation):** *Technically feasible as an incremental follow-up on top of the
Python v1 PR* using Candidate A (a client `ConnectBroadcast` with `writeReplace` + a server-side
`readResolve` that rehydrates from the SessionHolder registry). It is **materially harder and
riskier** than Python because the broadcast lives *inside* the serialized JVM closure rather than
as an out-of-band id list, so the server must intercept closure deserialization. **SPARK-46032 is
NOT a hard blocker** for this specific design (it is a general lambda-closure-deserialization bug,
still Open, unrelated to broadcast id transport), **but the same fragility it exposes — a Connect
Scala closure that fails to deserialize on the server — is exactly the surface this feature leans
on.** Ship Python v1 first; treat Scala as a separate follow-up JIRA that depends on the v1
registry landing, and de-risk closure deserialization robustness before committing.

---

## 1. How a Scala Connect UDF closure is captured, serialized, sent, and rehydrated

Verified symbols/paths (all in `/tmp/byteoak-spark-pr`):

### Client (JVM Connect client — `sql/connect/common`, `sql/api`)
- A user builds a Scala UDF with `functions.udf(...)`, producing a
  `SparkUserDefinedFunction` (an `InvokeInlineUserDefinedFunction` column node).
- `columnNodeSupport.scala:203-206` matches it and calls
  `UdfToProtoUtils.toProto(udf, ...)`.
- `UdfToProtoUtils.toProto` (`UdfToProtoUtils.scala:73-111`):
  - `toUdfPacketBytes(f.f, inputEncoders, outputEncoder)` (`:57-68`) runs
    `ClosureCleaner.clean(function, cleanTransitively = true, ...)`
    (`common/utils/.../ClosureCleaner.scala`) then
    `SparkSerDeUtils.serialize(UdfPacket(cleanedFunction, inputEncoders, outputEncoder))`
    — **plain Java/JVM serialization** (`common/utils/.../SparkSerDeUtils.scala:41`), NOT
    cloudpickle. It even eagerly round-trips (`checkDeserializable`, `:41-55`) to surface the
    classic `"cannot assign instance of java.lang.invoke.SerializedLambda"` failure early — this is
    literally the SPARK-46032 error string.
  - The bytes go into `ScalarScalaUDF.payload` (proto `expressions.proto:480-491`, field 1).
- The client has **no SparkContext**: `SparkSession.sparkContext`
  (`sql/connect/common/.../SparkSession.scala:106-107`) throws
  `ConnectClientUnsupportedErrors.sparkContext()`. So the user *cannot* call
  `sc.broadcast(v)` to get a `Broadcast[T]` to capture. **This is the crux.**

### Wire
- `ScalarScalaUDF` is carried inside `CommonInlineUserDefinedFunction.scalar_scala_udf`
  (`expressions.proto:460`). Fields top out at `aggregate = 5`; **field 6 is next-free**
  (matches the plan doc note). This spike adds `repeated int64 broadcast_ids = 6;`.

### Server (`sql/connect/server` — `SparkConnectPlanner.scala`)
- `transformScalaUDF` (`:2140`) → `transformScalaFunction` (`:2161`) → `unpackUdf` (`:2097`) →
  `unpackScalaUDF[UdfPacket]` (`:2105-2129`):
  `Utils.deserialize[T](fun.getPayload.toByteArray, contextOrSparkClassLoader)` — **Java
  deserialization of the opaque closure blob**, using the artifact classloader (so user classes
  added via `addArtifact` resolve). NoSuchMethod/ClassNotFound → a class-not-found error telling the
  user to `session.addArtifact`.
- `transformScalaFunction` (`:2161-2183`) then builds a `SparkUserDefinedFunction` /
  `UserDefinedAggregator` from `udfPacket.function` + encoders, which
  `UserDefinedFunctionUtils.toScalaUDF` turns into a Catalyst `ScalaUDF` for execution.

### Executor
- Unchanged from classic Spark. A `Broadcast[T]` (`core/.../broadcast/Broadcast.scala:57`,
  `abstract class Broadcast[T](val id: Long) extends Serializable`) captured in a closure serializes
  as **just its `id`** — `TorrentBroadcast._value/compressionCodec/blockSize` are all `@transient`
  (`TorrentBroadcast.scala:72-79`), and `writeObject` (`:249-252`) only does
  `defaultWriteObject()`. On the executor, `getValue()` → `readBroadcastBlock()` (`:254-313`)
  fetches `BroadcastBlockId(id)` blocks from the block manager / peers (Torrent fan-out). So **a
  broadcast reference is fundamentally an `id` plus a live `SparkEnv` to fetch blocks by that id.**

**Where a broadcast reference would have to be injected:** into the `udfPacket.function` object
graph — i.e. as a field of the captured closure — such that after
`unpackScalaUDF` deserializes it on the server, the closure holds a real driver-side
`Broadcast[T]` (a `TorrentBroadcast` on the server's live SparkContext) whose `id` matches a
broadcast that `CreateBroadcastCommand` already created. Everything downstream (Catalyst `ScalaUDF`
→ task serialization → executor `readBroadcastBlock`) is then classic Spark, unchanged.

---

## 2. Why Scala is materially harder than Python (the crux)

| | **Python v1 (works)** | **Scala (this spike)** |
|---|---|---|
| Closure transport | cloudpickle **bytes** in `PythonUDF.command`; never deserialized on the JVM | Java-serialized JVM **object graph** in `ScalarScalaUDF.payload`; **deserialized on the server** in `unpackScalaUDF` |
| Broadcast reference | Out-of-band: `Broadcast._from_id(bid)` token in the pickle **and** an explicit `broadcast_ids` id-list the server reads without touching the closure | In-band: the `Broadcast[T]` is a **field inside** the serialized closure object graph |
| Server resolution | `transformPythonFunction` sets `broadcastVars = resolveBroadcasts(ids)` — a `JList[Broadcast[PythonBroadcast]]` the `PythonRunner` injects into the worker (`PythonRunner.writeBroadcasts`, `PythonRDD.scala:90/107`). The closure is **opaque and untouched**. | The server must make the id resolve to a real `Broadcast[T]` **during** `ObjectInputStream.readObject`, because that is when the closure's broadcast field is reconstructed |
| Client can create the ref? | Yes trivially — `_from_id` is just a Python constructor; needs no SparkContext | **No.** `Broadcast[T]` is abstract; the only concrete impl `TorrentBroadcast` calls `SparkEnv.get` in its constructor (`TorrentBroadcast.scala:95,100`). The JVM-less client cannot instantiate one to capture. |

**The essence:** Python cleanly *decouples* the broadcast id from the closure — the id rides a
proto field and the worker is handed a ready `broadcastVars` list, so the pickled command is never
inspected. Scala *couples* them — the broadcast is embedded in the closure, so you cannot resolve it
without hooking the deserialization of the closure itself. That hook (`readResolve` reaching a
per-request registry via a thread-local) is the fragile, non-obvious part, and it means the
feature's correctness is entangled with JVM-serialization internals rather than a clean proto
contract.

Secondary hard points:
1. **Client can't construct `Broadcast[T]`.** Requires a new client-only `ConnectBroadcast[T]`
   subclass (prototyped here) that overrides `getValue` (local value for driver-parity reads) and
   does NOT touch `SparkEnv`. Its `writeReplace` must emit an id-only token so nothing
   SparkContext-bound is serialized.
2. **`ClosureCleaner` + `checkDeserializable` round-trip on the client.** `toUdfPacketBytes`
   deserializes the cleaned closure *on the client* to pre-validate (`:41-66`). The client's
   `ConnectBroadcastRef.readResolve` therefore also fires on the client, where there is no registry
   — it must be tolerated (return a placeholder / skip validation) or `checkDeserializable` must be
   made broadcast-aware. Prototyped `readResolve` currently throws loud when no registry is bound;
   the client round-trip needs a guarded path. **SPIKE TODO in `ConnectBroadcast.scala`.**
3. **Thread-local safety.** `readResolve` gets no context from `ObjectInputStream`, so the only way
   to hand the registry to it is a thread-local installed around `unpackScalaUDF`
   (`ConnectBroadcastResolver.withRegistry`). Safe only because deserialization is synchronous on
   the request thread today. Python needs none of this.
4. **Type erasure / `ClassTag`.** `ConnectBroadcast[T]` needs a `ClassTag`; the server swaps in a
   `Broadcast[_]` and the closure field is `Broadcast[T]` — casts are unavoidable and the value
   type is only checked at `.value` use sites. Manageable but a sharp edge.
5. **UDAF path.** `TypedAggregateExpression`/`UserDefinedAggregator` also flow through
   `ScalarScalaUDF` (`columnNodeSupport.scala:177-178`, planner `:2165-2172`); the same token would
   need to work inside an `Aggregator`. In scope structurally, more surface to test.

---

## 3. SPARK-46032 state + dependency

**Verified via Apache JIRA REST (2026-07-20):**
- **SPARK-46032** — *"connect: cannot assign instance of `java.lang.invoke.SerializedLambda` to
  field `org.apache.spark.rdd.MapPartitionsRDD.f`"* — **Status: Open, Resolution: none, no
  fixVersion, last updated 2024-07-12.** (`gh` is available in this env, but the issue lives in
  Apache JIRA, not GitHub Issues; queried the JIRA REST API directly.)
- It is a **general Scala-closure-deserialization bug** on Spark Connect (a lambda captured in a
  Connect Scala closure fails to deserialize server-side), NOT specifically about broadcast id
  transport. It is the same failure `UdfToProtoUtils.checkDeserializable` (`:38-55`) already guards
  against and reports as *"UDF cannot be executed … it cannot be deserialized … self-reference …
  not supported by java serialization."*

**Dependency verdict:** SPARK-46032 is **not a strict blocker** of the broadcast-id transport
prototyped here — our `writeReplace`/`readResolve` design adds an id token to an object graph that
already serializes, and does not depend on SerializedLambda handling being fixed. **However**, the
Scala broadcast feature *lives or dies on the reliability of deserializing a user's Scala closure on
the Connect server*, which is precisely the area SPARK-46032 shows is fragile. So while you can
prototype/land the id transport independently, the customer-facing reliability of "capture a
broadcast in a Scala Connect UDF" is coupled to closure-deserialization robustness generally.
Recommendation: track SPARK-46032 as a **related risk**, not a blocking parent, and add a
closure-deserialization robustness test to the Scala follow-up's DoD.

---

## 4. Options explored — which is viable

### Option (a) — client `ConnectBroadcast` + `writeReplace`/`readResolve` (SELECTED, prototyped)
Client subclass emits an id-only `ConnectBroadcastRef` token via `writeReplace`; server rehydrates
via `readResolve` reading a thread-local registry installed around `unpackScalaUDF`.
- **Pro:** mirrors the Python `__reduce__ -> (_from_id,(bid,))` contract exactly; reuses the Python
  v1 SessionHolder registry and `BROADCAST_NOT_FOUND` verbatim; executor path stays 100% classic;
  the value travels once out-of-band through the existing `cache/<sha256>` artifact channel (the
  same transport Python uses) rather than inside the closure.
- **Con:** the thread-local `readResolve` bridge (fragile if deserialization ever moves off-thread);
  the client-side `checkDeserializable` round-trip fires `readResolve` with no registry (needs a
  guarded path); `ClassTag`/cast sharp edges. All the "hard points" in §2.
- **Verdict: most viable.** Prototyped in `ConnectBroadcast.scala` + planner `unpackUdf` edit.

### Option (b) — server-side injection without touching the closure
Resolve `broadcast_ids` to `Broadcast[_]`s and inject them into UDF execution *after*
deserialization (e.g. set a field on the deserialized closure by reflection, or pass broadcasts as
extra hidden `ScalaUDF` children/env).
- **Pro:** avoids `writeReplace`/`readResolve` and the thread-local.
- **Con:** there is no natural seam — a Scala UDF's function is an arbitrary user object; there is no
  generic "broadcast slot" to inject into. Reflection-setting a user field is brittle and
  non-general (which field? what if captured indirectly?). Passing broadcasts as hidden `ScalaUDF`
  children changes the Catalyst expression shape and the user's function signature never references
  them. **Rejected as non-general**, though a *constrained* form (a documented
  `spark.connectBroadcast[T](id)` accessor the user calls *inside* the UDF body instead of
  capturing) could sidestep capture entirely — noted as a possible v0 ergonomic fallback.

---

## 5. What a real Scala implementation would require

1. **Proto:** `ScalarScalaUDF.broadcast_ids = 6` (done in spike) + regenerate Scala/Python stubs
   (`dev/generate-connect-protos.sh`). Re-verify field 6 is still free at PR time.
2. **Stack on Python v1:** reuse `SessionHolder.broadcasts` registry, `registerBroadcast`/
   `getBroadcast`/`removeBroadcast`, `CreateBroadcastCommand`/`UnpersistBroadcastCommand`,
   `CreateBroadcastResult`, and `BROADCAST_NOT_FOUND`. **But the value carrier differs:** Python
   wraps the staged temp file in `PythonBroadcast`; Scala must `sc.broadcast(theRealValue)` of the
   *deserialized JVM object* (Java-deserialize the cached artifact bytes into `T`, then broadcast
   `T` directly) so the executor gets a normal `TorrentBroadcast[T]`. That means either a
   type-parameterized `CreateBroadcastCommand` variant or a "language=scala" flag so the server
   deserializes rather than `PythonBroadcast`-wraps. **New server work beyond Python v1.**
3. **Client:** `ConnectBroadcast[T]` (subclass of `Broadcast[T]`, no `SparkEnv`), a
   `SparkSession.broadcast[T](v)` on the Connect Scala client that JVM-serializes `v` →
   `cacheArtifact` → `CreateBroadcastCommand`, and a thread-local capture registry drained in
   `UdfToProtoUtils.toProto` into `broadcast_ids` (sketched).
4. **Server:** bind registry thread-local around `unpackScalaUDF` (done in spike);
   `ConnectBroadcastRef.readResolve` swaps id → real `Broadcast[T]` (sketched). Guard the
   client-side `checkDeserializable` round-trip.
5. **Lifecycle:** `unpersist`/`destroy` route through `UnpersistBroadcastCommand`; `SessionHolder`
   sweep already covers it (python-v1).
6. **Tests:** the Vizio `selectCandidatesScala`-shaped E2E (Scala UDF over a large DataFrame reading
   a broadcast); UDAF variant; unknown-id → `BROADCAST_NOT_FOUND`; encryption-on; closure
   deserialization robustness (SPARK-46032 regression guard); client `.value` driver-parity read.

---

## 6. Files touched by this spike

- `sql/connect/common/src/main/protobuf/spark/connect/expressions.proto` — add
  `repeated int64 broadcast_ids = 6;` to `ScalarScalaUDF` (additive; stubs NOT regenerated).
- `sql/connect/common/src/main/scala/org/apache/spark/sql/connect/ConnectBroadcast.scala` — **new**;
  `ConnectBroadcast[T]` (client proxy, `writeReplace`), `ConnectBroadcastRef` (id-only wire token,
  `readResolve`), `ConnectBroadcastResolver` (thread-local server bridge). Illustrative;
  `// SPIKE:` TODOs mark the client round-trip guard and RPC-routed unpersist/destroy.
- `sql/connect/server/.../planner/SparkConnectPlanner.scala` — `unpackUdf` binds the per-session
  registry into `ConnectBroadcastResolver.withRegistry` around `unpackScalaUDF` when
  `broadcast_ids` is non-empty; eagerly validates ids (fail-loud). **Depends on python-v1 symbols**
  (`sessionHolder.getBroadcast`, `InvalidInputErrors.broadcastNotFound`) — does not compile on bare
  master; clearly marked.
- `sql/connect/common/.../UdfToProtoUtils.scala` — `// SPIKE:` comment block in `toProto` showing
  where/how to drain captured `ConnectBroadcast` ids into `broadcast_ids` after closure
  serialization (mirrors Python `to_plan` drain).

Nothing compiled/built (spike rule). All incomplete spots carry `// SPIKE:` markers.
