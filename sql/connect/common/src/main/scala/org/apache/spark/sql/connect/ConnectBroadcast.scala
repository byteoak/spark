/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.connect

import scala.reflect.ClassTag

import org.apache.spark.broadcast.Broadcast

/**
 * ============================================================================
 * SPIKE ONLY -- NOT FOR MERGE. Illustrative prototype for SPARK-51705 Scala
 * follow-up ("broadcast variables over Spark Connect", Scala/ScalarScalaUDF path).
 * See /tmp/byteoak-spark-pr/SCALA-SPIKE-FINDINGS.md for the full analysis.
 * ============================================================================
 *
 * A client-side stand-in for a driver-side [[Broadcast]] that a Spark Connect Scala client can
 * capture inside a UDF closure even though the client has NO SparkContext (so it can never call
 * `sc.broadcast(v)` to obtain a real [[org.apache.spark.broadcast.TorrentBroadcast]]).
 *
 * This is the Scala analogue of the Python `ConnectBroadcast` proxy on the
 * `broadcast-connect-python-v1` branch (`python/pyspark/sql/connect/broadcast.py`). It exists to
 * solve the exact same gap that motivated `SparkSession.broadcast()` for Python: the Connect
 * client is JVM-less w.r.t. the cluster, so the user cannot construct the `Broadcast[T]` object
 * that classic Spark expects them to capture.
 *
 * ----------------------------------------------------------------------------
 * How this is meant to work end to end (Candidate A -- writeReplace/readResolve):
 * ----------------------------------------------------------------------------
 *   client:   spark.broadcast(v)
 *               -> SparkSerDeUtils.serialize(v)  (JVM/Java serialization, NOT cloudpickle -- Scala)
 *               -> client.artifactManager.cacheArtifact(bytes) -> sha256
 *               -> ExecutePlan(CreateBroadcastCommand{artifact_hash})
 *               <- CreateBroadcastResult{broadcast_id}   // server-assigned driver-side Broadcast.id
 *               -> return new ConnectBroadcast[T](broadcast_id, v)
 *
 *   capture:  the user writes a normal Scala UDF that closes over the ConnectBroadcast:
 *               val bc: Broadcast[Map[String, Row]] = spark.broadcast(candidates)
 *               val f = udf((k: String) => bc.value.get(k))   // selectCandidatesScala
 *             UdfToProtoUtils.toProto -> ClosureCleaner.clean -> SparkSerDeUtils.serialize(UdfPacket)
 *             During serialization, ConnectBroadcast.writeReplace() substitutes a tiny
 *             ConnectBroadcastRef(id) token into the object graph -- the captured `value` is NEVER
 *             written to the wire (it already travels once, out of band, via the cache artifact).
 *             The client ALSO records `id` into ScalarScalaUDF.broadcast_ids (proto field 6) as an
 *             out-of-band hint for the server (see below).
 *
 *   server:   see SparkConnectPlanner.transformScalaFunction (SPIKE edits): before deserializing
 *             the UdfPacket payload, the planner binds the per-session SessionHolder broadcast
 *             registry into a thread-local, keyed by id. When Java deserialization reaches the
 *             ConnectBroadcastRef token, ConnectBroadcastRef.readResolve() looks up its id in that
 *             thread-local and returns the REAL driver-side Broadcast[T] (a TorrentBroadcast on the
 *             server's live SparkContext, created by CreateBroadcastCommand). The deserialized
 *             closure now holds a genuine TorrentBroadcast whose @transient _value is null and
 *             whose id matches -- so executor rehydration is byte-identical to classic Spark.
 *
 *   executor: UNCHANGED. TorrentBroadcast.readBroadcastBlock() fetches BroadcastBlockId(id) blocks.
 *
 * @param bid   the server-assigned driver-side Broadcast.id
 * @param value the local value, retained ONLY so client-side `.value` reads work (parity with
 *              classic driver-side reads). Marked @transient so it is never serialized even if
 *              writeReplace were somehow bypassed.
 */
private[sql] class ConnectBroadcast[T: ClassTag](
    bid: Long,
    @transient private val value_ : T)
    extends Broadcast[T](bid) {

  // Client-side value read (driver parity). On the cluster, .value is served by the real
  // TorrentBroadcast that readResolve swaps in, not by this object.
  override protected def getValue(): T = value_

  // Unpersist/destroy over Connect are RPCs (UnpersistBroadcastCommand), issued by SparkSession,
  // not local BlockManager ops -- the client has no BlockManager. SPIKE: the real implementation
  // routes these through the SparkConnectClient; here they are no-ops with a marker.
  override protected def doUnpersist(blocking: Boolean): Unit = {
    // SPIKE: real impl -> spark.client.execute(UnpersistBroadcastCommand(id, blocking, destroy=false))
  }

  override protected def doDestroy(blocking: Boolean): Unit = {
    // SPIKE: real impl -> spark.client.execute(UnpersistBroadcastCommand(id, blocking, destroy=true))
  }

  /**
   * THE CRUX (client half). When the enclosing UDF closure is Java-serialized into the UdfPacket
   * payload, substitute a tiny id-only token for this object. This is the exact same contract the
   * Python side implements via `ConnectBroadcast.__reduce__ -> (_from_id, (bid,))`.
   *
   * Because writeReplace returns the token, neither `value_` (already @transient) nor any
   * SparkContext-bound state is ever serialized -- which is essential, since none exists on the
   * client.
   */
  private def writeReplace(): AnyRef = new ConnectBroadcastRef(id)
}

/**
 * SPIKE ONLY. The wire token that stands in for a [[ConnectBroadcast]] inside a serialized Scala
 * UDF closure. Carries ONLY the broadcast id.
 *
 * THE CRUX (server half). readResolve runs during UdfPacket deserialization on the server. It must
 * resolve `id` to the real driver-side Broadcast[T] from the per-session registry. The registry is
 * handed in via a thread-local that SparkConnectPlanner installs immediately before deserializing
 * (see ConnectBroadcastResolver below and the planner SPIKE edits) -- there is no other channel,
 * because ObjectInputStream.readResolve receives no context.
 */
private[sql] class ConnectBroadcastRef(val id: Long) extends Serializable {
  private def readResolve(): AnyRef = {
    // SPIKE: On the SERVER this resolves to the real Broadcast[_] from the SessionHolder registry
    // that CreateBroadcastCommand populated. On the CLIENT (e.g. a round-trip in tests) the
    // resolver is empty and we must fail loud rather than hand back a broken proxy.
    ConnectBroadcastResolver.resolve(id).getOrElse {
      throw new IllegalStateException(
        s"SPIKE: ConnectBroadcastRef($id) could not be resolved -- no broadcast registry bound to " +
          "this deserialization thread. On the server this indicates BROADCAST_NOT_FOUND; the id " +
          "was never created on this session or belongs to another session.")
    }
  }
}

/**
 * SPIKE ONLY. Thread-local bridge that lets [[ConnectBroadcastRef.readResolve]] (which gets no
 * context from ObjectInputStream) reach the per-session broadcast registry that only the server's
 * SparkConnectPlanner holds.
 *
 * This type lives in `sql/connect/common` so BOTH the client (which defines the token) and the
 * server (`sql/connect/server` depends on common) can see the same class -- the token class must
 * be identical on both sides of the wire for Java deserialization to bind to it.
 *
 * SPIKE / OPEN ISSUE: a thread-local is only safe because UdfPacket deserialization in
 * `unpackScalaUDF` is synchronous on the request-handling thread. If closure deserialization ever
 * moves to a different thread (it does NOT today), this bridge breaks. The Python side sidesteps
 * this entirely because Python never deserializes the closure on the JVM -- it injects
 * `broadcastVars` as an explicit id list into the worker, out-of-band from the pickled command.
 */
private[sql] object ConnectBroadcastResolver {
  // id -> real driver-side Broadcast[_], installed by the server for the duration of one deserialize.
  private val bound = new ThreadLocal[Map[Long, Broadcast[_]]]()

  /** SPIKE: called by the SERVER (SparkConnectPlanner) around unpackScalaUDF. */
  def withRegistry[R](registry: Map[Long, Broadcast[_]])(body: => R): R = {
    val prev = bound.get()
    bound.set(registry)
    try body
    finally {
      if (prev == null) bound.remove() else bound.set(prev)
    }
  }

  def resolve(id: Long): Option[Broadcast[_]] =
    Option(bound.get()).flatMap(_.get(id))
}
