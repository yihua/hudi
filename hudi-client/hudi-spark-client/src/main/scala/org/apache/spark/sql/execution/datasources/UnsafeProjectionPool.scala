/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.internal.SQLConf

import java.io.Closeable
import java.util

/**
 * Reuses the [[UnsafeProjection]]s that readers generate per file across the files a thread reads.
 *
 * Generating the projection of a wide nested schema costs milliseconds: Spark caches the compiled class, but it
 * generates and splits the source again for every projection before it looks the class up. A reader generates one
 * projection per file, although the projection only depends on the schemas and casts it is generated for, which are
 * the same for most files of a scan. So each thread keeps the projections its finished iterators used, keyed by those
 * inputs, and hands them to later iterators with the same key, in the same task or in later tasks.
 *
 * An UnsafeProjection writes every row into the same buffer, so it serves one iterator at a time:
 *  - A [[Lease]] takes its projection out of the pool, so iterators whose rows are alive at the same time, even in
 *    one task, never share a projection.
 *  - The projection goes back to the pool only once its iterator has no more rows. A caller may use a row of a scan
 *    only until it calls hasNext or next on that iterator again (the vectorized reader reloads its batch in hasNext),
 *    so no row the projection wrote is still in use when a later iterator takes it. An iterator that is not read to
 *    the end never gives its projection back.
 *  - Each thread has its own pool and only that thread reads or changes it, so a projection is never used by two
 *    threads at once.
 *
 * A key must hold everything the generated projection depends on, with value equality. Expressions other than column
 * references, such as casts and map builders, read the SQL conf when they are built, so the keys of such projections
 * include [[sqlConf]].
 */
object UnsafeProjectionPool {

  /** The most idle projections a thread keeps, one per key. The least recently returned one goes first. */
  val MAX_IDLE_PER_THREAD = 16

  private val idle: ThreadLocal[util.LinkedHashMap[AnyRef, UnsafeProjection]] =
    new ThreadLocal[util.LinkedHashMap[AnyRef, UnsafeProjection]] {
      override def initialValue(): util.LinkedHashMap[AnyRef, UnsafeProjection] =
        new util.LinkedHashMap[AnyRef, UnsafeProjection](MAX_IDLE_PER_THREAD, 0.75f, true) {
          override def removeEldestEntry(eldest: util.Map.Entry[AnyRef, UnsafeProjection]): Boolean =
            size() > MAX_IDLE_PER_THREAD
        }
    }

  /**
   * Leases the projection of `key` for one iterator. It is taken from the calling thread's pool, or generated with
   * `generate`, the first time a row is projected, so an iterator that returns no rows needs none.
   */
  def lease(key: AnyRef, generate: => UnsafeProjection): Lease = new Lease(key, () => generate)

  /**
   * The SQL conf the calling task runs with (the session's on the driver), for keys of projections whose expressions
   * read it when they are built.
   */
  def sqlConf: Map[String, String] = SQLConf.get.getAllConfs

  /** The number of idle projections the calling thread holds. */
  private[sql] def idleCount: Int = idle.get().size()

  /** Drops the idle projections of the calling thread. */
  private[sql] def clear(): Unit = idle.get().clear()

  private def acquire(key: AnyRef, generate: () => UnsafeProjection): UnsafeProjection = {
    val pooled = idle.get().remove(key)
    if (pooled != null) pooled else generate()
  }

  private def giveBack(key: AnyRef, projection: UnsafeProjection): Unit = {
    val pool = idle.get()
    if (!pool.containsKey(key)) {
      pool.put(key, projection)
    }
  }

  /**
   * The projection of one iterator. Not thread-safe, like the projection itself: an iterator is read by one thread
   * at a time.
   */
  final class Lease private[UnsafeProjectionPool](key: AnyRef, generate: () => UnsafeProjection) {
    private var leased: UnsafeProjection = _
    private var released = false

    /** Projects `row` into the projection's buffer, overwriting the row it returned last. */
    def apply(row: InternalRow): UnsafeRow = projection(row)

    def projection: UnsafeProjection = {
      if (leased == null) {
        leased = acquire(key, generate)
      }
      leased
    }

    /**
     * Returns the projection to the calling thread's pool, once no row it wrote is used any more. Later calls do
     * nothing.
     */
    def release(): Unit = {
      if (!released) {
        released = true
        if (leased != null) {
          giveBack(key, leased)
          leased = null
        }
      }
    }

    /** Wraps `rows`, whose rows this lease projects, to release it once `rows` has no more rows. */
    def releaseWhenExhausted[T](rows: Iterator[T]): Iterator[T] = new Iterator[T] {
      override def hasNext: Boolean = checkHasNext(rows.hasNext)

      override def next(): T = rows.next()
    }

    /** As [[releaseWhenExhausted]], for iterators that the caller may close before they are exhausted. */
    def releaseWhenExhaustedCloseable[T](rows: Iterator[T] with Closeable): Iterator[T] with Closeable =
      new Iterator[T] with Closeable {
        override def hasNext: Boolean = checkHasNext(rows.hasNext)

        override def next(): T = rows.next()

        override def close(): Unit = rows.close()
      }

    /** Releases the lease when `hasNext` is false, and returns `hasNext`. */
    def checkHasNext(hasNext: Boolean): Boolean = {
      if (!hasNext) {
        release()
      }
      hasNext
    }
  }
}
