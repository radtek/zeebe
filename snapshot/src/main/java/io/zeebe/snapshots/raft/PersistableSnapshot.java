/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.snapshots.raft;

import io.zeebe.util.sched.future.ActorFuture;

/** A volatile snapshot which can be persisted. */
public interface PersistableSnapshot {

  /** Aborts the not yet persisted snapshot and removes all related data. */
  void abort();

  /**
   * Persists the snapshot with all his data and returns the representation of this snapshot.
   *
   * @return the persisted snapshot
   */
  ActorFuture<PersistedSnapshot> persist();
}
