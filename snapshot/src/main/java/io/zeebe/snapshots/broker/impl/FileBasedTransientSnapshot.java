/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.snapshots.broker.impl;

import io.zeebe.snapshots.raft.PersistedSnapshot;
import io.zeebe.snapshots.raft.TransientSnapshot;
import io.zeebe.util.FileUtil;
import io.zeebe.util.sched.ActorControl;
import io.zeebe.util.sched.future.ActorFuture;
import io.zeebe.util.sched.future.CompletableActorFuture;
import java.io.IOException;
import java.nio.file.Path;
import java.util.function.Consumer;
import java.util.function.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents a pending snapshot, that is a snapshot in the process of being written and has not yet
 * been committed to the store.
 */
public final class FileBasedTransientSnapshot implements TransientSnapshot {
  private static final Logger LOGGER = LoggerFactory.getLogger(FileBasedTransientSnapshot.class);

  private final Path directory;
  private final ActorControl actor;
  private final FileBasedSnapshotStore snapshotStore;
  private final FileBasedSnapshotMetadata metadata;
  private final ActorFuture<Void> takenFuture = new CompletableActorFuture<>();

  FileBasedTransientSnapshot(
      final FileBasedSnapshotMetadata metadata,
      final Path directory,
      final FileBasedSnapshotStore snapshotStore,
      final ActorControl actor) {
    this.metadata = metadata;
    this.snapshotStore = snapshotStore;
    this.directory = directory;
    this.actor = actor;
  }

  @Override
  public ActorFuture<Boolean> take(final Predicate<Path> takeSnapshot) {
    return actor.call(() -> takeInternal(takeSnapshot));
  }

  @Override
  public void onSnapshotTaken(final Consumer<Throwable> runnable) {
    actor.call(() -> takenFuture.onComplete((nothing, error) -> runnable.accept(error)));
  }

  private boolean takeInternal(final Predicate<Path> takeSnapshot) {
    final var snapshotMetrics = snapshotStore.getSnapshotMetrics();
    boolean failed;

    try (final var ignored = snapshotMetrics.startTimer()) {
      try {
        failed = !takeSnapshot.test(getPath());
        takenFuture.complete(null);
      } catch (final Exception exception) {
        LOGGER.warn("Unexpected exception on taking snapshot ({})", metadata, exception);
        failed = true;
        takenFuture.completeExceptionally(exception);
      }
    }

    if (failed) {
      abortInternal();
    }

    return !failed;
  }

  @Override
  public ActorFuture<Void> abort() {
    return actor.call(this::abortInternal);
  }

  @Override
  public ActorFuture<PersistedSnapshot> persist() {
    return actor.call(() -> snapshotStore.newSnapshot(metadata, directory));
  }

  private void abortInternal() {
    try {
      LOGGER.debug("DELETE dir {}", directory);
      FileUtil.deleteFolder(directory);
    } catch (final IOException e) {
      LOGGER.warn("Failed to delete pending snapshot {}", this, e);
    }
  }

  private Path getPath() {
    return directory;
  }

  @Override
  public String toString() {
    return "FileBasedTransientSnapshot{"
        + "directory="
        + directory
        + ", snapshotStore="
        + snapshotStore
        + ", metadata="
        + metadata
        + '}';
  }
}
