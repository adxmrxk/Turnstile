package dev.turnstile.sim;

import dev.turnstile.domain.DomainEvent;
import dev.turnstile.eventstore.AppendResult;
import dev.turnstile.eventstore.EventStore;
import dev.turnstile.eventstore.StoredEvent;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.OptionalLong;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * A stateless model checker for the command handler and the event store.
 *
 * <p>A stress test hopes the scheduler happens to produce the bad interleaving.
 * This takes the scheduler away. Each "actor" runs the real handler on its own
 * thread, but every call into the store is a gate: the thread parks, and this
 * class decides which parked thread moves next. One thread moves at a time, so
 * every run is a single, fully determined interleaving of store calls, and the
 * set of interleavings can be enumerated instead of sampled.
 *
 * <p>The store calls themselves are the atomic steps. That matches the real
 * contract (a compare-and-append is one indivisible operation) and it is where
 * the races live: between a read and the write that was decided from it.
 *
 * <p>Exploration is depth-first by replay. A run follows a forced prefix of
 * choices and then takes the first option at every later point, recording how
 * many options there were; each untaken option becomes a new prefix to replay.
 * Nothing is cloned, so a scenario only has to be deterministic given the
 * choices, which the scenarios here are (fixed clock, fixed ids).
 *
 * <p>A checker that cannot fail proves nothing, so the tests also point this at
 * a store that is deliberately broken and require it to report violations.
 */
final class ScheduleExplorer {

  /** One fresh world per schedule. */
  interface Scenario {
    /** The real store, for the verifier to read afterwards. */
    EventStore store();

    /** Each actor receives a store that gates every call, and returns an outcome label. */
    List<Function<EventStore, String>> actors();

    /** Throws AssertionError if the finished run broke an invariant. */
    void verify(List<String> outcomes);
  }

  record Report(
      long schedules, long distinct, long violations, boolean exhausted, String firstViolation) {}

  private static final long STUCK_MS = 10_000;

  private final Supplier<Scenario> factory;

  ScheduleExplorer(Supplier<Scenario> factory) {
    this.factory = factory;
  }

  /** Every interleaving, up to {@code limit} schedules. */
  Report exhaustive(long limit) {
    long schedules = 0;
    long violations = 0;
    String first = null;
    java.util.Set<String> seen = new java.util.HashSet<>();
    Deque<List<Integer>> pending = new ArrayDeque<>();
    pending.push(List.of());

    while (!pending.isEmpty() && schedules < limit) {
      List<Integer> prefix = pending.pop();
      Execution run = execute((step, options) -> step < prefix.size() ? prefix.get(step) : 0);
      schedules++;
      seen.add(String.join(",", run.trace));
      if (run.violation != null) {
        violations++;
        if (first == null) {
          first = run.describe();
        }
      }
      for (int step = prefix.size(); step < run.optionCounts.size(); step++) {
        for (int alt = 1; alt < run.optionCounts.get(step); alt++) {
          List<Integer> branch = new ArrayList<>(run.choices.subList(0, step));
          branch.add(alt);
          pending.push(branch);
        }
      }
    }
    return new Report(schedules, seen.size(), violations, pending.isEmpty(), first);
  }

  /** {@code count} pseudo-random interleavings, reproducible from {@code seed}. */
  Report sampled(long count, long seed) {
    Random random = new Random(seed);
    java.util.Set<String> seen = new java.util.HashSet<>();
    long violations = 0;
    String first = null;
    for (long i = 0; i < count; i++) {
      Execution run = execute((step, options) -> random.nextInt(options));
      seen.add(String.join(",", run.trace));
      if (run.violation != null) {
        violations++;
        if (first == null) {
          first = run.describe();
        }
      }
    }
    return new Report(count, seen.size(), violations, false, first);
  }

  private interface Chooser {
    int choose(int step, int options);
  }

  private Execution execute(Chooser chooser) {
    Scenario scenario = factory.get();
    List<Function<EventStore, String>> actors = scenario.actors();
    Execution run = new Execution(actors.size());
    EventStore gated = new GatedStore(scenario.store(), run);

    List<Thread> threads = new ArrayList<>();
    for (int i = 0; i < actors.size(); i++) {
      final int id = i;
      Thread t =
          new Thread(
              () -> {
                Execution.CURRENT.set(id);
                String outcome;
                try {
                  outcome = actors.get(id).apply(gated);
                } catch (Throwable failure) {
                  outcome = "CRASH " + failure;
                }
                run.finish(id, outcome);
              },
              "actor-" + i);
      t.setDaemon(true);
      threads.add(t);
      t.start();
    }

    run.drive(chooser);
    for (Thread t : threads) {
      try {
        t.join(STUCK_MS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException(e);
      }
    }

    try {
      scenario.verify(List.of(run.outcomes));
    } catch (AssertionError failure) {
      run.violation = failure.getMessage();
    }
    return run;
  }

  /** The bookkeeping for one schedule: who is parked, who is done, what was chosen. */
  private static final class Execution {
    static final ThreadLocal<Integer> CURRENT = new ThreadLocal<>();

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition changed = lock.newCondition();
    private final boolean[] parked;
    private final boolean[] done;
    private final String[] pendingOp;
    private int granted = -1;

    final String[] outcomes;
    final List<Integer> choices = new ArrayList<>();
    final List<Integer> optionCounts = new ArrayList<>();
    final List<String> trace = new ArrayList<>();
    String violation;

    Execution(int actors) {
      parked = new boolean[actors];
      done = new boolean[actors];
      pendingOp = new String[actors];
      outcomes = new String[actors];
    }

    /** Called on an actor thread: park until the driver lets this actor take its step. */
    void gate(String op) {
      int me = CURRENT.get();
      lock.lock();
      try {
        parked[me] = true;
        pendingOp[me] = op;
        changed.signalAll();
        while (granted != me) {
          awaitOrFail();
        }
        granted = -1;
        parked[me] = false;
      } finally {
        lock.unlock();
      }
    }

    void finish(int id, String outcome) {
      lock.lock();
      try {
        outcomes[id] = outcome;
        done[id] = true;
        changed.signalAll();
      } finally {
        lock.unlock();
      }
    }

    /** Runs the schedule to completion, choosing among parked actors at each step. */
    void drive(Chooser chooser) {
      int step = 0;
      while (true) {
        lock.lock();
        try {
          while (!(granted == -1 && settled())) {
            awaitOrFail();
          }
          List<Integer> enabled = new ArrayList<>();
          for (int i = 0; i < parked.length; i++) {
            if (parked[i]) {
              enabled.add(i);
            }
          }
          if (enabled.isEmpty()) {
            return;
          }
          int index = chooser.choose(step, enabled.size());
          int actor = enabled.get(index);
          choices.add(index);
          optionCounts.add(enabled.size());
          trace.add("actor-" + actor + " " + pendingOp[actor]);
          granted = actor;
          changed.signalAll();
          step++;
        } finally {
          lock.unlock();
        }
      }
    }

    private boolean settled() {
      for (int i = 0; i < parked.length; i++) {
        if (!parked[i] && !done[i]) {
          return false;
        }
      }
      return true;
    }

    private void awaitOrFail() {
      try {
        if (!changed.await(STUCK_MS, TimeUnit.MILLISECONDS)) {
          throw new IllegalStateException("scheduler stuck; trace so far: " + trace);
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException(e);
      }
    }

    String describe() {
      return violation
          + "\n  outcomes: "
          + List.of(outcomes)
          + "\n  schedule: "
          + String.join(" -> ", trace);
    }
  }

  /** Parks the calling actor before every call the handler makes into the store. */
  private record GatedStore(EventStore delegate, Execution run) implements EventStore {
    @Override
    public List<StoredEvent> load(String streamId) {
      run.gate("load(" + streamId + ")");
      return delegate.load(streamId);
    }

    @Override
    public AppendResult append(
        String streamId, long expectedVersion, List<DomainEvent> events, String idempotencyKey) {
      run.gate("append(" + streamId + " @" + expectedVersion + " " + events.size() + " event(s))");
      return delegate.append(streamId, expectedVersion, events, idempotencyKey);
    }

    @Override
    public OptionalLong versionForIdempotencyKey(String idempotencyKey) {
      run.gate("idempotency-check(" + idempotencyKey + ")");
      return delegate.versionForIdempotencyKey(idempotencyKey);
    }

    @Override
    public List<StoredEvent> readAll() {
      return delegate.readAll();
    }

    @Override
    public long currentVersion(String streamId) {
      return delegate.currentVersion(streamId);
    }
  }
}
