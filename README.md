# Turnstile

An event-sourced seat reservation server that is built to never oversell, and to
prove it afterwards from its own log.

It is a running server (REST, GraphQL, WebSocket) over a PostgreSQL event store,
with a Kafka-fed read model, a purchase saga that refunds on failure, role-based
access, and a live demo page.

## Run it

```bash
./bin/turnstilectl up        # http://localhost:8080  (in-memory store, security off)
```

Open the page and press **Release the crowd**: thousands of simulated buyers
compete for a few hundred seats while payments fail on purpose, and the seat map
updates live. At the end the run's log is audited and the payment ledger is
checked against the seats sold.

Audit a running server from a shell, with a checker that shares no code with it:

```bash
./bin/turnstilectl export | ./bin/turnstilectl verify -s
```

For PostgreSQL, Kafka and real tokens use `SPRING_PROFILES_ACTIVE=prod`. It
refuses to start without its database password and token-key URL.

## The problem

Two buyers read the same seat as free and both write. Payment is slow, so a hold
has to expire, and a payment that lands after expiry must not sell a seat that
went back on sale. Clients retry, and a retry must not buy a second seat or be
told a successful purchase failed. And you have to be able to prove it afterwards.

## How it works

Nothing stores current state. The truth is an append-only log, and a seat's status
is a fold over its own events: `AVAILABLE -> HELD -> SOLD` (terminal).

- **Compare-and-append.** Every append names the version it was decided from. If
  the stream moved, it is refused. In Postgres that is a unique index on
  `(stream_id, version)`.
- **A conflict discards the decision, not just the write.** The loser reloads and
  is re-judged against the new state, so a race yields one sale and one refusal.
- **Expiry is evaluated, not scheduled.** A hold is expired if `now > expiresAt`
  when a command runs. No sweeper is needed for correctness.
- **Idempotency keys** make retries safe end to end, including over HTTP.

The audit is `scripts/verify-invariants.sh`, an awk program that reads the
exported log as text and re-checks it. It has never heard of the application's
code. A Java twin of it runs in-process, and a test requires the two to agree.

## API

| | |
|---|---|
| `POST /api/purchases` | Buy a seat. `Idempotency-Key` makes retries safe. 201 bought, 409 taken or refunded, 402 payment failed. |
| `GET /api/seats`, `/api/stats` | Seat map and counts. |
| `GET /api/seats/{id}?asOf=<instant>` | A seat's state now, or as it was at any past moment. |
| `GET /api/seats/{id}/history` | Every event for the seat. Staff. |
| `GET /api/export`, `/api/audit` | The live log as NDJSON, and an in-process audit. Staff. |
| `GET /api/log/checkpoint`, `POST /api/log/verify` | Tamper evidence (Postgres only): a checkpoint of every seat's hash-chain head, and a check of the log against one. Staff. |
| `POST /graphql` | `seat`, `seats`, `stats`; staff-only `audit` and `history`. |
| `WS /ws` | STOMP; subscribe to `/topic/seats`. |
| `POST /api/demo/rush` | The live demo. Off in `prod`. |

Security defaults to **off** (and says so at startup). With `mode=jwt`, buyers are
whoever their token says, and the raw log, audit and history need a staff role.

## What was verified

| Claim | Against |
|---|---|
| Postgres store meets the same contract as the in-memory one | The same contract test, on real PostgreSQL 16 started in-process |
| Outbox loses nothing and keeps a seat's events ordered | A broker that drops requests and acknowledgements |
| Postgres to Kafka to read model | A real Kafka broker (KRaft) in-process |
| Money and seats balance under payment failures | A ledger check over concurrent purchases, on Postgres too |
| A crash at any saga step recovers | Crashes injected at every step |
| Roles are enforced | Real signed JWTs: forged, expired and tampered ones rejected |
| The two verifiers agree | Awk and Java on 40 random logs plus hand-built cases |
| Two servers share one database and Kafka safely | Two application instances, buyers split across both: no oversell, a retry on the other server returns the original purchase, both seat maps match the log |
| Rewriting the log is detectable | A test that plays a database owner: switches the append-only trigger off, then edits, deletes, rewrites and truncates history |
| The live page works | Its real JavaScript run under Node against a live server |

## Measured improvements

Found by measuring, with `mvn test -Dbench=true -Dtest=Benchmarks#<name>` on real
PostgreSQL and Kafka. Numbers are from one Windows laptop running the load
generator, the server and the database together, so read them as ratios.

| Weak point | Before | After |
|---|---|---|
| Exporting or rebuilding a 600,000-event log, 300 MB heap | OutOfMemoryError | completes in 3.5 s |
| Outbox rows kept after publishing (3,000 events) | 3,000, forever | 0 |
| Messages re-read from Kafka after a restart | 3,000 (the whole topic) | 0 |
| `GET /api/seats` with 20,000 seats, 32 readers | 51 reads/s, p50 615 ms | about 280 reads/s, p50 about 100 ms; an unchanged map is a 304 |
| Verifying the hash chain over 600,000 events, 300 MB heap | OutOfMemoryError | completes in 3.7 s |
| Purchases stranded by a crash | recovered only at restart | recovered by a scheduled reaper |
| Metrics | none | store, purchase, projection, outbox, saga and pool metrics at `/actuator/prometheus` |

Things that were tried and **did not help**, and were kept out or reverted:

- **A bigger connection pool.** Pool 10 to 80 cut connection wait from 111 s to 4 s
  and left throughput unchanged at about 900 purchases/s.
- **Persisting fewer saga states per purchase.** No measurable change; reverted.
- **Fewer statements per append.** Within noise on a loopback database. Kept, since it
  removes round trips that would cost real time over a network, but it is not a
  measured speedup.

Another early benchmark result was an artifact: the test database handle was not a
connection pool, which made appends look 80 times slower than they are.

## Limits and open problems

- **The 200,000-buyer goal is not met.** The largest run is 20,000 buyers on 2,000
  seats through Gatling, spread over 20 seconds: no failures, mean 11 ms, audit
  clean, in-memory store, no packet loss.
- **A single-instant stampede of 20,000 connections fails on the dev machine**
  (about 70% refused). The cause is not established.
- **Never run:** Docker Compose, Kubernetes, the systemd units, Keycloak, and the
  chaos script (Linux only). Compose is only schema-checked.
- **The payment provider is a simulator.**
- **The hash chain has two blind spots**, both tested and both closed only by a checkpoint kept outside the database: an attacker who recomputes every later hash, and someone who deletes the newest events. It is per seat, Postgres only, and the in-memory store has none.
- **The page has not been viewed in a browser**, only executed against the server.
- **The mutation harness (`make mutate`) is unfinished.** 15 of 24 mutants were
  confirmed caught, at earlier states of the code; 9 never ran, and its
  hang-killing watchdog is untested. `mutation-check.sh -c` works.
- **Substitutions:** a polling outbox relay instead of Debezium, a plain Kafka
  consumer instead of Kafka Streams, embedded Postgres and Kafka instead of
  Testcontainers.
- The rush demo still loads the whole log into memory (it is off in production).
- Each server process has its own Kafka consumer group and deletes it on a clean
  shutdown. A process that is killed leaves its group behind until Kafka expires it.
  Stopping the offset commits was tried and did not work: the container kept
  committing, and the cause was not found.

## Build and test

Needs JDK 17+, Maven, bash and curl.

```bash
make test        # Java and shell suites (no Docker needed)
make prove       # simulate a contended sale and audit it
./bin/turnstilectl load -u 20000 -s 2000     # Gatling, then audit the server's log
```

```
Java   Tests run: 123, Failures: 0, Errors: 0, Skipped: 0
Shell  all 38 shell assertions passed
```

## Layout

```
src/main/java/dev/turnstile/
  domain/  eventstore/  command/     state machine, log (memory + Postgres), handler
  saga/  payment/                    purchase saga; simulated payment gateway
  readmodel/  query/  messaging/     seat map, time travel, outbox and Kafka
  audit/  security/  web/            export and auditor, roles, REST/GraphQL/WS
src/main/resources/                  Flyway migrations, GraphQL schema, live page
src/load/java/                       Gatling simulation (only built with -Pload)
bin/turnstilectl                     up, export, verify, prove, load, mutate, test
scripts/                             awk verifier, mutation harness, chaos script
ops/                                 systemd, Keycloak realm, Kubernetes
```
