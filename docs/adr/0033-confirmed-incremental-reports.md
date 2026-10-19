# 0033. Confirmed incremental block reports

Status: accepted
Date: 2026-10-20

## Context

A node sends an incremental block report after each store change (a put, a repair copy, a rebuild, a trim or GC delete, a quarantine) and a full report every 30 s. Incremental reports were fire-and-forget: one the network lost was repaired only by the next full report. A lost report of a new copy left the commit unable to count it for up to 30 s (the client retries commit for 45 s), and a lost report of a delete left the metadata server counting a copy that was gone, which repair and trims could act on until then. The chaos harness's phantom check (design.md, invariant 14) has to allow for exactly this window.

## Options considered

| Option | For | Against |
|---|---|---|
| Leave it: full reports every 30 s | Nothing new | Up to 30 s of commit delay or a phantom copy per lost report |
| Shorter full reports | No protocol change | Every node lists its whole store more often, for a rare loss |
| An ack message per incremental report, resend on timeout (HDFS's DataNode keeps pending reports until the NameNode's RPC returns) | Direct | A message per report per peer, and a timer per report |
| Heartbeat says the latest report seq; the server asks for a full report on a gap | One field | A full report on request goes to one peer only, so the others see a gap that never closes and ask forever |
| **Each heartbeat lists the reports that peer has not confirmed; its ack sorts them into received and missing; the node resends the missing** | No new message kinds; extra messages only after a loss; resending is safe because reports are ordered by seq (bugs-found #7) | The node keeps unconfirmed reports per peer; a resend waits one or two heartbeats |

## Decision

The node keeps every incremental report it sends, per metadata peer, until that peer confirms it. Each heartbeat to a peer lists, in `unconfirmed_reports`, up to 64 seqs it sent that peer before the previous heartbeat, so reports still in flight are not asked about. The peer's ledger records the seqs of incremental reports it applied since its last full report; `Cluster.Confirm` answers each listed seq as received (applied, or at or below the last full report) or missing, in `reports_received` and `reports_missing` on the heartbeat ack. The node drops the received ones and resends the missing ones to that peer at once (`Stats.ReportsResent`). A full report to a peer drops every unconfirmed report numbered below it, since it covers them.

A report from an incarnation the peer has not applied anything of counts as missing: `Report` refuses a report that arrives before the heartbeat announcing a restart, so it must be sent again. Resending is idempotent: `Report` already ignores any change older than one it applied.

SIMPLIFIED: at most 4,096 unconfirmed reports per peer; past that the node forgets them and the next full report covers them, as every report was before. HDFS keeps a DataNode's pending incremental block reports until the NameNode accepts them.

## Consequences

- `TestLostIncrementalReportIsResent`: every node loses the report of its copy of the one chunk; the commit goes through 1.2 s after the writes, with 2 reports resent. With the resend disabled it is still waiting at 20 s.
- Extra messages appear only after a loss, but in the sim each one moves the 1% drop draws: the six scenario goldens were regenerated. Every scenario keeps its defining events; `gc`, `corrupt-chunk` and `kill-leader` now make fewer repair copies, because a chunk whose report was lost no longer looks short.
- Chaos: 1,000 default, 500 `--metas=3` and 500 `--ec` seeds pass.
- Heartbeats are now built per metadata peer; the count of heartbeat messages is unchanged.
- Full reports still run every 30 s: they catch what no incremental report describes (a disk edited by hand, a peer that restarted).

## At 100x scale

Thousands of nodes each listing up to 64 seqs per heartbeat is small next to the heartbeat itself. The pending set per peer bounds memory per node, not per cluster. HDFS batches incremental reports per heartbeat interval instead of sending one per change, which would also cut the message rate at high write rates; the confirmation scheme carries over unchanged.
