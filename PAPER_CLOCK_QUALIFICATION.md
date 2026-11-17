# Paper clock qualification diagnostic

Run `python -m lab.paper_run.clock_quality` from the Lab repository. It queries the local Windows Time service and prints a JSON series. Use `--interval` and `--samples` to span separate natural polls when needed. Save the output separately to retain evidence. Each invocation starts a new monotonic series. The tool has no input for saved reports and creates no production evidence files.

The diagnostic reports the Windows phase offset, root delay, root dispersion, sync age, reference identity and discipline state. Its Windows-only screen adds the absolute phase offset to root dispersion and half the root delay. The result is an estimate from reported fields, with no independent verification of upstream time. A local source, missing evidence or a reported bound over 250 ms fails the screen. The diagnostic reports a stale Windows sample after five hours and flags wall-clock movement that differs from monotonic elapsed time by over 100 ms. A healthy diagnostic requires distinct, advancing successful sync samples from a stable Windows network reference in `Sync`. In `Hold`, even low reported uncertainty remains unverified without independent measurements.

The diagnostic is separate from paper entry, exit, protection and account state. The production runner still uses its existing Windows gate. A clock check and exchange-price age answer different questions. Price validation still requires the original source and receipt order, monotonic checks and the original two-second maximum age. While clock qualification is unavailable the runner still reconciles existing accounting, expires pending ideas and records protection gaps for open positions. A new reference-priced exit requires qualified evidence.

The managed Kraken trade session also needs a valid subscription clock anchor. The transport checks cumulative wall-to-monotonic movement against the acknowledgement anchor at the same 100 ms limit used by the price consumer. Loss of the anchor triggers the existing bounded reconnect path. That path discards buffered points and retains the trade sequence watermark. A new connection must reauthorize and obtain a new acknowledgement before it can supply a price.

A prospective policy for `Hold` would require approved time references with independent upstream paths, multiple distinct agreeing samples, an uncertainty allowance for source distance, network asymmetry and monotonic time since sampling, and a fresh observation series after a clock step or reboot. Require the upper error bound to remain at or below 250 ms and account for that uncertainty when determining whether an exchange observation can be proven current. Independently verify source sequence and publication ordering. A reviewed prospective protocol and explicit activation would be required for any admission change.

## Repeated independent NTP review

Pass `--ntp-evidence PATH` once for each preserved precise-clock NTP window. The versioned `independent_ntp_review_v1` section reports the three non-smearing reference operators, observed offset direction (peer minus workstation), RTT, upstream delay and dispersion, sample age, timeouts, individual uncertainty and common offset interval. Each window needs at least three valid samples from each operator. The report checks that windows are distinct, that wall and monotonic elapsed time agree within the same boot, and that the latest evidence is still recent when reviewed. The Windows state still appears separately.

The report can show repeatable conditional agreement at the sampled times. Public UDP NTP and saved files have no authenticated provenance, and this review cannot observe changes between query windows. It always reports `admission_authorized: false`. A future Hold policy needs a trusted observer, fresh verified evidence at the economic transition and a separate prospective rollout. In addition to existing quote checks, that policy needs a conservative upper quote age including clock uncertainty and a final freshness check immediately before the HTTP path's SQLite commit.

References

- [Microsoft Windows Time discipline states](https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-w32t/b4abb4b0-6d6e-41e4-bd41-d54980d0598a)
- [Microsoft Windows Time tools and settings](https://learn.microsoft.com/en-us/windows-server/networking/windows-time-service/windows-time-service-tools-and-settings)
- [RFC 5905 synchronization distance](https://www.rfc-editor.org/rfc/rfc5905.html#section-11.2.3)
