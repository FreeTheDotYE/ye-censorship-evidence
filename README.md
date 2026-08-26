# Yemen censorship and network-interference evidence

This repository preserves reproducible public evidence about Internet censorship and network interference affecting Yemen. It tracks OONI measurements reported from Yemen, retains the complete public summary returned for each flagged measurement, and publishes daily totals needed to interpret those events in context.

The archive is maintained by [FreeTheDotYE](https://freethedotye.org/) as part of its work documenting the misuse of Yemen's Internet resources and infrastructure.

## What is preserved

- Every OONI result for Yemen returned with anomaly=true.
- OONI-confirmed results, which are retained within the anomaly corpus and separately counted.
- The complete public measurement-list summary, its canonical SHA-256 checksum, the stable measurement UID, and the official OONI measurement URL.
- Content-addressed summary variants when OONI returns different public metadata for the same UID; unique-measurement and variant counts remain separate.
- Daily measurement totals by test and by originating network, including ok, anomaly, confirmed, and failure counts.
- A deterministic day-partitioned event archive, a checksum index, a summary, and a public historical-backfill cursor.

The repository does not mirror OONI raw measurement bodies. Each event links to the authoritative raw record maintained by OONI. This keeps the Git history compact while preserving a stable citation and a checksummed copy of the public result summary.

## Evidence structure

- data/events/YYYY/MM/YYYY-MM-DD.jsonl.gz contains unique flagged measurement summaries.
- data/events/index.csv inventories every event file and its SHA-256 checksum.
- data/aggregates/daily_by_test.csv supplies daily denominators by OONI test.
- data/aggregates/daily_by_network.csv supplies daily denominators by probe ASN.
- data/summary.json gives corpus-level counts and date coverage.
- state/cursor.json records deterministic historical-backfill progress.
- scripts/update_ooni.py performs bounded recent collection and advances historical 180-day windows within a fixed request budget.
- scripts/validate.py verifies event checksums, source-summary hashes, ordering, uniqueness, state, and aggregate arithmetic.

All JSON objects are serialized with sorted keys. Gzip files use a zero timestamp. Re-running against unchanged OONI results therefore produces identical bytes and no Git commit. Event record count and unique measurement count are both published, so a source-summary correction or date-boundary variant cannot silently inflate the number of distinct measurements.

## Interpreting the evidence

An OONI anomaly is a measurement that warrants investigation; it is not, by itself, conclusive proof of deliberate blocking. A confirmed result carries OONI's confirmation status. Failures may reflect measurement or control failures. Researchers should use the linked raw record, the test methodology, repeated observations, network comparison, and the daily aggregate denominators before making an attribution.

The repository preserves those distinctions rather than turning every anomaly into an unsupported claim. This makes documented patterns easier to audit, cite, and compare over time.

## Reproduce locally

This repository uses only the Python standard library.

    python3 -m unittest discover -s tests -v
    python3 scripts/update_ooni.py
    python3 scripts/validate.py

The scheduled GitHub Action performs the same sequence daily and advances the archive's historical backfill in bounded 180-day windows.

## Source and licensing

The measurement source is the [OONI API](https://api.ooni.org/). Read [DATA-LICENSE.md](DATA-LICENSE.md) before redistributing the data. Repository code is licensed under the MIT License.
