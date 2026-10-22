# Queue position scale probe

The release server was tested against one PostgreSQL launch while its queue grew from
1,000 to 1,000,000 waiting entries. `scripts/probe-position-scale.sh` seeded the rows,
then made 20 forced estimate requests and 20 forced exact-count requests for the last
customer at each depth. The table reports mean time inside the server's position path,
using deltas from `launchos_position_duration_ms`; it excludes curl startup and transport.

| Waiting entries | Estimated position | Exact count | Exact / estimate | Position error |
| ---: | ---: | ---: | ---: | ---: |
| 1,000 | 0.705 ms | 0.832 ms | 1.18× | 1 |
| 10,000 | 0.635 ms | 2.703 ms | 4.26× | 0 |
| 100,000 | 0.651 ms | 16.635 ms | 25.55× | 1 |
| 1,000,000 | 0.820 ms | 182.419 ms | 222.46× | 3 |

The estimate stays below 1 ms at this queue depth. Its counter can drift because
sequences have gaps and admissions may occur between requests, so the API labels the
answer as estimated and retains an exact-count option. This probe measures position
lookup at a million rows; it does not measure a million simultaneous clients, end-to-end
checkout throughput, or the simulator's ability to schedule a million customers.

Reproduce with a release build and a running node:

```bash
SKU=position-scale-new SAMPLES=20 scripts/probe-position-scale.sh
```
