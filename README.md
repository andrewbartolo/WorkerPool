Schedules jobs in parallel across multiple host machines using SSH.

Supports GNU Parallel-like memory pressure handling (kills and re-schedules
youngest job when host is low on memory).

### Usage

In host machines' `/etc/ssh/sshd_config`, make sure to set the following. E.g.,
for a machine with 512 max. workers supported,

`MaxSessions 512`

`MaxStartups 512:10:1024`

This will ensure that the host doesn't drop SSH connections.
