# Hadoop Usage Live Monitoring

Script for getting on-line information about Hadoop cluster usage.

See *--help* for usage. By default secured cluster is expected (*--gss* and *--ssl* options are on). Set also proper number of slots (max. number of containers according to heap memory).

## Example

    ./monitoring-live.py -d example.com -x 2 -m master1,master2 -n node1,node2,node3
