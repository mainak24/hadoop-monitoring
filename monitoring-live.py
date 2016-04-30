#! /usr/bin/python2

# http://hadoop.apache.org/docs/stable/hadoop-yarn/hadoop-yarn-site/ResourceManagerRest.html

import errno, json, pycurl, sys
from io import BytesIO

base_hdfs_url = 'https://hador-c1.ics.muni.cz:50470'
base_yarn_urls = {
    'hador-c1.ics.muni.cz': 'https://hador-c1.ics.muni.cz:8090',
    'hador-c2.ics.muni.cz': 'https://hador-c2.ics.muni.cz:8090',
}
hosts = ['hador%s.ics.muni.cz' % i for i in ['-c1', '-c2'] + range(1, 25)]
maxslots = 16

# base_hdfs_url = 'https://took90.ics.muni.cz:50470'
# base_yarn_urls = {
#     'took90.ics.muni.cz': 'https://took90.ics.muni.cz:8090',
#     'took98.ics.muni.cz': 'https://took98.ics.muni.cz:8090',
# }
# hosts = ['took%s.ics.muni.cz' % i for i in [90] + range(98, 103)]
# maxslots = 2

# base_hdfs_url = 'https://myriad12.zcu.cz:50470'
# base_yarn_urls = {
#     'myriad12.zcu.cz': 'https://myriad12.zcu.cz:8090',
#     'myriad4.zcu.cz': 'https://myriad4.zcu.cz:8090',
# }
# hosts = ['myriad%s.zcu.cz' % i for i in [12, 4, 14, 15]]
# maxslots = 5

base_yarn_url = None
curl = pycurl.Curl()
debug = 4
monitoring = {}


def get_rest(base_url, url):
    if debug >= 3:
        print '# %s%s' % (base_url, url)

    b = BytesIO()
    curl.setopt(pycurl.URL, str(base_url + url))
    #curl.setopt(pycurl.WRITEDATA, b)
    curl.setopt(pycurl.WRITEFUNCTION, b.write)
    curl.perform()
    s = b.getvalue().decode('utf-8')

    if curl.getinfo(curl.RESPONSE_CODE) != 200:
        err = 'Status: %d' % curl.getinfo(curl.RESPONSE_CODE)
        print s
        curl.close()
        b.close()
        raise Exception(err)

    j = json.loads(s)

    if debug >= 4:
        print json.dumps(j, indent=4)

    return j


def get_cluster_info(base_url):
    try:
        j = get_rest(base_url, '/ws/v1/cluster/info')
    except pycurl.error:
        if curl.getinfo(pycurl.OS_ERRNO) == errno.ECONNREFUSED:
            j = json.loads('{"clusterInfo":{"state":"NO CONNETION"}}')
        else:
            raise

    if not j['clusterInfo']:
        if debug >= 3:
            print 'Error with YARN RM'
        return None

    ci = j['clusterInfo']
    if not 'haState' in ci.keys():
        ci['haState'] = 'NONE'
    if debug >= 3:
        print '[YARN] state=%s, haState=%s' % (ci['state'], ci['haState'])
    if ci['state'] != 'STARTED':
        return None

    return ci


curl.setopt(pycurl.HTTPAUTH, pycurl.HTTPAUTH_GSSNEGOTIATE)
curl.setopt(pycurl.USERPWD, ":")

# masters - High Availability status
for host, url in base_yarn_urls.iteritems():
    j = get_cluster_info(url)
    if j:
        if j['haState'] == 'ACTIVE':
            monitoring[host] = 100
            base_yarn_url = url
        elif j['haState'] == 'STANDBY':
            monitoring[host] = 0

if not base_yarn_url:
    print '[YARN] problem with RM'

if debug >= 3:
    print "Active YARN URL: %s" % base_yarn_url

# nodes - max(memory usage, slots usage)
if base_yarn_url:
    j = get_rest(base_yarn_url, '/ws/v1/cluster/nodes')
    if j and 'nodes' in j and 'node' in j['nodes']:
        nodes = j['nodes']['node']
        for node in nodes:
            if node['state'] == 'RUNNING':
                host = node['nodeHostName']
                used = float(node['usedMemoryMB'])
                avail = float(node['availMemoryMB'])
                containers = float(node['numContainers'])

                usage_mem = used / (used + avail) * 100
                usage_slots = containers / maxslots * 100

                monitoring[host] = max(usage_mem, usage_slots)

# HDFS
hdfs = get_rest(base_hdfs_url, '/jmx?qry=Hadoop:service=NameNode,name=FSNamesystem')
hdfs = hdfs['beans'][0]
cap_used = hdfs['CapacityUsed']
cap_remaining = hdfs['CapacityRemaining']
cap_overhead = hdfs['CapacityUsedNonDFS']
cap_total = cap_used + cap_remaining
hdfs_usage = float(cap_used) / (cap_used + cap_remaining) * 100
hdfs_overhead = float(cap_overhead) / (cap_used + cap_remaining) * 100
hdfs_underreplicatedblocks = hdfs['UnderReplicatedBlocks']
hdfs_blocks = hdfs['BlocksTotal']

# result
for host in hosts:
    if host in monitoring:
        usage = monitoring[host]
        print '%21s        %.1f %%' % (host, usage)
    else:
        print '%21s        -' % (host)
print
print "HDFS total: %.2f TB" % (float(cap_total) / 1024**4)
print "HDFS usage: %.1f %%" % hdfs_usage
print "HDFS overhead: %.1f %%" % hdfs_overhead
print "HDFS blocks: %d" % hdfs_blocks
print "HDFS underreplicated blocks: %d" % hdfs_underreplicatedblocks
