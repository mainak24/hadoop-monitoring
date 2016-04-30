#! /usr/bin/python2

# http://hadoop.apache.org/docs/stable/hadoop-yarn/hadoop-yarn-site/ResourceManagerRest.html

import errno
import getopt
import json
import pycurl
import sys
from io import BytesIO

base_yarn_url = None
curl = pycurl.Curl()
debug = 4
domain = ''
maxslots = 1
gss = 1
ssl = 1
hdfs_masters = []
yarn_masters = []
yarn_nodes = []
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


def usage(name = sys.argv[0]):
    print '%s [OPTIONS]\n\
\n\
OPTIONS are:\n\
  -h, --help ........... help mesage\n\
  -d, --domain ......... domain to add to all hostnames\n\
  -g, --gss ............ enable SPNEGO authentization [1]\n\
  -s, --ssl ............ use https URLs [1]\n\
  -v, --verbose LEVEL .. verbosity level (0-4) [4]\n\
  -x, --max-slots ...... maximal number of containers per node\n\
  -m, --masters ........ list of HDFS and YARN master nodes\n\
  -f, --hdfs-masters ... list of HDFS masters (if different from masters, only one needed)\n\
  -y, --yarn-masters ... list of YARN masters (if different from masters)\n\
  -n, --yarn-nodes ..... list of YARN nodes\n\
' % name


try:
    opts, args = getopt.getopt(sys.argv[1:], 'hd:g:s:v:x:m:f:y:n:', [
        'help',
        'domain=',
        'gss=',
        'max-slots=',
        'ssl=',
        'verbose=',
        'masters=',
        'hdfs-masters=',
        'yarn-masters=',
        'yarn-nodes=',
    ])
except getopt.GetoptError:
    print 'Error in arguments'
    sys.exit(2)
for opt, arg in opts:
    if opt in ['-h', '--help']:
        usage()
        sys.exit()
    elif opt in ['-d', '--domain']:
        domain = arg
    elif opt in ['-g', '--gss']:
        gss = int(arg)
    elif opt in ['-s', '--ssl']:
        ssl = int(arg)
    elif opt in ['-v', '--verbosity']:
        debug = int(arg)
    elif opt in ['-x', '--max-slots']:
        maxslots = int(arg)
    elif opt in ['-m', '--masters']:
        yarn_masters = arg.split(r',')
        hdfs_masters = yarn_masters
    elif opt in ['-f', '--hdfs-masters']:
        hdfs_masters = arg.split(r',')
    elif opt in ['-y', '--yarn-masters']:
        yarn_masters = arg.split(r',')
    elif opt in ['-n', '--yarn-nodes']:
        yarn_nodes = arg.split(r',')
if not hdfs_masters:
    usage()
    print 'HDFS master not specified (-m or -f options)'
    sys.exit(1)
if not yarn_masters or not yarn_nodes:
    usage()
    print 'YARN masters or nodes not specified (-m or -y, -n options)'
    sys.exit(1)

if domain:
    domain = '.' + domain
hdfs_masters = [host + domain for host in hdfs_masters]
yarn_masters = [host + domain for host in yarn_masters]
yarn_nodes = [host + domain for host in yarn_nodes]
if gss:
    curl.setopt(pycurl.HTTPAUTH, pycurl.HTTPAUTH_GSSNEGOTIATE)
    curl.setopt(pycurl.USERPWD, ":")
if ssl:
    base_hdfs_url = 'https://%s:50470' % hdfs_masters[0]
    base_yarn_urls = {host: 'https://%s:8090' % host for host in yarn_masters}
else:
    base_hdfs_url = 'http://%s:50070' % hdfs_masters[0]
    base_yarn_urls = ['http://%s:8088' % host for host in yarn_masters]
for yarn_master in yarn_masters:
    add = []
    if yarn_master not in yarn_nodes:
        add = add + [yarn_master]
    yarn_nodes = add + yarn_nodes
if debug >= 4:
    print 'HDFS URL: %s' % base_hdfs_url
    print 'YARN URL: %s' % ', '.join(base_yarn_urls.values())
    print 'YARN hosts: %s' % ', '.join(yarn_nodes)

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
for host in yarn_nodes:
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
