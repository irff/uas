import sys

args = sys.argv

if len(args) > 1:
    node_number = args[1]
    print 'NODE_ID = {}'.format(node_number)
