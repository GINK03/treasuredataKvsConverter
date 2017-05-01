import os
import sys
import glob

import json
import pickle

import urllib.parse as PS
class DT(object):
  pass

def merge():
  tuuid_kwds = {}
  for name in reversed(sorted(glob.glob('red1/*.pkl'), key=lambda x:x)):
    tuuid_logs = pickle.loads(open(name, 'rb').read())
    for t, log in sorted(tuuid_logs.items()):
      ls = sorted([l for l in log.data], key=lambda x:x['date_time'])
      kwds = list( map(lambda x:(x[0], PS.unquote(x[1]), x[2]),  \
               filter(lambda x: x[1] != None and x[1] != '' and x[2] != None, \
                 [  ( l['date_time'],  \
                      l['request_uri'].get('ipao9702'), \
                      l['request_uri'].get('src') ) for l in ls])) )
      if kwds == [] :
        continue
      if tuuid_kwds.get(t) is None: tuuid_kwds[t] = []
      tuuid_kwds[t].extend(kwds)
    print("now iter {name}...".format(name=name))
  print("try save...")
  open('result.pkl', 'wb').write(pickle.dumps(tuuid_kwds))

def print2():
  tuuid_logs = pickle.loads(open('result.pkl', 'rb').read())
  for tuuid, kwds in tuuid_logs.items():
    print(tuuid, kwds)
  

if __name__ == '__main__':
  if '--merge' in sys.argv:
    merge()
  if '--print' in sys.argv:
    print2() 
