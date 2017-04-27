import plyvel
import json
import pickle
import sys
import random
import time
heads = []
with open('header.csv') as f:
  for line in f:
    line = line.strip()
    name = line.split('\t').pop()
    heads.append(name)

db = plyvel.DB('kvs.ldb', create_if_missing=True)

with open('../../sdb/138717728.json.tmp', 'r') as f:
  ancker = time.time()
  for ind, line in enumerate(f):
    line = line.strip()
    line = line.replace('[[', '[')
    if ind%1000 == 0:
      print('now iter {ind} {time}'.format(ind=ind, time="%04f"%(time.time() - ancker)))
      ancker = time.time()
    if line[-1] == ',':
      o = json.loads(line[:-1])
      z = dict(zip(heads, o))
      tuuid = z['tuuid']
      dt    = z['date_time']
      if tuuid is None: 
        continue

      if db.get(bytes(tuuid, 'utf-8')) is None:
        db.put(bytes(tuuid, 'utf-8'), pickle.dumps({dt:z}) )
        #print('now iter {ind}, create new'.format(ind=ind), file=sys.stderr)
      else: 
        state     = pickle.loads( db.get(bytes(tuuid, 'utf-8')) )
        if state.get(dt) is not None:
          ...
          #print('no need, will continue...')
          continue
        state[dt] = z
        if random.random() < 0.001: 
          db.put(bytes(tuuid, 'utf-8'), pickle.dumps(state), sync=True )
          print('will sync...')
        else: 
          db.put(bytes(tuuid, 'utf-8'), pickle.dumps(state), sync=False )
        ...
 

