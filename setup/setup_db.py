import os
import time
import sys

nodes = int(sys.argv[1])

for i in range(1, nodes+1):
    os.system('pg_ctl stop -D ./../../ss'+str(i)+' -m smart')
    time.sleep(1)  
    os.system('rm -rf ./../../ss'+str(i))
    os.system('initdb ./../../ss'+str(i))
    os.system('pg_ctl start -D ./../../ss'+str(i)+' -o "-p 1500'+str(i)+' -c max_prepared_transactions=99"')
    os.system('/opt/homebrew/opt/postgresql@14/bin/createuser -s postgres -p 1500'+str(i))
    os.system('createdb aaryansharma -p 1500'+str(i))