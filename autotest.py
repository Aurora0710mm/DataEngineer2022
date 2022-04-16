import urllib3
import json

print("downloding with urllib")
http = urllib3.PoolManager()
r = http.request('GET','http://www.psudateng.com:8000/getgetBreadCrumbData')
print("downloading with urllib")

with open('test.json','w') as test:
    temp = str(r.data, 'utf-8')
    print(type(temp))
    test.write(temp)
    test.close()