import qcfractal.interface as portal
import json

# Build a interface to the server 
client = portal.FractalClient("localhost:7777", verify=False)

td = client.get_procedures({"procedure": "torsiondrive"})
print(td[0])

assert len(td) > 0
