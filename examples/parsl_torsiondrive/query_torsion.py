import qcfractal.interface as portal

# Build a interface to the server 
client = portal.FractalClient("localhost:7777", verify=False)

td = client.query_procedures({"procedure": "torsiondrive"})
print(td[0])

assert len(td) > 0
