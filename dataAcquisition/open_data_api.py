from sodapy import Socrata

def get_open_data(url,endpoint,api_key,limit=1000,query=None):
    #input: Socrata endpoint for data, and OPTIONAL query
    client = Socrata(url,api_key)
    out = []
    idx=0
    data = client.get(endpoint,limit=limit,offset=idx,where=query)
    while len(data) > 0:
        #page through the results, appending to the out list
        out+=data
        idx+=limit
        data = client.get(endpoint,limit=limit,offset=idx,where=query)

    client.close()
    return out
        
        
    
