import json

def parse_jsonarr(data):
    data = eval(data)
    data = str(data).replace("'", "")
    js_data = json.loads(data)[0]
    return js_data

def parse_jsonToHbase(data, cf_prefix):
    '''
    :param json data
    :return:
    '''
    print("hbase_data : "+ json.dumps(data))
    ps_data={}
    for key, value in data.items():
        ps_data[cf_prefix + ':' + key] = str(value)
    #print("jsonarr" + ps_data)
    return ps_data