import json

#js_arr = '[{"A": "KSC01_V01", "C": "38.0", "B": "181011170000.63", "E": "2", "D": "14.9", "G": "1897", "F": "2048", "I": "02", "H": "2844", "K": "-0173", "J": "-0217", "M": "75", "L": "21601", "O": "00057", "N": "-0184", "Q": 20, "P": "00129", "S": -65535, "R": -65535, "U": "0", "T": "2596", "W": "12721.39155", "V": "3621.35120", "Y": 0, "X": "1.21", "Z": 120}]'

#js_data = json.loads(js_arr)

def parse_jsonarr(data):
    #data=json.dumps(data).encode("UTF-8")
    #print("data : "+ str(data))
    data = eval(data)
    data = str(data).replace("'", "")
    print(data)
    #data = data.replace("'[","").replace("]'","")

    #'{"A":"KSC01_V01","C":"38.0","B":"181011170000.63","E":"2","D":"14.9","G":"1897","F":"2048","I":"02","H":"2844","K":"-0173","J":"-0217","M":"75","L":"21601","O":"00057","N":"-0184","Q":20,"P":"00129","S":-65535,"R":-65535,"U":"0","T":"2596","W":"12721.39155","V":"3621.35120","Y":0,"X":"1.21","Z":120}'
    #data = data.replace("'{", "{").replace("}'", "}")


    js_data = json.loads(data)[0]
    print("js_data : "+str(js_data))
    #for js in js_data:
    #    print(js)
    #    json_data = js

    print("jsonarr : "+ json.dumps(js_data))
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