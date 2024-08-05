# import flask module
from flask import Flask, request, jsonify
from elasticsearch import Elasticsearch
import json
import time

app = Flask(__name__)
es = Elasticsearch("http://localhost:9200")

#xóa dấu tiếng việt 
s1 = u'ÀÁÂÃÈÉÊÌÍÒÓÔÕÙÚÝàáâãèéêìíòóôõùúýĂăĐđĨĩŨũƠơƯưẠạẢảẤấẦầẨẩẪẫẬậẮắẰằẲẳẴẵẶặẸẹẺẻẼẽẾếỀềỂểỄễỆệỈỉỊịỌọỎỏỐốỒồỔổỖỗỘộỚớỜờỞởỠỡỢợỤụỦủỨứỪừỬửỮữỰựỲỳỴỵỶỷỸỹ'
s0 = u'AAAAEEEIIOOOOUUYaaaaeeeiioooouuyAaDdIiUuOoUuAaAaAaAaAaAaAaAaAaAaAaAaEeEeEeEeEeEeEeEeIiIiOoOoOoOoOoOoOoOoOoOoOoOoUuUuUuUuUuUuUuYyYyYyYy'
def remove_accents(input_str):
	s = ''
	for c in input_str:
		if c in s1:
			s += s0[s1.index(c)]
		else:
			s += c
	return str(s).lower()
@app.route("/test",methods=['GET', 'POST'])
def test():
   return jsonify({
            "data":{
                "listuser":"ok"
                
            }
        })
@app.route("/getListNewRegister",methods=['GET', 'POST'])
def getListNewRegister():
    try:
        data = request.form
        client = Elasticsearch('http://localhost:9200')
        print(data)
        index="company"
        if( 'pageSize' in data and data['pageSize'] != '' ):
            pageSize = int(data['pageSize'])
        else:
            pageSize = 30
        
        if( 'page' in data and data['page'] != '' ):
            page = int(data['page'])
        else:
            page = 1
        skip = (page -1)*pageSize
        must = []

        #must.append({
            #"match":
                #{
                #    "usc_md5.keyword" : "" 
                #}
        #})

        if( 'idTimViec365' in data and data['idTimViec365'] != '' ):
            must.append({
                        "match": {
                            "usc_id": int(data['idTimViec365'])
                            }
            })
        if( 'dk' in data and data['dk'] != ''):
            findword = str(data['dk'])
            if( findword != '0'):
                if(findword == '1'):
                    must.append({
                        "bool": {
                            "should": [
                                {"match": {"dk": "0"}},
                                {"match": {"dk": "1"}}
                            ]
                        }
                    
                    })
                else:
                    must.append({
                                "match": {
                                    "dk": findword
                                    }
                    })
            
        if( 'userName' in data and data['userName'] != ''):
            findword = remove_accents(str(data['userName']))
            words = findword.split()
            #for findword_child in words:
                #must.append({
                    #"regexp": {
                        #"usc_name_novn": {
                            #"value": ".*" + findword_child + ".*"
                        #}
                    #}
                #})
            must.append({
                        "match_phrase": {
                            "usc_name_novn": findword
                            }
            })
        if( 'name' in data and data['name'] != ''):
            findword = remove_accents(str(data['name']))
            words = findword.split()
            for findword_child in words:
                must.append({
                    "regexp": {
                        "usc_name_novn": {
                            "value": ".*" + findword_child + ".*"
                        }
                    }
                })
            # must.append({
            #             "match_phrase": {
            #                 "usc_name": findword
            #                 }
            # })
        if( 'phone' in data and data['phone'] != ''):
            findword = str(data['phone'])
            must.append({
                        "regexp": {
                            "usc_phone": {
                                   "value": ".*"+findword+".*"
                                }
                            }
            })
        if( 'phoneTK' in data and data['phoneTK'] != ''):
            findword = str(data['phoneTK'])
            must.append({
                        "regexp": {
                            "usc_phone_tk": {
                                   "value": ".*"+findword+".*"
                                }
                            }
            })
        
        if( 'emailContact' in data and data['emailContact'] != ''):
            findword = str(data['emailContact'])
            words = findword.split('@')
            for findword_child in words:
                must.append({
                    "regexp": {
                        "usc_email": {
                            "value": ".*" + findword_child.lower() + ".*"
                        }
                    }
                })
            # must.append({
            #             "regexp": {
            #                 "usc_email": {
            #                        "value": ".*"+findword+".*"
            #                     }
            #                 }
            # })
              
        if( 'email' in data and data['email'] != ''):
            findword = str(data['email'])
            words = findword.split('@')
            for findword_child in words:
                must.append({
                    "regexp": {
                        "usc_name_email": {
                            "value": ".*" + findword_child.lower() + ".*"
                        }
                    }
                })
            findword = remove_accents(str(data['email']))
           
        if( 'city' in data and data['city'] != ''):
            must.append({
                        "term": {
                            "usc_city": int(data['city'])
                            }
            })  
          
        if( 'district' in data and data['district'] != ''):
            must.append({
                        "term": {
                            "usc_qh": int(data['district'])
                            }
            })   
             
        if( 'supportKD' in data and data['supportKD'] != ''):
            must.append({
                        "term": {
                            "usc_kd": int(data['supportKD'])
                            }
            }) 
        
        if( 'checkVip' in data and data['checkVip'] != ''):
            if( str(data['checkVip']) == '1' ):#Vip
                    must.append({
                        "range": {
                                    "point_usc": {
                                        "gte": 1
                                    }
                        }
                    })
            if( str(data['checkVip']) == '2' ):#Từng vip
                    must.append({
                        "match": {
                                    "point_usc": 0
                        }
                    })
                    must.append({
                        "range": {
                                    "ngay_reset_diem_ve_0": {
                                        "gte": 1
                                    }
                        }
                    })
            if( str(data['checkVip']) == '3' ):#Chưa vip
                must.append({
                    "bool": {
                        "should": [
                            {
                            "bool": {
                                "must": [
                                {
                                    "match": {
                                        "ngay_reset_diem_ve_0": 0
                                    }
                                },
                                {
                                    "match": {
                                        "point_usc": 0
                                    }
                                }
                                ]
                            }
                            },
                            {
                            "bool": {
                                "must_not": [
                                {
                                    "exists": {
                                        "field": "point_usc"
                                    }
                                }
                                ]
                            }
                            }
                        ]
                        }
                    })
                    
             
        if( ( 'fromDate' in data and data['fromDate'] != '' ) and ( 'toDate' not in data or data['toDate'] == '' ) ):
            must.append({
                "range": {
                            "usc_create_time": {
                                "gte": int(data['fromDate'])
                            }
                }
            })
        
        if( ( 'fromDate' not in data or data['fromDate'] == '' ) and ( 'toDate' in data and data['toDate'] != '' ) ):
            must.append({
                "range": {
                            "usc_create_time": {
                                "lte": int(data['toDate'])
                            }
                }
            })
        if( ( 'fromDate' in data and data['fromDate'] != '' ) and ( 'toDate' in data and data['toDate'] != '' ) ):
            must.append({
                "range": {
                            "usc_create_time": {
                                "gte": int(data['fromDate'])
                            }
                }
            })
            must.append({
                "range": {
                            "usc_create_time": {
                                "lte": int(data['toDate'])
                            }
                }
            }) 
        

        query = {
            "bool": {
                "must": must
            }
        }
        
        print(query)
        # print(page)
        # print(pageSize)
        listuser = client.search(
            index=index,
            body={  
                    "from": skip,
                    "size": pageSize,
                    "sort":[
                        { "usc_create_time" : "desc" }
                    ],
                    "query": query,
                }
        )
        
        listuser = listuser["hits"]["hits"]
        listuserfinal = []
        for user in listuser:
            obj = user["_source"]
            listuserfinal.append(int(obj["usc_id"]))

        count = client.count(
            index=index,
            body={  
                    "query": query,
                }
        )
        return jsonify({
            "data":{
                "listuser":listuserfinal,
                "query":query,
                "count":int(count['count']),
                "listuserFull":listuser
            }
        })
    except Exception as error:
        print("An exception occurred:", error)
        return jsonify({
            "error":"err"
        })

@app.route("/getListNewRegisterAdminBoPhan",methods=['GET', 'POST'])
def getListNewRegisterAdminBoPhan():
    try:
        data = request.form
        client = Elasticsearch('http://localhost:9200')
        index="company"
        if( 'pageSize' in data and data['pageSize'] != '' ):
            pageSize = int(data['pageSize'])
        else:
            pageSize = 30
        
        if( 'page' in data and data['page'] != '' ):
            page = int(data['page'])
        else:
            page = 1
        skip = (page -1)*pageSize
        must = []

        must.append({
            "match":
                {
                    "usc_md5.keyword" : "" 
                }
        })

        if( 'idTimViec365' in data and data['idTimViec365'] != '' ):
            must.append({
                        "match": {
                            "usc_id": int(data['idTimViec365'])
                            }
            })
        if( 'dk' in data and data['dk'] != ''):
            findword = str(data['dk'])
            if( findword != '0'):
                if(findword == '1'):
                    must.append({
                        "bool": {
                            "should": [
                                {"match": {"dk": "0"}},
                                {"match": {"dk": "1"}}
                            ]
                        }
                    
                    })
                else:
                    must.append({
                                "match": {
                                    "dk": findword
                                    }
                    })
            
        if( 'userName' in data and data['userName'] != ''):
            findword = remove_accents(str(data['userName']))
            words = findword.split()
            for findword_child in words:
                must.append({
                    "regexp": {
                        "usc_name_novn": {
                            "value": ".*" + findword_child + ".*"
                        }
                    }
                })
            # must.append({
            #             "match_phrase": {
            #                 "usc_name": findword
            #                 }
            # })
        if( 'name' in data and data['name'] != ''):
            findword = remove_accents(str(data['name']))
            words = findword.split()
            for findword_child in words:
                must.append({
                    "regexp": {
                        "usc_name_novn": {
                            "value": ".*" + findword_child + ".*"
                        }
                    }
                })
            # must.append({
            #             "match_phrase": {
            #                 "usc_name": findword
            #                 }
            # })
        if( 'phone' in data and data['phone'] != ''):
            findword = str(data['phone'])
            must.append({
                        "regexp": {
                            "usc_phone": {
                                   "value": ".*"+findword+".*"
                                }
                            }
            })
        if( 'phoneTK' in data and data['phoneTK'] != ''):
            findword = str(data['phoneTK'])
            must.append({
                        "regexp": {
                            "usc_phone_tk": {
                                   "value": ".*"+findword+".*"
                                }
                            }
            })
        
        if( 'emailContact' in data and data['emailContact'] != ''):
            findword = str(data['emailContact'])
            words = findword.split('@')
            for findword_child in words:
                must.append({
                    "regexp": {
                        "usc_email": {
                            "value": ".*" + findword_child.lower() + ".*"
                        }
                    }
                })
            # must.append({
            #             "regexp": {
            #                 "usc_email": {
            #                        "value": ".*"+findword+".*"
            #                     }
            #                 }
            # })
              
        if( 'email' in data and data['email'] != ''):
            findword = remove_accents(str(data['email']))
            must.append({
                        "regexp": {
                            "usc_name_email": {
                                   "value": ".*"+findword+".*"
                                }
                            }
            })  
        if( 'city' in data and data['city'] != ''):
            must.append({
                        "term": {
                            "usc_city": int(data['city'])
                            }
            })  
          
        if( 'district' in data and data['district'] != ''):
            must.append({
                        "term": {
                            "usc_qh": int(data['district'])
                            }
            })   
             
        if( 'supportKD' in data and data['supportKD'] != ''):
            if(data['supportKD'] != '0'):
                must.append({
                            "term": {
                                "usc_kd": int(data['supportKD'])
                                }
                }) 
        
        if( 'checkVip' in data and data['checkVip'] != ''):
            if( str(data['checkVip']) == '1' ):#Vip
                    must.append({
                        "range": {
                                    "point_usc": {
                                        "gte": 1
                                    }
                        }
                    })
            if( str(data['checkVip']) == '2' ):#Từng vip
                    must.append({
                        "match": {
                                    "point_usc": 0
                        }
                    })
                    must.append({
                        "range": {
                                    "ngay_reset_diem_ve_0": {
                                        "gte": 1
                                    }
                        }
                    })
            if( str(data['checkVip']) == '3' ):#Chưa vip
                must.append({
                    "bool": {
                        "should": [
                            {
                            "bool": {
                                "must": [
                                {
                                    "match": {
                                        "ngay_reset_diem_ve_0": 0
                                    }
                                },
                                {
                                    "match": {
                                        "point_usc": 0
                                    }
                                }
                                ]
                            }
                            },
                            {
                            "bool": {
                                "must_not": [
                                {
                                    "exists": {
                                        "field": "point_usc"
                                    }
                                }
                                ]
                            }
                            }
                        ]
                        }
                    })
                    
             
        if( ( 'fromDate' in data and data['fromDate'] != '' ) and ( 'toDate' not in data or data['toDate'] == '' ) ):
            must.append({
                "range": {
                            "usc_create_time": {
                                "gte": int(data['fromDate'])
                            }
                }
            })
        
        if( ( 'fromDate' not in data or data['fromDate'] == '' ) and ( 'toDate' in data and data['toDate'] != '' ) ):
            must.append({
                "range": {
                            "usc_create_time": {
                                "lte": int(data['toDate'])
                            }
                }
            })
        if( ( 'fromDate' in data and data['fromDate'] != '' ) and ( 'toDate' in data and data['toDate'] != '' ) ):
            must.append({
                "range": {
                            "usc_create_time": {
                                "gte": int(data['fromDate'])
                            }
                }
            })
            must.append({
                "range": {
                            "usc_create_time": {
                                "lte": int(data['toDate'])
                            }
                }
            }) 
        

        query = {
            "bool": {
                "must": must
            }
        }
        
        # print(query)
        # print(page)
        # print(pageSize)
        listuser = client.search(
            index=index,
            body={  
                    # "from": skip,
                    "size": 6000,
                    "sort":[
                        { "usc_create_time" : "desc" }
                    ],
                    "query": query,
                }
        )
        
        listuser = listuser["hits"]["hits"]
        listuserfinal = []
        for user in listuser:
            obj = user["_source"]
            listuserfinal.append(int(obj["usc_id"]))

        count = client.count(
            index=index,
            body={  
                    "query": query,
                }
        )
        return jsonify({
            "data":{
                "listuser":listuserfinal,
                "query":query,
                "count":int(count['count']),
            }
        })
    except Exception as error:
        print("An exception occurred:", error)
        return jsonify({
            "error":"err"
        })

# ứng viên đăng ký mới  candi_register
@app.route("/candi_register",methods=['GET', 'POST'])
def candi_register():
    try:
        data = request.form
        client = Elasticsearch('http://localhost:9200')
        index="users"
        pageSize = int(data['pageSize'])
        page = int(data['page'])
        skip = (page -1)*pageSize
        must = []
        must.append({
            "range": {
                    "idTimViec365": {
                        "gte": 1
                    }
                }
        })
        if(str(data['use_id']) != "0"):
            must.append({
                        "match": {
                            "idTimViec365":int(data['use_id'])
                            }
            })
        if(str(data['use_first_name']) != "0"):
            array = str(data['use_first_name']).split()
            for ele in array:
                findword = remove_accents(str(ele))
                must.append({
                            "regexp": {
                                "userName": {
                                    "value": ".*"+findword+".*"
                                    }
                                }
                })
        if(str(data['use_phone']) != "0"):
            findword = str(data['use_phone'])
            must.append({
                        "regexp": {
                            "phone": {
                                   "value": ".*"+findword+".*"
                                }
                            }
            })
        if(str(data['use_email']) != "0"):
            findword = remove_accents(str(data['use_email']))
            must.append({
                        "regexp": {
                            "email": {
                                   "value": ".*"+findword+".*"
                                }
                            }
            })    
        if(str(data['use_phone_tk']) != "-1"):
            findword = remove_accents(str(data['use_phone_tk']))
            must.append({
                        "regexp": {
                            "phoneTK": {
                                   "value": ".*"+findword+".*"
                                }
                            }
            })  
        if(str(data['use_email_lh']) != "0"):
            findword = remove_accents(str(data['use_email_lh']))
            must.append({
                        "regexp": {
                            "emailContact": {
                                   "value": ".*"+findword+".*"
                                }
                            }
            })    
        if(str(data['cv_title']) != "0"):
            array = str(data['cv_title']).split()
            for ele in array:
                findword = remove_accents(str(ele))
                must.append({
                            "regexp": {
                                "inForPerson.candidate.cv_title": {
                                    "value": ".*"+findword+".*"
                                    }
                                }
                })
        if(str(data['register'])!= "-1"):
            must.append({
                        "match": {
                            "fromDevice":int(data['register'])
                            }
            })
        if(str(data['category'])!= "0"):
            must.append({
                        "wildcard": {
                            "cv_cate_id": str(data['category'])
                            }
            })
        if(str(data['city'])!= "0"):
            must.append({
                        "wildcard": {
                            "cv_city_id": str(data['city'])
                            }
            })
        if(str(data['authentic'])!= "-1"):
            must.append({
                        "match": {
                            "authentic":int(data['authentic'])
                            }
            })
        
        if(str(data['uv_time']) == "0"):
            MonthsAgo = time.time() - ( 30 * 3 * 86400 ) 
            must.append({
                "range": {
                            "updatedAt": {
                                "gte": int(MonthsAgo)
                            }
                }
            })
        if(str(data['uv_time']) == "1"):
            MonthsAgo = time.time() - ( 30 * 6 * 86400 ) 
            must.append({
                "range": {
                            "updatedAt": {
                                "gte": int(MonthsAgo)
                            }
                }
            })
        if(str(data['uv_time']) == "2"):
            MonthsAgo = time.time() - ( 30 * 12 * 86400 ) 
            must.append({
                "range": {
                            "updatedAt": {
                                "gte": int(MonthsAgo)
                            }
                }
            })
        if(str(data['uv_time']) == "3"):
            MonthsAgo = time.time() - ( 30 * 24 * 86400 ) 
            must.append({
                "range": {
                            "updatedAt": {
                                "gte": int(MonthsAgo)
                            }
                }
            })
        
        if((str(data['time_start']) != "0") and (str(data['time_end']) == "0") ):
            must.append({
                "range": {
                            "updatedAt": {
                                "gte": int(data['time_start'])
                            }
                }
            })
        if((str(data['time_start']) == "0") and (str(data['time_end']) != "0") ):
            must.append({
                "range": {
                            "updatedAt": {
                                "gte": int(data['time_end'])
                            }
                }
            })
        if((str(data['time_start']) != "0") and (str(data['time_end']) != "0") ):
            must.append({
                "range": {
                            "updatedAt": {
                                "gte": int(data['time_start'])
                            }
                }
            })
            must.append({
                "range": {
                            "updatedAt": {
                                "lte": int(data['time_end'])
                            }
                }
            })
        query = {
            "bool": {
                "must":must,
                "must_not": [
                    {
                        "match": {
                            "type": 1 
                        }
                    },
                    {
                        "match": {
                            "fromDevice": 4 
                        }
                    },
                    {
                        "match": {
                            "fromDevice": 7  
                        }
                    },
                    {
                        "match": {
                            "phone.keyword": ""
                        }
                    }
                ]
            }
        }
        

        listuser = client.search(
            index=index,
            body={  
                    "from":skip,
                    "size": pageSize,
                    "sort":[
                        { "createdAt" : "desc" }
                    ],
                    "query": query,
                }
        )
        listuser = listuser["hits"]["hits"]
        listuserfinal = []
        for user in listuser:
            obj = user["_source"]
            listuserfinal.append(int(obj["chat_id"]))

        count = client.count(
            index=index,
            body={  
                    "query": query,
                }
        )
        return jsonify({
            "data":{
                "listuser":listuserfinal,
                "count":int(count['count']),
                "query":query
            }
        })
    except Exception as error:
        print("An exception occurred:", error)
        return jsonify({
            "error":"err"
        })

@app.route("/candi_register_2",methods=['GET', 'POST'])
def candi_register_2():
    try:
        data = request.form
        client = Elasticsearch('http://localhost:9200')
        index="candidate"
        if( 'pageSize' in data and data['pageSize'] != '' ):
            pageSize = int(data['pageSize'])
        else:
            pageSize = 30
        
        if( 'page' in data and data['page'] != '' ):
            page = int(data['page'])
        else:
            page = 1
        skip = (page -1)*pageSize
        must = []
        must.append({
            "bool": 
            {
                "should": 
                [
                    {
                        "bool": {
                        "must": [
                            {
                            "exists": {
                                "field": "use_phone"
                            }
                            }
                        ], 
                        "must_not": [
                            {
                            "match": {
                                "use_phone.keyword": ""
                            }
                            }
                        ]
                        }
                    },
                    {
                        "bool": {
                        "must": [
                            {
                            "exists": {
                                "field": "use_phone_tk"
                            }
                            }
                        ], 
                        "must_not": [
                            {
                            "match": {
                                "use_phone_tk.keyword": ""
                            }
                            }
                        ]
                        }
                    }
                ]
            }
        })
        must.append({
            "range": {
                "percents": {
                    "gte": 45
                }
            }
        })
        must.append({
            "exists": {
                "field": "cv_user_id"
            }
        })
        if( 'use_id' in data and data['use_id'] != '' ):
            must.append({
                        "match": {
                            "use_id":int(data['use_id'])
                            }
            })
        if('use_first_name' in data and data['use_first_name'] != ''):
            array = str(data['use_first_name']).split()
            for ele in array:
                findword = remove_accents(str(ele))
                must.append({
                            "regexp": {
                                "use_first_name": {
                                    "value": ".*"+findword+".*"
                                    }
                                }
                })
        if('use_phone' in data and data['use_phone'] != ''):
            findword = str(data['use_phone'])
            must.append({
                        "regexp": {
                            "use_phone": {
                                   "value": ".*"+findword+".*"
                                }
                            }
            })
        if('use_email' in data and data['use_email'] != ''):
            findword = remove_accents(str(data['use_email']))
            must.append({
                        "regexp": {
                            "use_email": {
                                   "value": ".*"+findword+".*"
                                }
                            }
            })    
        if('use_phone_tk' in data and data['use_phone_tk'] != ''):
            findword = remove_accents(str(data['use_phone_tk']))
            must.append({
                        "regexp": {
                            "use_phone_tk": {
                                   "value": ".*"+findword+".*"
                                }
                            }
            })  
        if('use_email_lh' in data and data['use_email_lh'] != ''):
            findword = remove_accents(str(data['use_email_lh']))
            must.append({
                        "regexp": {
                            "use_email_lienhe": {
                                   "value": ".*"+findword+".*"
                                }
                            }
            })    
        if('cv_title' in data and data['cv_title'] != ''):
            array = str(data['cv_title']).split()
            for ele in array:
                findword = remove_accents(str(ele))
                must.append({
                            "regexp": {
                                "cv_title": {
                                    "value": ".*"+findword+".*"
                                    }
                                }
                })
        if('register' in data and data['register'] != ''):
            must.append({
                        "match": {
                            "dk":int(data['register'])
                            }
            })
        if('category' in data and data['category'] != ''):
            must.append({
                        "wildcard": {
                            "cv_cate_id": str(data['category'])
                            }
            })
            must.append({
                "match": {
                        "use_check": 1
                        }
            })
            must.append({
                "match": {
                        "use_show": 1
                        }
            })
        if('city' in data and data['city'] != ''):
            must.append({
                        "wildcard": {
                            "cv_city_id": str(data['city'])
                            }
            })
        if('authentic' in data and data['authentic'] != ''):
            must.append({
                        "match": {
                            "use_authentic":int(data['authentic'])
                            }
            })
        if('use_check' in data and data['use_check'] != ''):
            must.append({
                        "match": {
                            "use_check":int(data['use_check'])
                            }
            })
        if('use_show' in data and data['use_show'] != ''):
            must.append({
                        "match": {
                            "use_show":int(data['use_show'])
                            }
            })
        if(str(data['uv_time']) == "0"):
            MonthsAgo = time.time() - 8035200
            must.append({
                "range": {
                            "use_update_time": {
                                "gte": int(MonthsAgo)
                            }
                }
            })
        if(str(data['uv_time']) == "1"):
            MonthsAgo = time.time() - 16070400
            must.append({
                "range": {
                            "use_update_time": {
                                "gte": int(MonthsAgo)
                            }
                }
            })
        if(str(data['uv_time']) == "2"):
            MonthsAgo = time.time() - 31536000
            must.append({
                "range": {
                            "use_update_time": {
                                "gte": int(MonthsAgo)
                            }
                }
            })
        if(str(data['uv_time']) == "3"):
            MonthsAgo = time.time() - 63072000 
            must.append({
                "range": {
                            "use_update_time": {
                                "gte": int(MonthsAgo)
                            }
                }
            })
        
        if( ('time_start' in data and data['time_start'] != '' ) and ( 'time_end' not in data or data['time_end'] == '' ) ):
            must.append({
                "range": {
                            "use_create_time": {
                                "gte": int(data['time_start'])
                            }
                }
            })
        if( ( 'time_start' not in data or data['time_start'] == '' ) and ( 'time_end' in data and data['time_end'] != '' ) ):
            must.append({
                "range": {
                            "use_create_time": {
                                "gte": int(data['time_end'])
                            }
                }
            })
        if( ( 'time_start' in data and data['time_start'] != '' ) and ( 'time_end' in data and data['time_end'] != '' ) ):
            must.append({
                "range": {
                            "use_create_time": {
                                "gte": int(data['time_start'])
                            }
                }
            })
            must.append({
                "range": {
                            "use_create_time": {
                                "lte": int(data['time_end'])
                            }
                }
            })
        
        query = {
            "bool": {
                "must":must,
                "must_not": [
                    {
                        "match": {
                            "dk": 4 
                        }
                    },
                    {
                        "match": {
                            "dk": 7  
                        }
                    }
                ]
            }
        }
        # print(query)
        # print(page)
        # print(pageSize)
        listuser = client.search(
            index=index,
            body={  
                    "from":skip,
                    "size": pageSize,
                    "sort":[
                        { "use_create_time" : "desc" }
                    ],
                    "query": query,
                }
        )
        listuser = listuser["hits"]["hits"]
        listuserfinal = []
        for user in listuser:
            obj = user["_source"]
            listuserfinal.append(int(obj["use_id"]))

        count = client.count(
            index=index,
            body={  
                    "query": query,
                }
        )
        return jsonify({
            "data":{
                "listuser":listuserfinal,
                "count":int(count['count']),
                "query":query
            }
        })
    except Exception as error:
        print("An exception occurred:", error)
        return jsonify({
            "error":"err"
        }) 

# ứng viên sửa cập nhật hồ sơ candi_update
@app.route("/candi_update",methods=['GET', 'POST'])
def candi_update():
    try:
        data = request.form
        client = Elasticsearch('http://localhost:9200')
        index="users"
        pageSize = int(data['pageSize'])
        page = int(data['page'])
        skip = (page -1)*pageSize
        must = []
        must.append({
            "exists": {
                "field": "phone"
                }
            })
        must.append({
            "range": {
                            "idTimViec365": {
                                "gte": 1
                            }
                    }
        })
        must.append({
            "range": {
                            "updatedAt": {
                                "gte": 1
                            }
                    }
        })
        if(str(data['use_first_name']) != "0"):
            findword = remove_accents(str(data['use_first_name']))
            words = findword.split()
            for findword_child in words:
                must.append({
                    "regexp": {
                        "userName": {
                            "value": ".*" + findword_child + ".*"
                        }
                    }
                })
        if(str(data['use_phone']) != "0"):
            findword = str(data['use_phone'])
            must.append({
                        "regexp": {
                            "phone": {
                                   "value": ".*"+findword+".*"
                                }
                            }
            })
        if(str(data['use_email']) != "0"):
            findword = remove_accents(str(data['use_email']))
            must.append({
                        "regexp": {
                            "email": {
                                   "value": ".*"+findword+".*"
                                }
                            }
            })    
        if(str(data['use_phone_tk']) != "0"):
            findword = remove_accents(str(data['use_phone_tk']))
            must.append({
                        "regexp": {
                            "phoneTK": {
                                   "value": ".*"+findword+".*"
                                }
                            }
            })  
             
        if((str(data['time_start']) != "0") and (str(data['time_end']) == "0") ):
            must.append({
                "range": {
                            "updatedAt": {
                                "gte": int(data['time_start'])
                            }
                }
            })
        if((str(data['time_start']) == "0") and (str(data['time_end']) != "0") ):
            must.append({
                "range": {
                            "updatedAt": {
                                "gte": int(data['time_end'])
                            }
                }
            })
        if((str(data['time_start']) != "0") and (str(data['time_end']) != "0") ):
            must.append({
                "range": {
                            "updatedAt": {
                                "gte": int(data['time_start'])
                            }
                }
            })
            must.append({
                "range": {
                            "updatedAt": {
                                "lte": int(data['time_end'])
                            }
                }
            }) 
        
        query = {
            "bool": {
                "must":must,
                "must_not": [
                    {
                        "match": {
                            "type": 1 
                        }
                    },
                    {
                        "match": {
                            "phone.keyword": ""
                        }
                    }
                ],
                "filter": [
                    {
                        "script": {
                            "script": "doc['updatedAt'].value != doc['createdAt'].value"
                        }
                    }
                ]  
            }
        }

        listuser = client.search(
            index=index,
            body={  
                    "from": skip,
                    "size": pageSize,
                    "sort":[
                        { "updatedAt" : "desc" }
                    ],
                    "query": query,
                }
        )
        listuser = listuser["hits"]["hits"]
        listuserfinal = []
        for user in listuser:
            obj = user["_source"]
            listuserfinal.append(int(obj["idTimViec365"]))
        count = client.count(
            index=index,
            body={  
                    "query": query,
                }
        )
        return jsonify({
            "data":{
                "listuser":listuserfinal,
                "count":int(count['count']),
            }
        })
    except Exception as error:
        print("An exception occurred:", error)
        return jsonify({
            "error":"err"
        })

# ứng viên sửa cập nhật hồ sơ candi_update
@app.route("/candi_update_2",methods=['GET', 'POST'])
def candi_update_2():
    try:
        data = request.form
        client = Elasticsearch('http://localhost:9200')
        index="candidate"
        if( 'pageSize' in data and data['pageSize'] != '' ):
            pageSize = int(data['pageSize'])
        else:
            pageSize = 30
        
        if( 'page' in data and data['page'] != '' ):
            page = int(data['page'])
        else:
            page = 1
        skip = (page -1)*pageSize
        must = []
        must.append({
            "exists": {
                "field": "cv_user_id"
            }
        })
        # print(data)
        if('use_first_name' in data and data['use_first_name'] != ''):
            findword = remove_accents(str(data['use_first_name']))
            words = findword.split()
            for findword_child in words:
                must.append({
                    "regexp": {
                        "use_first_name": {
                            "value": ".*" + findword_child + ".*"
                        }
                    }
                })
            # must.append({
            #             "match_phrase": {
            #                 "use_first_name": findword
            #                 }
            # })
            
        if('use_phone' in data and data['use_phone'] != ''):
            findword = str(data['use_phone'])
            must.append({
                        "regexp": {
                            "use_phone": {
                                   "value": ".*"+findword+".*"
                                }
                            }
            })
        if('use_email' in data and data['use_email'] != ''):
            findword = remove_accents(str(data['use_email']))
            must.append({
                        "regexp": {
                            "use_email": {
                                   "value": ".*"+findword+".*"
                                }
                            }
            })    
        if('use_phone_tk' in data and data['use_phone_tk'] != ''):
            findword = remove_accents(str(data['use_phone_tk']))
            must.append({
                        "regexp": {
                            "use_phone_tk": {
                                   "value": ".*"+findword+".*"
                                }
                            }
            })  
             
        if( ( 'time_start' in data and data['time_start'] != '' ) and ( 'time_end' not in data or data['time_end'] == '' ) ):
            must.append({
                "range": {
                            "use_update_time": {
                                "gte": int(data['time_start'])
                            }
                }
            })
            must.append({
                "range": {
                            "use_create_time": {
                                "lte": int(data['time_start'])
                            }
                }
            })
        if( ( 'time_start' not in data or data['time_start'] == '' ) and ( 'time_end' in data and data['time_end'] != '' ) ):
            must.append({
                "range": {
                            "use_update_time": {
                                "gte": int(data['time_end'])
                            }
                }
            })
        if( 'time_start' in data and data['time_start'] != '' ) and ( 'time_end' in data and data['time_end'] != '' ):
            must.append({
                "range": {
                            "use_update_time": {
                                "gte": int(data['time_start'])
                            }
                }
            })
            must.append({
                "range": {
                            "use_update_time": {
                                "lte": int(data['time_end'])
                            }
                }
            })
            must.append({
                "range": {
                            "use_create_time": {
                                "lte": int(data['time_start'])
                            }
                }
            })
        query = {
            "bool": {
                "must":must,
                "must_not": [
                    {
                        "match": {
                            "use_phone.keyword": ""
                        }
                    }
                ],
                "filter": [
                    {
                        "script": {
                            "script": "doc['use_create_time'].value != doc['use_update_time'].value"
                        }
                    }
                ]
            }
        }

        listuser = client.search(
            index=index,
            body={  
                    "from": skip,
                    "size": pageSize,
                    "sort":[
                        { "use_update_time" : "desc" }
                    ],
                    "query": query
                }
        )
        listuser = listuser["hits"]["hits"]

        listuserfinal = []
        for user in listuser:
            obj = user["_source"]
            listuserfinal.append(int(obj["use_id"]))
        count = client.count(
            index=index,
            body={  
                    "query": query,
                }
        )
        return jsonify({
            "data":{
                "listuser":listuserfinal,
                "count":int(count['count']),
            }
        })
    except Exception as error:
        print("An exception occurred:", error)
        return jsonify({
            "error":"err"
        })
          
# ứng viên có điểm hồ sơ < 45
@app.route("/list_percents",methods=['GET', 'POST'])
def list_percents():
    try:
        data = request.form
        client = Elasticsearch('http://localhost:9200')
        index="users"
        pageSize = int(data['pageSize'])
        page = int(data['page'])
        skip = (page -1)*pageSize
        must = []
        must.append({
                "range": {
                    "inForPerson.candidate.percents": {
                        "lt": 45
                    }
                }
            })
        must.append({
                "range": {
                    "idTimViec365": {
                        "gte": 1 
                    }
                }
            })
        if( str(data['idTimViec365']) != "0"):
            must.append({
                        "match": {
                            "idTimViec365": int(data['idTimViec365'])
                            }
            })
        if( str(data['email']) != "0"):
            findword = remove_accents(str(data['email']))
            must.append({
                "regexp": {
                    "email":{
                            "value": ".*" + findword + ".*"
                        }
                }
            })
        if( str(data['phoneTK']) != "-1"):
            findword = remove_accents(str(data['phoneTK']))
            must.append({
                "regexp": {
                    "phoneTK":{
                            "value": ".*" + findword + ".*"
                        }
                }        
            })
        if( str(data['userName']) != "0"):
            findword = remove_accents(str(data['userName']))
            words = findword.split()
            for findword_child in words:
                must.append({
                    "regexp": {
                        "userName": {
                            "value": ".*" + findword_child + ".*"
                        }
                    }
                })
        if((str(data['start']) != "0") and (str(data['end']) == "0") ):
            must.append({
                "range": {
                            "createdAt": {
                                "gte": int(data['start'])
                            }
                }
            })
        if((str(data['start']) == "0") and (str(data['end']) != "0") ):
            must.append({
                "range": {
                            "createdAt": {
                                "gte": int(data['end'])
                            }
                }
            })
        if((str(data['start']) != "0") and (str(data['end']) != "0") ):
            must.append({
                "range": {
                            "createdAt": {
                                "gte": int(data['start'])
                            }
                }
            })
            must.append({
                "range": {
                            "createdAt": {
                                "lte": int(data['end'])
                            }
                }
            }) 
        if (str(data['cv_cate_id']) != "-1"):
             must.append({
                        "wildcard": {
                            "cv_cate_id": str(data['cv_cate_id'])
                            }
            })
        if (str(data['cv_city_id']) != "-1"):
            must.append({
                        "wildcard": {
                            "cv_city_id": str(data['city'])
                            }
            })
        if (str(data['cv_title']) != "0"):
            findword = remove_accents(str(data['cv_title']))
            words = findword.split()
            for findword_child in words:
                must.append({
                        "regexp": {
                            "inForPerson.candidate.cv_title": {
                                        "value": ".*" + findword_child + ".*"
                                    }
                    }
                })
        query = {
            "bool": {
                "must":must,
                "must_not": [
                    {
                        "match": {
                            "type": 1 
                        }
                    }
                ]
            }
        }
        listuser = client.search(
            index=index,
            body={  
                    "from": skip,
                    "size": pageSize,
                    "sort":[
                        { "createdAt" : "desc" },
                        { "idTimViec365": "desc"}
                    ],
                    "query": query,
                }
        )
        listuser = listuser["hits"]["hits"]
        listuserfinal = []
        for user in listuser:
            obj = user["_source"]
            listuserfinal.append(int(obj["idTimViec365"]))
        count = client.count(
            index=index,
            body={  
                    "query": query,
                }
        )
        return jsonify({
            "data":{
                "listuser":listuserfinal,
                "count":int(count['count'])
            }
        })
    except Exception as error:
        print("An exception occurred:", error)
        return jsonify({
            "error":"err"
        })

# ứng viên có điểm hồ sơ < 45
@app.route("/list_percents_2",methods=['GET', 'POST'])
def list_percents_2():
    try:
        data = request.form
        client = Elasticsearch('http://localhost:9200')
        index="candidate"
        if( 'pageSize' in data and data['pageSize'] != '' ):
            pageSize = int(data['pageSize'])
        else:
            pageSize = 30
        
        if( 'page' in data and data['page'] != '' ):
            page = int(data['page'])
        else:
            page = 1
        skip = (page -1)*pageSize
        must = []
        must.append({
            "exists": {
                "field": "cv_user_id"
            }
        })
        must.append({
                "range": {
                    "percents": {
                        "lt": 45
                    }
                }
            })
        if( 'idTimViec365' in data and data['idTimViec365'] != '' ):
            must.append({
                        "match": {
                            "use_id": int(data['idTimViec365'])
                            }
            })
        if( 'email' in data and data['email'] != '' ):
            findword = remove_accents(str(data['email']))
            must.append({
                "regexp": {
                    "use_email":{
                            "value": ".*" + findword + ".*"
                        }
                }
            })
        if( 'phoneTK' in data and data['phoneTK'] != '' ):
            findword = remove_accents(str(data['phoneTK']))
            must.append({
                "regexp": {
                    "use_phone_tk":{
                            "value": ".*" + findword + ".*"
                        }
                }        
            })
        if( 'userName' in data and data['userName'] != '' ):
            findword = remove_accents(str(data['userName']))
            words = findword.split()
            for findword_child in words:
                must.append({
                    "regexp": {
                        "use_first_name": {
                            "value": ".*" + findword_child + ".*"
                        }
                    }
                })
        if( ('start' in data and data['start'] != '' ) and ( 'end' not in data or data['end'] == '' ) ):
            must.append({
                "range": {
                            "use_create_time": {
                                "gte": int(data['start'])
                            }
                }
            })
        if( ( 'start' not in data or data['start'] == '' ) and ( 'end' in data and data['end'] != '' ) ):
            must.append({
                "range": {
                            "use_create_time": {
                                "gte": int(data['end'])
                            }
                }
            })
        if( ('start' in data and data['start'] != '' ) and ( 'end' in data and data['end'] != '' ) ):
            must.append({
                "range": {
                            "use_create_time": {
                                "gte": int(data['start'])
                            }
                }
            })
            must.append({
                "range": {
                            "use_create_time": {
                                "lte": int(data['end'])
                            }
                }
            }) 
        if ( 'cv_cate_id' in data and data['cv_cate_id'] != '' ):
            must.append({
                        "wildcard": {
                            "cv_cate_id": str(data['cv_cate_id'])
                            }
            })
        if ( 'cv_city_id' in data and data['cv_city_id'] != '' ):
            must.append({
                        "wildcard": {
                            "cv_city_id": str(data['cv_city_id'])
                            }
            })
        if ( 'cv_title' in data and data['cv_title'] != '' ):
            findword = remove_accents(str(data['cv_title']))
            words = findword.split()
            for findword_child in words:
                must.append({
                        "regexp": {
                            "cv_title": {
                                        "value": ".*" + findword_child + ".*"
                                    }
                    }
                })
        if( 'use_check' in data and data['use_check'] != '' ):
            must.append({
                        "match": {
                            "use_check": int(data['use_check'])
                            }
            })
        if( 'use_show' in data and data['use_show'] != '' ):
            must.append({
                        "match": {
                            "use_show": int(data['use_show'])
                            }
            })
        query = {
            "bool": {
                "must":must,
                "must_not": [
                    {
                        "match": {
                            "dk": 4 
                        }
                    },
                    {
                        "match": {
                            "dk": 7  
                        }
                    }
                ]
            }
        }
        listuser = client.search(
            index=index,
            body={  
                    "from": skip,
                    "size": pageSize,
                    "sort":[
                        { "use_create_time" : "desc" }
                    ],
                    "query": query,
                }
        )
        listuser = listuser["hits"]["hits"]
        listuserfinal = []
        for user in listuser:
            obj = user["_source"]
            listuserfinal.append(int(obj["use_id"]))
        count = client.count(
            index="candidate",
            body={  
                    "query": query,
                }
        )

        return jsonify({
            "data":{
                "listuser":listuserfinal,
                "count":int(count['count'])
            }
        })
    except Exception as error:
        print("An exception occurred:", error)
        return jsonify({
            "error":"err"
        })
   
# ứng viên chưa hoàn thiện hồ sơ từ APP CV
@app.route("/candiNotCompleteAppCv",methods=['GET', 'POST'])
def candiNotCompleteAppCv():
    try:
        data = request.form
        client = Elasticsearch('http://localhost:9200')
        index="users"
        pageSize = int(data['pageSize'])
        page = int(data['page'])
        skip = (page -1)*pageSize
        must = []
        must.append({
            "range": {
                    "idTimViec365": {
                        "gte": 1
                    }
                    }
        })
        must.append({
            "range": {
                    "inForPerson.candidate.percents": {
                        "lte": 45
                    }
                }
        })
        if(str(data['use_id']) != "0"):
            must.append({
                        "match": {
                            "idTimViec365": int(data['use_id'])
                            }
            })
        if(str(data['use_first_name']) != "0"):
            findword = remove_accents(str(data['use_first_name']))
            words = findword.split()
            for findword_child in words:
                must.append({
                    "regexp": {
                        "userName": {
                            "value": ".*" + findword_child + ".*"
                        }
                    }
                })
        if(str(data['use_phone']) != "-1"):
            findword = str(data['use_phone'])
            must.append({
                        "regexp": {
                            "phone": {
                                   "value": ".*"+findword+".*"
                                }
                            }
            })
        if(str(data['use_email']) != "0"):
            findword = remove_accents(str(data['use_email']))
            must.append({
                        "regexp": {
                            "email": {
                                   "value": ".*"+findword+".*"
                                }
                            }
            })    
        if(str(data['use_phone_tk']) != "-1"):
            findword = remove_accents(str(data['use_phone_tk']))
            must.append({
                        "regexp": {
                            "phoneTK": {
                                   "value": ".*"+findword+".*"
                                }
                            }
            })  
        if(str(data['use_email_lh']) != "0"):
            findword = remove_accents(str(data['use_email_lh']))
            must.append({
                        "regexp": {
                            "emailContact": {
                                   "value": ".*"+findword+".*"
                                }
                            }
            }) 
        if (str(data['cv_title']) != "0"):
            findword = remove_accents(str(data['cv_title']))
            words = findword.split()
            for findword_child in words:
                must.append({
                        "regexp": {
                            "inForPerson.candidate.cv_title": {
                                        "value": ".*" + findword_child + ".*"
                                    }
                    }
                })
        if((str(data['time_start']) != "0") and (str(data['time_end']) == "0") ):
            must.append({
                "range": {
                            "createdAt": {
                                "gte": int(data['time_start'])
                            }
                }
            })
        if((str(data['time_start']) == "0") and (str(data['time_end']) != "0") ):
            must.append({
                "range": {
                            "createdAt": {
                                "gte": int(data['time_end'])
                            }
                }
            })
        if((str(data['time_start']) != "0") and (str(data['time_end']) != "0") ):
            must.append({
                "range": {
                            "createdAt": {
                                "gte": int(data['time_start'])
                            }
                }
            })
            must.append({
                "range": {
                            "createdAt": {
                                "lte": int(data['time_end'])
                            }
                }
            }) 
        if(str(data['register']) != "-1"):
            must.append({
                        "match": {
                            "fromDevice": int(data['register'])
                            }
            })
        if(str(data['category']) != "-1"):
            must.append({
                        "match": {
                            "inForPerson.candidate.cv_cate_id": int(data['category'])
                            }
            })
        if(str(data['city']) != "-1"):
            must.append({
                        "match": {
                            "inForPerson.candidate.cv_city_id": int(data['city'])
                            }
            })
        if(str(data['authentic']) != "-1"):
            must.append({
                        "match": {
                            "authentic": int(data['authentic'])
                            }
            })
        query = {
            "bool": {
                "must":must,
                "must_not": [
                    {
                        "match": {
                            "type": 1 
                        }
                    },
                    {
                        "match": {
                            "fromDevice": 4 
                        }
                    },
                    {
                        "match": {
                            "fromDevice": 7  
                        }
                    }
                ]
            }
        }
        listuser = client.search(
            index=index,
            body={  
                    "from": skip,
                    "size": pageSize,
                    "sort":[
                        { "createdAt" : "desc" },
                        { "idTimViec365" : "desc" }
                    ],
                    "query": query,
                }
        )
        listuser = listuser["hits"]["hits"]
        listuserfinal = []
        for user in listuser:
            obj = user["_source"]
            listuserfinal.append(int(obj["idTimViec365"]))
        count = client.count(
            index=index,
            body={  
                    "query": query,
                }
        )
        return jsonify({
            "data":{
                "listuser":listuserfinal,
                "count":int(count['count']),
            }
        })
    except Exception as error:
        print("An exception occurred:", error)
        return jsonify({
            "error":"err"
        })

# # ứng viên chưa hoàn thiện hồ sơ từ APP CV
@app.route("/candiNotCompleteAppCv_2",methods=['GET', 'POST'])
def candiNotCompleteAppCv_2():
    try:
        data = request.form
        client = Elasticsearch('http://localhost:9200')
        index="candidate"
        if( 'pageSize' in data and data['pageSize'] != '' ):
            pageSize = int(data['pageSize'])
        else:
            pageSize = 30
        
        if( 'page' in data and data['page'] != '' ):
            page = int(data['page'])
        else:
            page = 1
        skip = (page -1)*pageSize
        must=[]
        must.append({
            "bool": {
                "should": [
                {
                    "match": {
                    "dk": 4
                    }
                },
                {
                    "match": {
                    "dk": 7
                    }
                }
                ]
            }
        })
        must.append({
            "exists": {
                "field": "cv_user_id"
            }
        })
        if('use_id' in data and data['use_id'] != ''):
            must.append({
                        "match": {
                            "use_id": int(data['use_id'])
                            }
            })
        if('use_first_name' in data and data['use_first_name'] != ''):
            findword = remove_accents(str(data['use_first_name']))
            words = findword.split()
            for findword_child in words:
                must.append({
                    "regexp": {
                        "use_first_name": {
                            "value": ".*" + findword_child + ".*"
                        }
                    }
                })
        if('use_phone' in data and data['use_phone'] != ''):
            findword = str(data['use_phone'])
            must.append({
                        "regexp": {
                            "use_phone": {
                                   "value": ".*"+findword+".*"
                                }
                            }
            })
        if('use_email' in data and data['use_email'] != ''):
            findword = remove_accents(str(data['use_email']))
            must.append({
                        "regexp": {
                            "use_email": {
                                   "value": ".*"+findword+".*"
                                }
                            }
            })    
        if('use_phone_tk' in data and data['use_phone_tk'] != ''):
            findword = remove_accents(str(data['use_phone_tk']))
            must.append({
                        "regexp": {
                            "use_phone_tk": {
                                   "value": ".*"+findword+".*"
                                }
                            }
            })  
        if('use_email_lh' in data and data['use_email_lh'] != ''):
            findword = remove_accents(str(data['use_email_lh']))
            must.append({
                        "regexp": {
                            "use_email_lienhe": {
                                   "value": ".*"+findword+".*"
                                }
                            }
            }) 
        if ('cv_title' in data and data['cv_title'] != ''):
            findword = remove_accents(str(data['cv_title']))
            words = findword.split()
            for findword_child in words:
                must.append({
                        "regexp": {
                            "cv_title": {
                                        "value": ".*" + findword_child + ".*"
                                    }
                    }
                })
        if(( 'time_start' in data and data['time_start'] != '' ) and ( 'time_end' not in data or data['time_end'] == '' ) ):
            must.append({
                "range": {
                            "use_create_time": {
                                "gte": int(data['time_start'])
                            }
                }
            })
        if(( 'time_start' not in data or data['time_start'] == '' ) and ( 'time_end' in data and data['time_end'] != '' ) ):
            must.append({
                "range": {
                            "use_create_time": {
                                "gte": int(data['time_end'])
                            }
                }
            })
        if( 'time_start' in data and data['time_start'] != '' ) and ( 'time_end' in data and data['time_end'] != '' ):
            must.append({
                "range": {
                            "use_create_time": {
                                "gte": int(data['time_start'])
                            }
                }
            })
            must.append({
                "range": {
                            "use_create_time": {
                                "lte": int(data['time_end'])
                            }
                }
            }) 
        if('register' in data and data['register'] != ''):
            must.append({
                        "match": {
                            "dk": int(data['register'])
                            }
            })
        if('category' in data and data['category'] != ''):
            must.append({
                        "wildcard": {
                            "cv_cate_id": str(data['category'])
                            }
            })
        if('city' in data and data['city'] != ''):
            must.append({
                        "wildcard": {
                            "cv_city_id": str(data['city'])
                            }
            })
        if('authentic' in data and data['authentic'] != ''):
            must.append({
                        "match": {
                            "use_authentic": int(data['authentic'])
                            }
            })
        query = {
                "bool": {
                    "must":must
                }
            }
        listuser = client.search(
            index=index,
            body={  
                    "from": skip,
                    "size": pageSize,
                    "sort":[
                        { "use_create_time" : "desc" }
                    ],
                    "query": query,
                }
        )
        listuser = listuser["hits"]["hits"]
        listuserfinal = []
        for user in listuser:
            obj = user["_source"]
            listuserfinal.append(int(obj["use_id"]))
        count = client.count(
            index="candidate",
            body={  
                    "query": query,
                }
        )
        return jsonify({
            "data":{
                "listuser":listuserfinal,
                "count":int(count['count'])
            }
        })
    except Exception as error:
        print("An exception occurred:", error)
        return jsonify({
            "error":"err"
        })
       
# Tất cả ứng viên
@app.route("/candi_all",methods=['GET', 'POST'])
def candi_all():
    try:
        data = request.form
        client = Elasticsearch('http://localhost:9200')
        index="users"
        pageSize = int(data['pageSize'])
        page = int(data['page'])
        skip = (page -1)*pageSize
        must = []
        must.append({
            "range": {
                            "idTimViec365": {
                                "gte": 1
                            }
                    }
        })
        
        if(str(data['use_id']) != "0"):
            must.append({
                        "match": {
                            "idTimViec365": int(data['use_id'])
                            }
            })
        if(str(data['use_first_name']) != "0"):
            findword = remove_accents(str(data['use_first_name']))
            words = findword.split()
            for findword_child in words:
                must.append({
                    "regexp": {
                        "userName": {
                            "value": ".*" + findword_child + ".*"
                        }
                    }
                })
        if(str(data['use_phone']) != "-1"):
            findword = str(data['use_phone'])
            must.append({
                        "regexp": {
                            "phone": {
                                   "value": ".*"+findword+".*"
                                }
                            }
            })
        if(str(data['use_address']) != "0"):
            findword = remove_accents(str(data['use_address']))
            words = findword.split()
            for findword_child in words:
                must.append({
                    "regexp": {
                        "address": {
                            "value": ".*" + findword_child + ".*"
                        }
                    }
                })
        if(str(data['use_email']) != "0"):
            findword = remove_accents(str(data['use_email']))
            must.append({
                        "regexp": {
                            "email": {
                                   "value": ".*"+findword+".*"
                                }
                            }
            })    
        if(str(data['use_phone_tk']) != "-1"):
            findword = remove_accents(str(data['use_phone_tk']))
            must.append({
                        "regexp": {
                            "phoneTK": {
                                   "value": ".*"+findword+".*"
                                }
                            }
            })  
        if(str(data['use_email_lh']) != "0"):
            findword = remove_accents(str(data['use_email_lh']))
            must.append({
                        "regexp": {
                            "emailContact": {
                                   "value": ".*"+findword+".*"
                                }
                            }
            }) 
        if (str(data['cv_title']) != "0"):
            findword = remove_accents(str(data['cv_title']))
            words = findword.split()
            for findword_child in words:
                must.append({
                        "regexp": {
                            "inForPerson.candidate.cv_title": {
                                        "value": ".*" + findword_child + ".*"
                                    }
                    }
                })
        if((str(data['time_start']) != "0") and (str(data['time_end']) == "0") ):
            must.append({
                "range": {
                            "updatedAt": {
                                "gte": int(data['time_start'])
                            }
                }
            })
        if((str(data['time_start']) == "0") and (str(data['time_end']) != "0") ):
            must.append({
                "range": {
                            "updatedAt": {
                                "gte": int(data['time_end'])
                            }
                }
            })
        if((str(data['time_start']) != "0") and (str(data['time_end']) != "0") ):
            must.append({
                "range": {
                            "updatedAt": {
                                "gte": int(data['time_start'])
                            }
                }
            })
            must.append({
                "range": {
                            "updatedAt": {
                                "lte": int(data['time_end'])
                            }
                }
            }) 
        if(str(data['register']) != "-1"):
            must.append({
                        "match": {
                            "fromDevice": int(data['register'])
                            }
            })
        if(str(data['category']) != "-1"):
            must.append({
                        "match": {
                            "inForPerson.candidate.cv_cate_id": int(data['category'])
                            }
            })
        if(str(data['city']) != "-1"):
            must.append({
                        "match": {
                            "inForPerson.candidate.cv_city_id": int(data['city'])
                            }
            })
        if(str(data['authentic']) != "-1"):
            must.append({
                        "match": {
                            "authentic": int(data['authentic'])
                            }
            })
        
        query = {
            "bool": {
                "must":must,
                "must_not": [
                    {
                        "match": {
                            "type": 1 
                        }
                    },
                    {
                        "match": {
                            "phone.keyword": ""
                        }
                    }
                ]
            }
        }
        listuser = client.search(
            index=index,
            body={  
                    "from": skip,
                    "size": pageSize,
                    "sort":[
                        { "updatedAt" : "desc" },
                        { "idTimViec365": "desc" }
                    ],
                    "query": query,
                }
        )
        listuser = listuser["hits"]["hits"]
        listuserfinal = []
        for user in listuser:
            obj = user["_source"]
            listuserfinal.append(int(obj["idTimViec365"]))
        count = client.count(
            index=index,
            body={  
                    "query": query,
                }
        )
        return jsonify({
            "data":{
                "listuser":listuserfinal,
                "count":int(count['count'])
            }
        })
    except Exception as error:
        print("An exception occurred:", error)
        return jsonify({
            "error":"err"
        })

# Tất cả ứng viên
@app.route("/candi_all_2",methods=['GET', 'POST'])
def candi_all_2():
    try:
        data = request.form
        client = Elasticsearch('http://localhost:9200')
        index="candidate"
        if( 'pageSize' in data and data['pageSize'] != '' ):
            pageSize = int(data['pageSize'])
        else:
            pageSize = 30
        
        if( 'page' in data and data['page'] != '' ):
            page = int(data['page'])
        else:
            page = 1
        skip = (page -1)*pageSize
        must = []
        must.append({
          "exists": {
            "field": "use_phone"
          }
        })
        
        must.append({
            "exists": {
                "field": "cv_user_id"
            }
        })
        
        if('use_id' in data and data['use_id'] != ''):
            must.append({
                        "match": {
                            "use_id": int(data['use_id'])
                            }
            })
        if('use_first_name' in data and data['use_first_name'] != ''):
            findword = remove_accents(str(data['use_first_name']))
            words = findword.split()
            for findword_child in words:
                must.append({
                    "regexp": {
                        "use_first_name": {
                            "value": ".*" + findword_child + ".*"
                        }
                    }
                })
        if('use_phone' in data and data['use_phone'] != ''):
            findword = str(data['use_phone'])
            must.append({
                        "regexp": {
                            "use_phone": {
                                   "value": ".*"+findword+".*"
                                }
                            }
            })
        if('use_address' in data and data['use_address'] != ''):
            findword = remove_accents(str(data['use_address']))
            words = findword.split()
            for findword_child in words:
                must.append({
                    "regexp": {
                        "use_address": {
                            "value": ".*" + findword_child + ".*"
                        }
                    }
                })
        if('use_email' in data and data['use_email'] != ''):
            findword = remove_accents(str(data['use_email']))
            must.append({
                        "regexp": {
                            "use_email": {
                                   "value": ".*"+findword+".*"
                                }
                            }
            })    
        if('use_phone_tk' in data and data['use_phone_tk'] != ''):
            findword = remove_accents(str(data['use_phone_tk']))
            must.append({
                        "regexp": {
                            "use_phone_tk": {
                                   "value": ".*"+findword+".*"
                                }
                            }
            })  
        if('use_email_lh' in data and data['use_email_lh'] != ''):
            findword = remove_accents(str(data['use_email_lh']))
            must.append({
                        "regexp": {
                            "use_email_lienhe": {
                                   "value": ".*"+findword+".*"
                                }
                            }
            }) 
        if ('cv_title' in data and data['cv_title'] != ''):
            findword = remove_accents(str(data['cv_title']))
            words = findword.split()
            for findword_child in words:
                must.append({
                        "regexp": {
                            "cv_title": {
                                        "value": ".*" + findword_child + ".*"
                                    }
                    }
                })
        if(( 'time_start' in data and data['time_start'] != '' ) and ( 'time_end' not in data or data['time_end'] == '' ) ):
            must.append({
                "range": {
                            "use_update_time": {
                                "gte": int(data['time_start'])
                            }
                }
            })
        if(( 'time_start' not in data or data['time_start'] == '' ) and ( 'time_end' in data and data['time_end'] != '' ) ):
            must.append({
                "range": {
                            "use_update_time": {
                                "gte": int(data['time_end'])
                            }
                }
            })
        if( 'time_start' in data and data['time_start'] != '' ) and ( 'time_end' in data and data['time_end'] != '' ):
            must.append({
                "range": {
                            "use_update_time": {
                                "gte": int(data['time_start'])
                            }
                }
            })
            must.append({
                "range": {
                            "use_update_time": {
                                "lte": int(data['time_end'])
                            }
                }
            }) 
        if('register' in data and data['register'] != ''):
            must.append({
                        "match": {
                            "dk": int(data['register'])
                            }
            })
        if('category' in data and data['category'] != ''):
            must.append({
                        "wildcard": {
                            "cv_cate_id": str(data['category'])
                            }
            })
        if('city' in data and data['city'] != ''):
            must.append({
                        "wildcard": {
                            "cv_city_id": str(data['city'])
                            }
            })
        # if('cv_city_id' in data and data['cv_city_id'] != ''):
        #     must.append({
        #                 "wildcard": {
        #                     "cv_city_id": str(data['city'])
        #                     }
        #     })
        if('authentic' in data and data['authentic'] != ''):
            must.append({
                        "match": {
                            "use_authentic": int(data['authentic'])
                            }
            })

        if(len(must) == 0):
            query = {
                "bool": {
                    "must_not": [
                        {
                            "match": {
                                "use_phone.keyword": "" 
                            }
                        }
                    ]
                }
            }
        else:
            query = {
                "bool": {
                    "must":must,
                    "must_not": [
                        {
                            "match": {
                                "use_phone.keyword": "" 
                            }
                        }
                    ]
                }
            }

        listuser = client.search(
            index="candidate",
            body={  
                    "from": skip,
                    "size": pageSize,
                    "sort":[
                        { "use_update_time" : "desc" }
                    ],
                    "query": query,
                }
        )
        # print(query)
        # print(page)
        # print(pageSize)
        listuser = listuser["hits"]["hits"]
        listuserfinal = []
        for user in listuser:
            obj = user["_source"]
            listuserfinal.append(int(obj["use_id"]))
        count = client.count(
            index="candidate",
            body={  
                    "query": query,
                }
        )
        return jsonify({
            "data":{
                "listuser":listuserfinal,
                "count":int(count['count']),
                "query": query
            }
        })
    except Exception as error:
        print("An exception occurred:", error)
        return jsonify({
            "error":"err"
        })

# ứng viên chưa kích hoạt
@app.route("/listAuthentic",methods=['GET', 'POST'])
def listAuthentic():
    try:
        data = request.form
        client = Elasticsearch('http://localhost:9200')
        index="users"
        pageSize = int(data['pageSize'])
        page = int(data['page'])
        skip = (page -1)*pageSize
        must = []
        must.append({
            "range": {
                    "idTimViec365": {
                        "gte": 1
                    }
                }
            })
        must.append({
            "match": {
                    "authentic": 0
                }
            })
        if(str(data['use_id']) != "0"):
            must.append({
                        "match": {
                            "idTimViec365": int(data['use_id'])
                            }
            })
        if(str(data['use_first_name']) != "0"):
            findword = remove_accents(str(data['use_first_name']))
            words = findword.split()
            for findword_child in words:
                must.append({
                    "regexp": {
                        "userName": {
                            "value": ".*" + findword_child + ".*"
                        }
                    }
                })
        if(str(data['use_phone']) != "-1"):
            findword = str(data['use_phone'])
            must.append({
                        "regexp": {
                            "phone": {
                                   "value": ".*"+findword+".*"
                                }
                            }
            })
        if(str(data['use_email']) != "0"):
            findword = remove_accents(str(data['use_email']))
            must.append({
                        "regexp": {
                            "email": {
                                   "value": ".*"+findword+".*"
                                }
                            }
            })    
        
        if((str(data['time_start']) != "0") and (str(data['time_end']) == "0") ):
            must.append({
                "range": {
                            "createdAt": {
                                "gte": int(data['time_start'])
                            }
                }
            })
        if((str(data['time_start']) == "0") and (str(data['time_end']) != "0") ):
            must.append({
                "range": {
                            "createdAt": {
                                "gte": int(data['time_end'])
                            }
                }
            })
        if((str(data['time_start']) != "0") and (str(data['time_end']) != "0") ):
            must.append({
                "range": {
                            "createdAt": {
                                "gte": int(data['time_start'])
                            }
                }
            })
            must.append({
                "range": {
                            "createdAt": {
                                "lte": int(data['time_end'])
                            }
                }
            }) 
        
        if(str(data['uv_time']) == "0"):
            MonthsAgo = time.time() - ( 30 * 3 * 86400 ) 
            must.append({
                "range": {
                            "updatedAt": {
                                "gte": int(MonthsAgo)
                            }
                }
            })
        if(str(data['uv_time']) == "1"):
            MonthsAgo = time.time() - ( 30 * 6 * 86400 ) 
            must.append({
                "range": {
                            "updatedAt": {
                                "gte": int(MonthsAgo)
                            }
                }
            })
        if(str(data['uv_time']) == "2"):
            MonthsAgo = time.time() - ( 30 * 12 * 86400 ) 
            must.append({
                "range": {
                            "updatedAt": {
                                "gte": int(MonthsAgo)
                            }
                }
            })
        if(str(data['uv_time']) == "3"):
            MonthsAgo = time.time() - ( 30 * 24 * 86400 ) 
            must.append({
                "range": {
                            "updatedAt": {
                                "gte": int(MonthsAgo)
                            }
                }
            })
        query = {
            "bool": {
                "must":must,
                "must_not": [
                    {
                        "match": {
                            "type": 1 
                        }
                    }
                ]
            }
        }
        listuser = client.search(
            index=index,
            body={  
                    "from": skip,
                    "size": pageSize,
                    "sort":[
                        { "createdAt" : "desc" }
                    ],
                    "query": query,
                }
        )
        listuser = listuser["hits"]["hits"]
        listuserfinal = []
        for user in listuser:
            obj = user["_source"]
            listuserfinal.append(int(obj["idTimViec365"]))
        count = client.count(
            index=index,
            body={  
                    "query": query,
                }
        )
        return jsonify({
            "data":{
                "listuser": listuserfinal,
                "count": int(count['count'])
            }
        })
    except Exception as error:
        print("An exception occurred:", error)
        return jsonify({
            "error":"err"
        })

# # ứng viên chưa kích hoạt
@app.route("/listAuthentic_2",methods=['GET', 'POST'])
def listAuthentic_2():
    try:
        data = request.form
        client = Elasticsearch('http://localhost:9200')
        index="candidate"
        if( 'pageSize' in data and data['pageSize'] != '' ):
            pageSize = int(data['pageSize'])
        else:
            pageSize = 30
        
        if( 'page' in data and data['page'] != '' ):
            page = int(data['page'])
        else:
            page = 1
        skip = (page -1)*pageSize
        must = []
        must.append({
            "match": {
                    "use_authentic": 0
                }
            })
        must.append({
            "exists": {
                "field": "cv_user_id"
            }
        })
        if( 'use_id' in data and data['use_id'] != '' ):
            must.append({
                        "match": {
                            "use_id":int(data['use_id'])
                            }
            })
        if('use_first_name' in data and data['use_first_name'] != ''):
            array = str(data['use_first_name']).split()
            for ele in array:
                findword = remove_accents(str(ele))
                must.append({
                            "regexp": {
                                "use_first_name": {
                                    "value": ".*"+findword+".*"
                                    }
                                }
                })
        if('use_phone' in data and data['use_phone'] != ''):
            findword = str(data['use_phone'])
            must.append({
                        "regexp": {
                            "use_phone": {
                                   "value": ".*"+findword+".*"
                                }
                            }
            })
        if('use_email' in data and data['use_email'] != ''):
            findword = remove_accents(str(data['use_email']))
            must.append({
                        "regexp": {
                            "use_email": {
                                   "value": ".*"+findword+".*"
                                }
                            }
            })  
        if(str(data['uv_time']) == "0"):
            MonthsAgo = time.time() - 8035200
            must.append({
                "range": {
                            "use_update_time": {
                                "gte": int(MonthsAgo)
                            }
                }
            })
        if(str(data['uv_time']) == "1"):
            MonthsAgo = time.time() - 16070400 
            must.append({
                "range": {
                            "use_update_time": {
                                "gte": int(MonthsAgo)
                            }
                }
            })
        if(str(data['uv_time']) == "2"):
            MonthsAgo = time.time() - ( 30 * 12 * 86400 ) 
            must.append({
                "range": {
                            "use_update_time": {
                                "gte": int(MonthsAgo)
                            }
                }
            })
        if(str(data['uv_time']) == "3"):
            MonthsAgo = time.time() - ( 30 * 24 * 86400 ) 
            must.append({
                "range": {
                            "use_update_time": {
                                "gte": int(MonthsAgo)
                            }
                }
            })
        
        if( ('time_start' in data and data['time_start'] != '' ) and ( 'time_end' not in data or data['time_end'] == '' ) ):
            must.append({
                "range": {
                            "use_create_time": {
                                "gte": int(data['time_start'])
                            }
                }
            })
        if( ( 'time_start' not in data or data['time_start'] == '' ) and ( 'time_end' in data and data['time_end'] != '' ) ):
            must.append({
                "range": {
                            "use_create_time": {
                                "gte": int(data['time_end'])
                            }
                }
            })
        if( ( 'time_start' in data and data['time_start'] != '' ) and ( 'time_end' in data and data['time_end'] != '' ) ):
            must.append({
                "range": {
                            "use_create_time": {
                                "gte": int(data['time_start'])
                            }
                }
            })
            must.append({
                "range": {
                            "use_create_time": {
                                "lte": int(data['time_end'])
                            }
                }
            })
        
        query = {
            "bool": {
                "must":must
            }
        }
        # print(query)
        listuser = client.search(
            index=index,
            body={  
                    "from": skip,
                    "size": pageSize,
                    "sort":[
                        { "use_create_time" : "desc" }
                    ],
                    "query": query,
                }
        )
        listuser = listuser["hits"]["hits"]
        #    use_first_name = remove_accents(str(data['use_first_name']).lower())
        listuserfinal = []
        for user in listuser:
            obj = user["_source"]
            listuserfinal.append(int(obj["use_id"]))
        count = client.count(
            index="candidate",
            body={  
                    "query": query,
                }
        )
        return jsonify({
            "data":{
                "listuser":listuserfinal,
                "count":int(count['count'])
            }
        })
    except Exception as error:
        print("An exception occurred:", error)
        return jsonify({
            "error":"err"
        })

# ứng viên đăng nhập
@app.route("/candi_login",methods=['GET', 'POST'])
def candi_login():
    try:
        data = request.form
        client = Elasticsearch('http://localhost:9200')
        index="users"
        pageSize = int(data['pageSize'])
        page = int(data['page'])
        skip = (page -1)*pageSize
        must = []
        now = int(time.time()) - 24 * 3600
        must.append({
            "range": {
                    "idTimViec365": {
                        "gte": 1
                    }
                }
            })
        must.append({
            "range": {
                    "updatedAt": {
                        "gte": now
                    }
                }
            })
        if(str(data['idTimViec365']) != "0"):
            must.append({
                        "match": {
                            "idTimViec365": int(data['idTimViec365'])
                            }
            })
        if(str(data['userName']) != "0"):
            findword = remove_accents(str(data['userName']))
            words = findword.split()
            for findword_child in words:
                must.append({
                    "regexp": {
                        "userName": {
                            "value": ".*" + findword_child + ".*"
                        }
                    }
                })
        if(str(data['phoneTK']) != "-1"):
            findword = str(data['phoneTK'])
            must.append({
                        "regexp": {
                            "phoneTK": {
                                   "value": ".*"+findword+".*"
                                }
                            }
            })
        if(str(data['email']) != "0"):
            findword = remove_accents(str(data['email']))
            must.append({
                        "regexp": {
                            "email": {
                                   "value": ".*"+findword+".*"
                                }
                            }
            })    
        if (str(data['phone']) != "-1"):
            findword = remove_accents(str(data['phone']))
            must.append({
                        "regexp": {
                            "phone": {
                                   "value": ".*"+findword+".*"
                                }
                            }
            })   
        if (str(data['cv_cate_id']) != "-1"):
            must.append({
                        "wildcard": {
                            "cv_cate_id": str(data['cv_cate_id'])
                            }
            })
        if (str(data['cv_city_id']) != "-1"):
            must.append({
                        "wildcard": {
                            "cv_city_id": str(data['cv_city_id'])
                            }
            })
        if (str(data['cv_title']) != "0"):
            array = str(data['cv_title']).split()
            for ele in array:
                findword = remove_accents(str(ele))
                must.append({
                            "regexp": {
                                "inForPerson.candidate.cv_title": {
                                    "value": ".*"+findword+".*"
                                    }
                                }
                })
        query = {
            "bool": {
                "must":must,
                "must_not": [
                    {
                        "match": {
                            "type": 1 
                        }
                    }
                ]
            }
        }
        listuser = client.search(
            index=index,
            body={  
                    "from": skip,
                    "size": pageSize,
                    "sort":[
                        { "updatedAt" : "desc" }
                    ],
                    "query": query,
                }
        )
        listuser = listuser["hits"]["hits"]
        listuserfinal = []
        for user in listuser:
            obj = user["_source"]
            listuserfinal.append(int(obj["idTimViec365"]))
        count = client.count(
            index=index,
            body={  
                    "query": query,
                }
        )
        return jsonify({
            "data":{
                "listuser":listuserfinal,
                "count":int(count['count']),
            }
        })
    except Exception as error:
        print("An exception occurred:", error)
        return jsonify({
            "error":"err"
        })

# # ứng viên đăng nhập
@app.route("/candi_login_2",methods=['GET', 'POST'])
def candi_login_2():
    try:
        data = request.form
        client = Elasticsearch('http://localhost:9200')
        index="candidate"
        if( 'pageSize' in data and data['pageSize'] != '' ):
            pageSize = int(data['pageSize'])
        else:
            pageSize = 30
        
        if( 'page' in data and data['page'] != '' ):
            page = int(data['page'])
        else:
            page = 1
        skip = (page -1)*pageSize
        must = []
        now = int(time.time()) - 24 * 3600
        must.append({
            "exists": {
                "field": "cv_user_id"
            }
        })
        must.append({
            "range": {
                    "use_create_time": {
                        "gte": now
                    }
                }
            })
        if( 'idTimViec365' in data and data['idTimViec365'] != '' ):
            must.append({
                        "match": {
                            "use_id": int(data['idTimViec365'])
                            }
            })
        if( 'userName' in data and data['userName'] != '' ):
            findword = remove_accents(str(data['userName']))
            words = findword.split()
            for findword_child in words:
                must.append({
                    "regexp": {
                        "use_first_name": {
                            "value": ".*" + findword_child + ".*"
                        }
                    }
                })
        if( 'phoneTK' in data and data['phoneTK'] != '' ):
            findword = str(data['phoneTK'])
            must.append({
                        "regexp": {
                            "use_phone_tk": {
                                   "value": ".*"+findword+".*"
                                }
                            }
            })
        if( 'email' in data and data['email'] != '' ):
            findword = remove_accents(str(data['email']))
            must.append({
                        "regexp": {
                            "use_email": {
                                   "value": ".*"+findword+".*"
                                }
                            }
            })    
        if ( 'phone' in data and data['phone'] != '' ):
            findword = remove_accents(str(data['phone']))
            must.append({
                        "regexp": {
                            "use_phone": {
                                   "value": ".*"+findword+".*"
                                }
                            }
            })          
        if('cv_title' in data and data['cv_title'] != ''):
            array = str(data['cv_title']).split()
            for ele in array:
                findword = remove_accents(str(ele))
                must.append({
                            "regexp": {
                                "cv_title": {
                                    "value": ".*"+findword+".*"
                                    }
                                }
                })
        if('cv_cate_id' in data and data['cv_cate_id'] != ''):
            must.append({
                        "wildcard": {
                            "cv_cate_id": str(data['cv_cate_id'])
                            }
            })
        if('cv_city_id' in data and data['cv_city_id'] != ''):
            must.append({
                        "wildcard": {
                            "cv_city_id": str(data['cv_city_id'])
                            }
            })
        query = {
            "bool": {
                "must":must
            }
        }

        listuser = client.search(
            index=index,
            body={  
                    "from": skip,
                    "size": pageSize,
                    "sort":[
                        { "use_update_time" : "desc" }
                    ],
                    "query": query,
                }
        )
        listuser = listuser["hits"]["hits"]

        listuserfinal = []
        for user in listuser:
            obj = user["_source"]
            listuserfinal.append(int(obj["use_id"]))
        count = client.count(
            index="candidate",
            body={  
                    "query": query,
                }
        )
        return jsonify({
            "data":{
                "listuser":listuserfinal,
                "count":int(count['count'])
            }
        })
    except Exception as error:
        print("An exception occurred:", error)
        return jsonify({
            "error":"err"
        })

# nhà tuyển dụng 
@app.route("/getListHideNTD",methods=['GET', 'POST'])
def getListHideNTD():
    try:
        data = request.form
        client = Elasticsearch('http://localhost:9200')
        index="users"
        pageSize = int(data['pageSize'])
        page = int(data['page'])
        skip = (page -1)*pageSize
        must = []
        should = []
        should.append({
            "match":
                {
                    "email.keyword" : ""
                }
        })
        should.append({
            "match":
                {
                    "phoneTK.keyword" : ""
                }
        })
        must.append({
                "match": {
                    "type": 1 
                }
            })
        if(str(data['_id']) != "0"):
            must.append({
                        "match": {
                            "idTimViec365": int(data['_id'])
                            }
            })
        if(str(data['name']) != "0"):
            findword = remove_accents(str(data['name']))
            words = findword.split()
            for findword_child in words:
                must.append({
                    "regexp": {
                        "userName": {
                            "value": ".*" + findword_child + ".*"
                        }
                    }
                })
        if(str(data['phone']) != "-1"):
            findword = str(data['phone'])
            must.append({
                        "regexp": {
                            "phone": {
                                   "value": ".*"+findword+".*"
                                }
                            }
            })
        if(str(data['email']) != "0"):
            findword = remove_accents(str(data['email']))
            must.append({
                        "regexp": {
                            "email": {
                                   "value": ".*"+findword+".*"
                                }
                            }
            })    
        if((str(data['fromDate']) != "0") and (str(data['toDate']) == "0") ):
            must.append({
                "range": {
                            "createdAt": {
                                "gte": int(data['fromDate'])
                            }
                }
            })
        if((str(data['fromDate']) == "0") and (str(data['toDate']) != "0") ):
            must.append({
                "range": {
                            "createdAt": {
                                "lte": int(data['toDate'])
                            }
                }
            })
        if((str(data['fromDate']) != "0") and (str(data['toDate']) != "0") ):
            must.append({
                "range": {
                            "createdAt": {
                                "gte": int(data['fromDate'])
                            }
                }
            })
            must.append({
                "range": {
                            "createdAt": {
                                "lte": int(data['toDate'])
                            }
                }
            }) 
        
        query = {
            "bool": {
                "should": should,
                "must": must
            }
        }

        
        listuser = client.search(
            index=index,
            body={  
                    "from": skip,
                    "size": pageSize,
                    "sort":[
                        { "createdAt" : "desc" }
                    ],
                    "query": query,
                }
        )
        
        listuser = listuser["hits"]["hits"]
        listuserfinal = []
        for user in listuser:
            obj = user["_source"]
            listuserfinal.append(int(obj["idTimViec365"]))
        count = client.count(
            index=index,
            body={  
                    "query": query,
                }
        )
        return jsonify({
            "data":{
                "listuser":listuserfinal,
                "count":int(count['count']),
            }
        })
    except Exception as error:
        print("An exception occurred:", error)
        return jsonify({
            "error":"err"
        })

@app.route("/getListHideNTD_2",methods=['GET', 'POST'])
def getListHideNTD_2():
    try:
        data = request.form
        client = Elasticsearch('http://localhost:9200')
        index="company"
        if( 'pageSize' in data and data['pageSize'] != '' ):
            pageSize = int(data['pageSize'])
        else:
            pageSize = 30
        
        if( 'page' in data and data['page'] != '' ):
            page = int(data['page'])
        else:
            page = 1
        skip = (page -1)*pageSize
        must = []
        must.append(
            {
            "bool": {
                "should": [
                        {
                            "match": {
                            "usc_email.keyword": ""
                            }
                        },
                        {
                            "match": {
                            "usc_phone.keyword": ""
                            }
                        }
                    ] 
                }
            }
        )

        if( 'idTimViec365' in data and data['idTimViec365'] != '' ):
            must.append({
                        "match": {
                            "usc_id": int(data['idTimViec365'])
                            }
            })
        if( 'name' in data and data['name'] != ''):
            findword = remove_accents(str(data['name']))
            words = findword.split()
            for findword_child in words:
                must.append({
                    "regexp": {
                        "usc_company": {
                            "value": ".*" + findword_child + ".*"
                        }
                    }
                })
            # must.append({
            #             "match_phrase": {
            #                 "usc_company": findword
            #                 }
            # })
            
        if( 'phone' in data and data['phone'] != ''):
            findword = str(data['phone'])
            must.append({
                        "regexp": {
                            "usc_phone": {
                                   "value": ".*"+findword+".*"
                                }
                            }
            })
        if( 'email' in data and data['email'] != ''):
            findword = remove_accents(str(data['email']))
            must.append({
                        "regexp": {
                            "usc_email": {
                                   "value": ".*"+findword+".*"
                                }
                            }
            })  
            # must.append({
            #             "match_phrase": {
            #                 "usc_email": findword
            #                 }
            # })
        if( ( 'fromDate' in data and data['fromDate'] != '' ) and ( 'toDate' not in data or data['toDate'] == '' ) ):
            must.append({
                "range": {
                            "usc_create_time": {
                                "gte": int(data['fromDate'])
                            }
                }
            })
        
        if( ( 'fromDate' not in data or data['fromDate'] == '' ) and ( 'toDate' in data and data['toDate'] != '' ) ):
            must.append({
                "range": {
                            "usc_create_time": {
                                "lte": int(data['toDate'])
                            }
                }
            })
        if( ( 'fromDate' in data and data['fromDate'] != '' ) and ( 'toDate' in data and data['toDate'] != '' ) ):
            must.append({
                "range": {
                            "usc_create_time": {
                                "gte": int(data['fromDate'])
                            }
                }
            })
            must.append({
                "range": {
                            "usc_create_time": {
                                "lte": int(data['toDate'])
                            }
                }
            }) 
        
        query = {
            "bool": {
                "must": must
            }
        }
        
        listuser = client.search(
            index=index,
            body={  
                    "from": skip,
                    "size": pageSize,
                    "sort":[
                        { "usc_id" : "desc" }
                    ],
                    "query": query,
                }
        )
        
        listuser = listuser["hits"]["hits"]
        listuserfinal = []
        for user in listuser:
            obj = user["_source"]
            listuserfinal.append(int(obj["usc_id"]))
        count = client.count(
            index=index,
            body={  
                    "query": query,
                }
        )
        return jsonify({
            "data":{
                "listuser":listuserfinal,
                "count":int(count['count']),
            }
        })
    except Exception as error:
        print("An exception occurred:", error)
        return jsonify({
            "error":"err"
        })

@app.route("/checkProfile", methods=['GET', 'POST'])
def checkProfile():
    try:
        data = request.form
        client = Elasticsearch('http://localhost:9200')
        index="profile_users"
        pageSize = int(data['pageSize'])
        page = int(data['page'])
        skip = (page -1)*pageSize
        must = []
        
        must.append({
            "range": {
                    "idTimViec365": {
                        "gte": 1
                    }
                }
            })
        if (str(data['use_check']) == "1"):
            must.append({
                    "match": {
                        "use_check": 1
                }
            })
        else:
            must.append({
                    "match": {
                        "use_check": 0
                }
            }) 
            
            
        if(str(data['use_first_name']) != "0"):
            findword = remove_accents(str(data['use_first_name']))
            words = findword.split()
            for findword_child in words:
                must.append({
                    "regexp": {
                        "userName": {
                            "value": ".*" + findword_child + ".*"
                        }
                    }
                })
            
        if(str(data['use_phone']) != "-1"):
            findword = str(data['use_phone'])
            must.append({
                        "regexp": {
                            "phone": {
                                   "value": ".*"+findword+".*"
                                }
                            }
            })
        if(str(data['use_email']) != "0"):
            findword = remove_accents(str(data['use_email']))
            must.append({
                        "regexp": {
                            "email": {
                                   "value": ".*"+findword+".*"
                                }
                            }
            })    
        if((str(data['startdate']) != "0") and (str(data['enddate']) == "0") ):
            must.append({
                "range": {
                            "createdAt": {
                                "gte": int(data['startdate'])
                            }
                }
            })
        if((str(data['startdate']) == "0") and (str(data['enddate']) != "0") ):
            must.append({
                "range": {
                            "createdAt": {
                                "lte": int(data['enddate'])
                            }
                }
            })
        if((str(data['startdate']) != "0") and (str(data['enddate']) != "0") ):
            must.append({
                "range": {
                            "createdAt": {
                                "gte": int(data['startdate'])
                            }
                }
            })
            must.append({
                "range": {
                            "createdAt": {
                                "lte": int(data['enddate'])
                            }
                }
            }) 
        
        query = {
            "bool": {
                "must": must,
                "must_not": [
                    {
                        "match": {
                            "type": 1 
                        }
                    }
                ]
            }
        }
        
        listuser = client.search(
            index=index,
            body={  
                    "from": skip,
                    "size": pageSize,
                    "sort":[
                        { "hs_create_time" : "desc" }
                    ],
                    "query": query,
                }
        )
        
        listuser = listuser["hits"]["hits"]
        listuserfinal = []
        for user in listuser:
            obj = user["_source"]
            listuserfinal.append(int(obj["idTimViec365"]))
        count = client.count(
            index=index,
            body={  
                    "query": query,
                }
        )
        return jsonify({
            "data":{
                "listuser":listuserfinal,
                "count":int(count['count']),
            }
        })
    except Exception as error:
        print("An exception occurred:", error)
        return jsonify({
            "error":"err"
        })

@app.route("/listCvUvHide", methods=['GET', 'POST'])
def listCvUvHide():
    try:
        data = request.form
        client = Elasticsearch('http://localhost:9200')
        index="profile_users"
        pageSize = int(data['pageSize'])
        page = int(data['page'])
        skip = (page -1)*pageSize
        must = []
        
        must.append({
            "match": {
                        "is_scan": 1
                    }
            })
        must.append({
            "range": {
                    "idTimViec365": {
                        "gte": 1
                    }
                }
            })
        must.append({
            "exists": {
                    "field": "hs_link_hide"
                }
            })
        
        if (str(data['hs_user_id']) != "0"):
            must.append({
                    "match": {
                        "hs_user_id": int(data['hs_user_id'])
                }
            })
        
        if(str(data['use_first_name']) != "0"):
            findword = remove_accents(str(data['use_first_name']))
            words = findword.split()
            for findword_child in words:
                must.append({
                    "regexp": {
                        "userName": {
                            "value": ".*" + findword_child + ".*"
                        }
                    }
                })
            
        if(str(data['use_address']) != "0"):
            findword = str(data['use_address'])
            must.append({
                        "regexp": {
                            "address": {
                                   "value": ".*"+findword+".*"
                                }
                            }
            })
        if(str(data['use_email']) != "0"):
            findword = remove_accents(str(data['use_email']))
            must.append({
                        "regexp": {
                            "email": {
                                   "value": ".*"+findword+".*"
                                }
                            }
            })    
        if((str(data['startdate']) != "0") and (str(data['enddate']) == "0") ):
            must.append({
                "range": {
                            "hs_create_time": {
                                "gte": int(data['startdate'])
                            }
                }
            })
        if((str(data['startdate']) == "0") and (str(data['enddate']) != "0") ):
            must.append({
                "range": {
                            "hs_create_time": {
                                "lte": int(data['enddate'])
                            }
                }
            })
        if((str(data['startdate']) != "0") and (str(data['enddate']) != "0") ):
            must.append({
                "range": {
                            "hs_create_time": {
                                "gte": int(data['startdate'])
                            }
                }
            })
            must.append({
                "range": {
                            "hs_create_time": {
                                "lte": int(data['enddate'])
                            }
                }
            }) 
        
        query = {
            "bool": {
                "must": must,
                "must_not": [
                    {
                        "match": {
                            "type": 1 
                        }
                    },
                    {
                        "match": {
                            "hs_link_hide": ""
                        }
                    }
                ]
            }
        }
        
        listuser = client.search(
            index=index,
            body={  
                    "from": skip,
                    "size": pageSize,
                    "sort":[
                        { "hs_create_time" : "desc" }
                    ],
                    "query": query,
                }
        )
        
        listuser = listuser["hits"]["hits"]
        listuserfinal = []
        for user in listuser:
            obj = user["_source"]
            listuserfinal.append(int(obj["idTimViec365"]))
        count = client.count(
            index=index,
            body={  
                    "query": query,
                }
        )
        return jsonify({
            "data":{
                "listuser":listuserfinal,
                "count":int(count['count']),
            }
        })
    except Exception as error:
        print("An exception occurred:", error)
        return jsonify({
            "error":"err"
        })

@app.route("/testElastic", methods=['GET', 'POST'])
def testElastic():
    try:
        data = request.form
        client = Elasticsearch('http://localhost:9200')
        index="users"
        pageSize = int(data['pageSize'])
        page = int(data['page'])
        skip = (page -1)*pageSize
        excluded_chat_ids = [0]

        if( data['listIdChat'] is not None and data['listIdChat'] != ''):
            excluded_chat_ids = str(data['listIdChat']).split(",")
        must = []
        
        if( data['userName'] is not None):
            findword = remove_accents(str(data['userName']))
            words = findword.split()
            for findword_child in words:
                must.append({
                    "regexp": {
                        "userName": {
                            "value": ".*" + findword_child + ".*"
                        }
                    }
                })
        
        if(data['phoneTK'] is not None):
            findword = remove_accents(str(data['phoneTK']))
            words = findword.split()
            for findword_child in words:
                must.append({
                    "regexp": {
                        "phoneTK": {
                            "value": ".*" + findword_child + ".*"
                        }
                    }
                })  
        
        if( data['email'] is not None):
            findword = remove_accents(str(data['email']))
            words = findword.split()
            for findword_child in words:
                must.append({
                    "regexp": {
                        "email": {
                            "value": ".*" + findword_child + ".*"
                        }
                    }
                }) 
        
        
        query = {
            "bool": {
                "should": must,
                "must_not": [
                    {
                        "terms": {
                            "chat_id": excluded_chat_ids
                        }
                    }
                ]
            }
        }
        
        listuser = client.search(
            index=index,
            body={  
                    "from": skip,
                    "size": pageSize,
                    "sort":[
                        { "createdAt" : "desc" }
                    ],
                    "query": query,
                }
        )
        
        listuser = listuser["hits"]["hits"]
        listuserfinal = []
        for user in listuser:
            obj = user["_source"]
            listuserfinal.append(int(obj["chat_id"]))
        count = client.count(
            index=index,
            body={  
                    "query": query,
                }
        )
        return jsonify({
            "data":{
                "listuser":listuserfinal,
                "count":int(count['count']),
            }
        })
    except Exception as error:
        print("An exception occurred:", error)
        return jsonify({
            "error":"err"
        })

@app.route("/checkData",methods=['GET', 'POST'])
def checkData():
    try:
        data = request.form
        client = Elasticsearch('http://localhost:9200')
        index="candidate"
        query = {}
        query_1 = {
            "bool": { "must_not": [ { "exists": { "field": "use_phone" } } ] }
        }
        query_2 = {
            "bool": {
                "must": [
                    { "exists": { "field": "use_phone" } },
                    { "match": { "use_phone.keyword": "" } }
                ]
            }
        }
        query_3 = {
            "bool": {
                "must": [
                    { "exists": { "field": "use_phone" } } 
                ],
                "must_not": [
                    { "match": { "use_phone.keyword": "" } }
                ]
            }
        }
        
        query_4 = {
            "bool": {
                "must": [
                    { "range": { "percents": { "gte": 45 } } } 
                ]
            }
        }
        
        count = client.count(
            index=index
        )
        count_1 = client.count(
            index=index,
            body={  
                    "query": query_1,
                }
        )
        count_2 = client.count(
            index=index,
            body={  
                    "query": query_2,
                }
        )
        count_3 = client.count(
            index=index,
            body={  
                    "query": query_3,
                }
        )
        count_4 = client.count(
            index=index,
            body={  
                    "query": query_4,
                }
        )
        return jsonify({
            "data":{
                "sum":int(count['count']),
                "is null":int(count_1['count']),
                "empty":int(count_2['count']),
                "not empty":int(count_3['count']),
                " >= 45": int(count_4['count']),
            }
        })
    except Exception as error:
        print("An exception occurred:", error)
        return jsonify({
            "error":"err"
        })

@app.route("/getId",methods=['GET', 'POST'])
def getId():
    try:
        data = request.form
        client = Elasticsearch('http://localhost:9200')
        index="candidate"
        if( 'pageSize' in data and data['pageSize'] != '' ):
            pageSize = int(data['pageSize'])
        else:
            pageSize = 500
        
        if( 'page' in data and data['page'] != '' ):
            page = int(data['page'])
        else:
            page = 1
        skip = (page -1)*pageSize
        
        listuser = client.search(
            index=index,
            body={  
                    "from": skip,
                    "size": pageSize,
                    "sort":[
                        { "use_id" : "desc" }
                    ]
                }
        )
        
        listuser = listuser["hits"]["hits"]
        listuserfinal = []
        for user in listuser:
            obj = user["_source"]
            listuserfinal.append(int(obj["use_id"]))
        count = client.count(
            index=index
        )
        
        return jsonify({
            "data":{
                "count":int(count['count']),
                "listuser":listuserfinal
            }
        })
    except Exception as error:
        print("An exception occurred:", error)
        return jsonify({
            "error":"err"
        })



if __name__ == '__main__':
    app.run(host='0.0.0.0',debug=True, port=9002)
