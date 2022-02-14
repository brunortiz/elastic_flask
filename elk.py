#This code will allow a flask rest api to communicate to an elasticsearch index namely deaths and get information on the number of deaths per city based on a period
import json
from flask import Flask, request
from flask_restful import Resource, Api
from elasticsearch import  Elasticsearch
from elasticsearch_dsl import Search, MultiSearch
from elasticsearch.serializer import JSONSerializer
import csv
import pandas as pd
import sys


app = Flask(__name__)
api = Api(app)
#setting up elasticsearch connectivity with local container running on default port 9200
es = Elasticsearch(HOST='http://localhost',PORT=9200)

#defining deaths get endpoint
@app.route('/deaths', methods=['GET'])
def deaths():
    cityval = request.args.get('city')
    cityval = cityval.capitalize()

    s = Search(index='covid').using(es).query("match", city=cityval)
    response = s.execute()

    sumdeaths = 0       
    if response:
        for hit in s:
            sumdeaths = sumdeaths + int(hit.deaths)
        return "City: {} State: {} Deaths: {} ".format(hit.city, hit.state, sumdeaths)
    else:
        return "Data not Found"


if __name__ == '__main__':
    app.run(debug=True)