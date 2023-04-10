from elasticsearch import Elasticsearch
import csv

ES_CA = '~/.elasticsearch/root.crt'
ES_USER = ''
ES_PASS = ''
ES_HOSTS = [
  "",
  ]
es = Elasticsearch(
  ES_HOSTS,
  basic_auth=(ES_USER, ES_PASS),
  verify_certs=True,
  ca_certs=ES_CA)

print(f"Connected: `{es.info().body['cluster_name']}`")

with open("./the_oscar_award.csv", "r") as f:
    reader = csv.reader(f, )
    header = next(reader)
    for line in reader:
        document = dict(zip(header, line))
        es.index(index="films", document=document)
