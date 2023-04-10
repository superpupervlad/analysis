from flask import Flask, request, render_template, send_from_directory
from elasticsearch import Elasticsearch
import wikipedia


def connect_to_es():
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

    return es

es = connect_to_es()

app = Flask(__name__)

MAX_RESULTS = 15


@app.route("/")
def home():
    return render_template("index.html")


@app.route('/<path:path>')
def send_report(path):
    return send_from_directory('templates', path)


@app.route("/search")
def search_autocomplete():
    token = request.args["q"]
    type = request.args["t"]

    if type == 'wiki':
        payload = {"match": {"_id": token}}
        resp = es.search(index="films", query=payload, size=MAX_RESULTS)
        resp = resp['hits']['hits'][0]['_source']
        search_query = resp['name'] + resp['film'] + resp['category']
        wiki_pages = wikipedia.search(search_query, results=5)
        res = []
        for page in wiki_pages:
            try:
                res.append({'name': page, 'url': wikipedia.page(page).url})
            except wikipedia.exceptions.PageError:
                continue
    else:
        token.lower()
        payload = {"bool": {"should": [
                                    {"match": {type: token}},
                                    {"wildcard": {type: f"*{token}*"}},
                                    {"fuzzy": {type: {"value": token, "fuzziness": "AUTO:2,4"}}}],
                        "minimum_should_match": 1}}
        resp = es.search(index="films", query=payload, size=MAX_RESULTS)
        res = []
        for hit in resp['hits']['hits']:
            tmp = hit['_source']
            tmp['_id'] = hit['_id']
            res.append(tmp)
    return res


if __name__ == "__main__":
    app.run(debug=True)
