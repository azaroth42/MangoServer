
import requests
import json
import sys
import os
import random

container = {
 "@context": ["http://www.w3.org/ns/anno.jsonld", "http://www.w3c.org/ns/ldp.jsonld"],
 "type": ["BasicContainer", "AnnotationCollection"],
 "label": "A Basic Annotation Container"
}

anno = {
 "@context": "http://www.w3.org/ns/anno.jsonld",
 "type": "Annotation",
 "body": {"value": "I like this thing"},
 "target": "http://www.example.org/"
}

hdrs = {'Content-Type': 'application/ld+json;profile="http://www.w3.org/ns/anno.jsonld"'}
url = "http://localhost:8000/annos/"

if '--container' in sys.argv:
	data = json.dumps(container)
	print "Sending: " + data
	req = requests.put(url=url, data=data, headers=hdrs)

if '--delete-container' in sys.argv:
	req = requests.delete(url=url)

if '--annotation' in sys.argv:
	data = json.dumps(anno)
	req = requests.post(url=url, data=data, headers=hdrs)	

if '--examples' in sys.argv:
	where = "/Users/rsanderson/Development/OpenAnno/web-annotation/model/wd2/examples/correct/"
	files = os.listdir(where)
	for f in files:
		if f.startswith('anno'):
			fn = os.path.join(where, f)
			fh = file(fn)
			data = fh.read()
			fh.close()
			js = json.loads(data)
			uri = js['id']
			slug = os.path.split(uri)[1]
			hdrs['Slug'] = slug	
			req = requests.post(url=url, data=data, headers=hdrs)					


if '--many-annotations' in sys.argv:
	for x in range(500):
		anno['body']['value'] = "Annotation {0}".format(x)
		hdrs['Slug'] = 'anno_{0}'.format(x)
		data = json.dumps(anno)
		req = requests.post(url=url, data=data, headers=hdrs)

if '--slug-annotation' in sys.argv:
	data = json.dumps(anno)
	hdrs['Slug'] = 'my_first_annoation'
	req = requests.post(url=url, data=data, headers=hdrs)

if '--put' in sys.argv:
	# First get list of annotations
	phdrs = {"Prefer": 'return=representation;include="http://www.w3.org/ns/oa#PreferContainedURIs"'}
	req = requests.get(url=url, headers=phdrs)
	body = req.json()
	annos = body['contains']
	rnd = random.randrange(0, len(annos))
	annoUrl = annos[rnd]['id']

	req = requests.get(url=annoUrl)
	et = req.headers['etag']
	anno = req.json()
	anno['testing'] = "random stuff"
	data = json.dumps(anno)
	print data
	req = requests.put(url=annoUrl, data=data, headers=hdrs)
	print req.text

	hdrs['If-Match'] = et
	req = requests.put(url=annoUrl, data=data, headers=hdrs)
	print ""
	print req.text

if '--delete' in sys.argv:
	phdrs = {"Prefer": 'return=representation;include="http://www.w3.org/ns/oa#PreferContainedURIs"'}
	req = requests.get(url=url, headers=phdrs)
	body = req.json()
	annos = body['contains']
	rnd = random.randrange(0, len(annos))
	annoUrl = annos[rnd]['id']

	req = requests.get(url=annoUrl)
	et = req.headers['etag']
	hdrs['If-Match'] = et
	req = requests.delete(url=annoUrl, headers=hdrs)


 
if '--ttl' in sys.argv:
	# First get list of annotations

	annoUrl = "http://localhost:8000/annos/my_first_annoation"


	# Fetch it as turtle
	fetchHdrs = {"Accept": "text/turtle"}
	req = requests.get(url=annoUrl, headers=fetchHdrs)
	body = req.text
	print body

	body = body.replace("this thing", 'this thing FROM TURTLE :)')

	ttlhdrs = {"Content-Type": "text/turtle"}
	et = req.headers['etag']
	ttlhdrs['If-Match'] = et

	req = requests.put(url=annoUrl, data=body, headers=ttlhdrs)
	print req.text


if '--patch' in sys.argv:
	phdrs = {"Prefer": 'return=representation;include="http://www.w3.org/ns/oa#PreferContainedURIs"'}
	req = requests.get(url=url, headers=phdrs)
	body = req.json()
	annos = body['contains']
	rnd = random.randrange(0, len(annos))
	annoUrl = annos[rnd]['id']

	req = requests.get(url=annoUrl)
	et = req.headers['etag']
	data = json.dumps({"testing_patch": "Some Value Here"})
	req = requests.patch(url=annoUrl, data=data, headers=hdrs)
	print req.text

	hdrs['If-Match'] = et
	req = requests.patch(url=annoUrl, data=data, headers=hdrs)
	print ""
	print req.text
