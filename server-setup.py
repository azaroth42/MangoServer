
import requests
import json
import sys


container = {
 "@context": "http://www.w3.org/ns/anno.jsonld",
 "@type": "ldp:BasicContainer",
 "dc:title": "A Basic Annotation Container"
}

anno = {
 "@context": "http://www.w3.org/ns/anno.jsonld",
 "@type": "Annotation",
 "body": {"value": "I like this thing"},
 "target": "http://www.example.org/"
}

hdrs = {'Content-Type': 'application/json'}
url = "http://localhost:8000/annos/"

if '--container' in sys.argv:
	data = json.dumps(container)
	req = requests.put(url=url, data=data, headers=hdrs)

if '--delete-container' in sys.argv:
	req = requests.delete(url=url)

if '--annotation' in sys.argv:
	data = json.dumps(anno)
	req = requests.post(url=url, data=data, headers=hdrs)	

if '--slug-annotation' in sys.argv:
	data = json.dumps(anno)
	hdrs['Slug'] = 'my_first_annoation'
	req = requests.post(url=url, data=data, headers=hdrs)

