
import requests
import json
import sys


container = {
 "@context": "http://www.w3.org/ns/anno.jsonld",
 "@type": "ldp:BasicContainer",
 "dc:title": "A Basic Container"
}

anno = {
 "@context": "http://www.w3.org/ns/anno.jsonld",
 "@type": "Annotation",
 "body": {"text": "I like this thing"},
 "target": "http://www.example.org/"
}

hdrs = {'Content-Type': 'application/json'}
url = "http://localhost:8000/annos/"

if '--container' in sys.argv:
	data = json.dumps(container)
	req = requests.put(url=url, data=data, headers=hdrs)

if '--annotation' in sys.argv:
	data = json.dumps(anno)
	req = requests.post(url=url, data=data, headers=hdrs)	


