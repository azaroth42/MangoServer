
# Design Notes
# All URLs that end in a / are containers
# All containers are Mongo collections, regardless of where they appear in the tree
# All resources are in the appropriate collection

import json
from functools import partial
import uuid
import datetime
import time
import hashlib
from collections import OrderedDict

from bottle import Bottle, route, run, request, response, abort, error, redirect

# Requires pymongo 3.x
from bson import ObjectId
from pymongo import MongoClient
from rdflib import Graph
from pyld import jsonld

# Stop code from looking up the contexts online EVERY TIME
def load_document_local(url):
    doc = {
        'contextUrl': None,
        'documentUrl': None,
        'document': ''
    }
    if url == "http://iiif.io/api/presentation/2/context.json":
        fn = "contexts/context_20.json"
    elif url in ["http://www.w3.org/ns/oa.jsonld", "http://www.w3.org/ns/oa-context-20130208.json"]:
        fn = "contexts/context_oa.json"
    elif url in ['http://www.w3.org/ns/anno.jsonld']:
        fn = "contexts/context_wawg.json"
    fh = file(fn)
    data = fh.read()
    fh.close()
    doc['document'] = data;
    return doc

jsonld.set_document_loader(load_document_local)

class MongoEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, ObjectId):
            return str(obj)
        elif isinstance(obj, datetime.datetime):
            return obj.isoformat()
        return super(MongoEncoder, self).default(obj)

class MangoServer(object):

    def __init__(self, database="mango", host='localhost', port=27017,
                 sort_keys=True, human_sort_keys=True, compact_json=False, indent_json=2,
                 url_host="http://localhost:8000/", url_prefix="", json_ld=True):

        # Mongo Connection
        self.mongo_host = host
        self.mongo_port = port
        self.mongo_db = database
        self.connection = self._connect(database, host, port)

        # JSON Serialization options
        self.sort_keys = sort_keys
        self.human_sort_keys = human_sort_keys
        self.compact_json = compact_json
        self.indent_json = indent_json
        self.json_content_type = "application/ld+json" if json_ld else "application/json"

        self.known_profiles = ["http://iiif.io/api/presentation/2/context.json",
             "http://www.w3.org/ns/oa.jsonld",
             "http://www.w3.org/ns/oa-context-20130208.json",
             'http://www.w3.org/ns/anno.jsonld']
        self.default_profile = 'http://www.w3.org/ns/anno.jsonld'

        self.url_host = url_host
        self.url_prefix = url_prefix
        self.server_identity = {"type": "Software", "label": "MangoServer v0.9", "homepage": "https://github.com/azaroth42/MangoServer/"}

        self._container_desc_id = "__container_metadata__"
        self.json_ld_profile = "http://www.w3.org/ns/anno.jsonld"
        self.default_context = "http://www.w3.org/ns/anno.jsonld"
        self.uri_page_size = 500
        self.description_page_size = 20
        self.server_prefers = "description" # or "uri"
        self.require_if_match = True

        self.rdflib_format_map = {
              'application/rdf+xml' : 'pretty-xml',
              'text/rdf+xml' : 'pretty-xml',
              'text/turtle' : 'turtle',
              'application/turtle' : 'turtle',
              'application/x-turtle' : 'turtle',
              'text/plain' : 'nt',
              'text/rdf+n3' : 'n3'}

        self.key_order = ['@context', 'id', 'type', 'label', 'name', 'account', 'motivation',
            'creator', 'created', 'modified', 'generator', 'generated', 'audience', 'via', 'canonical', 
            'stylesheet', 'purpose', 'value', 'format', 'language', 'start', 'end', 'prefix', 'exact', 
            'suffix', 'body', 'bodyValue', 'target', 'total', 'partOf', 'first', 'last', 'state', 'selector',
            'styleClass', 'scope', 'renderedVia', 'source']
        self.key_order_hash = dict([(self.key_order[x],x) for x in range(len(self.key_order))])
        self.key_order_default = 1000
        # make sure structures and long lists are at the end
        self.key_order_hash['refinedBy'] = 4000
        self.key_order_hash['items'] = 5000
        self.key_order_hash['contains'] = 5001

    def _connect(self, database, host=None, port=None):
        return MongoClient(host=host, port=port)[database]

    def _collection(self, container):
        if not self.connection:
            self.connection = self._connect(self.mongo_db, self.mongo_host, self.mongo_port)

        container = self.connection[container]
        return container

    def _make_uri(self, container, resource=""):
        return "%s/%s%s/%s" % (self.url_host, self.url_prefix, container, resource)

    def _slug_ok(self, value):
        if len(value) > 128: return False
        if value.find('/') > -1: return False
        if value.find('#') > -1: return False
        if value.find('%') > -1: return False
        if value.find('?') > -1: return False
        if value == self._container_desc_id: return False
        value = value.replace(' ', '+')
        value = value.replace('[', '')
        value = value.replace(']', '')
        return value

    def _make_id(self, container, resource=""):
        if not resource:
            # Create new id, maybe using slug
            resource = str(uuid.uuid4())
            slug = self._slug_ok(request.headers.get('slug', ''))
            if slug:
                # make sure it doesn't already exist
                coll = self._collection(container)
                exists = coll.find_one({"_id": slug})
                if not exists:
                    resource = slug                
        return "anno_" + resource

    def _unmake_id(self, value):
        if value.startswith('anno_'):
            return value[5:]
        else:
            return value

    def _handle_ld_json(self):
        b = request._get_body_string()
        if request.headers.get('Content-Type', '').strip().startswith("application/ld+json"):
            # put the json into request.json, like application/json does automatically
            # Except it doesn't seem to work :(
            if b:
                request._json = json.loads(b)
            else:
                request._json = request.json
        elif b:
            print "Got {0} for body: {1}".format(request.headers['Content-Type'], b)


    def _fix_json(self, js={}, via=False):
        # Validate / Patch JSON
        if not js:
            try:    
                js = request._json
                if not js:
                    abort(400, "Empty JSON")
            except Exception, e:
                abort(400, "JSON is not well formed: {0}".format(e))
        if js.has_key('_id'):
            del js['_id']
        if js.has_key('id'):
            # Record old IRI in via
            if via:
                if js.has_key('via'):
                    v = js['via']
                    if type(v) != list:
                        v = [v]
                    if not js['id'] in v:
                        v.append(js['id'])
                        js['via'] = v
                else:
                    js['via'] = js['id']
            del js['id']
        return js

    def decorate_annotation(self, js):
        now = time.strftime("%Y-%m-%dT%H:%m:%SZ", time.gmtime())
        if not js.has_key('created'):
            # Add created now() as created time
            js['created'] = now
        else:
            js['modified'] = now
        # if we have authentication, then add user attributes
        if not js.has_key('canonical') and not js.has_key('via'):
            js['canonical'] = js['id']
        js['generator'] = self.server_identity
        return js

    def _jsonify_human(self, what):
            what = OrderedDict(sorted(what.items(), 
                key=lambda x: self.key_order_hash.get(x[0], self.key_order_default)))
            for k,v in what.items():
                if type(v) == dict:
                    what[k] = self._jsonify_human(v)
                elif type(v) == list:
                    nl = []
                    for i in v:
                        if type(i) == dict:
                            nl.append(self._jsonify_human(i))
                        else:
                            nl.append(i)
                    what[k] = nl
            return what

    def _jsonify(self, what, uri):
        what['id'] = uri
        try:
            del what['_id']
        except:
            pass            
        if self.compact_json:
            me = MongoEncoder(sort_keys=self.sort_keys, separators=(',',':'))
        if self.human_sort_keys:
            me = MongoEncoder(sort_keys=False, indent=self.indent_json)
            # Rebuild ALL the json with OrderedDicts
            what = self._jsonify_human(what)
        else:
            me = MongoEncoder(sort_keys=self.sort_keys, indent=self.indent_json)
        return me.encode(what)

    def _parse_accept(self, value):
        prefs = []
        for item in value.split(","):
            parts = item.split(";")
            main = parts.pop(0).strip()
            params = []
            q = 1.0
            for part in parts:
                (key, value) = part.lstrip().split("=", 1)
                key = key.strip()
                value = value.strip().replace('"', '')
                if key == "q":
                    q = float(value)
                else:
                    params.append((key, value))
            prefs.append((main, dict(params), q))
        prefs.sort(lambda x, y: -cmp(x[2], y[2]))        
        return prefs

    def _parse_prefer(self, value):
        prefs = []
        for item in value.split(","):
            parts = item.split(";")
            main = parts.pop(0).strip()
            if "=" in main:
                # Can have whitespace, so can't use as useful key
                main = [x.strip().replace('"', '') for x in main.split('=')]
            else:
                main = [main, ""]
            params = []
            for part in parts:
                (key, value) = part.lstrip().split("=", 1)
                key = key.strip()
                value = value.strip().replace('"', '')
                params.append((key, value))
            prefs.append((main, dict(params)))       
        return prefs

    def _conneg(self, data, uri):
        # Content Negotiate with client
        out = self._jsonify(data, uri)

        # We're on our way out the door ...
        # Construct our ETag from the JSON first
        h = hashlib.md5()
        h.update(out)
        response['ETag'] = h.hexdigest()

        accept = request.headers.get('Accept', '')
        ct = self.json_content_type
        if accept:
            prefs = self._parse_accept(accept)
            format = ""
            for p in prefs:
                if self.rdflib_format_map.has_key(p[0]):
                    ct = p[0]
                    format = self.rdflib_format_map[p[0]]
                    break
                elif p[0] in ['application/json', 'application/ld+json']:
                    ct = p[0]
                    if "profile" in p[1]:
                        prof = p[1]['profile']
                        if prof in self.known_profiles:
                            ct += ';profile="{0}"'.format(prof)
                            if prof != self.default_profile:
                                # XXX reframe our content
                                # See Context Switcher code
                                pass
                    break
            if format:
                g = Graph()
                g.parse(data=out, format='json-ld')
                out = g.serialize(format=format)

        response['content_type'] = ct
        return out

    def add_link_header(self, uri, params):
        # XXX Make this less ugly
        l = "<{0}>".format(uri)
        if params:
            p = ['{0}="{1}"'.format(k,v) for k,v in params.items()]
            l += ";"
            l += ";".join(p)
        if 'link' in response.headers:
            response.headers['link'] += ", {0}".format(l)
        else:
            response.headers['link'] = l

    def get_container_page(self, container, coll, metadata):
        uri = self._make_uri(container)

        include = request.query.get('include', self.server_prefers)
        page = request.query.get('page', '0')
        page = int(page)    
        page_size = getattr(self, "{0}_page_size".format(include))        
        offset = page * page_size

        if include == 'description':
            cursor = coll.find({'_id': {'$ne' : self._container_desc_id}})
        else:
            cursor = coll.find({'_id': {'$ne' : self._container_desc_id}}, {'_id':1})

        totalItems = cursor.count()

        included = []
        for what in cursor.skip(offset).limit(page_size):
            myid = what['_id']
            if include == 'uri':
                included.append(self._make_uri(container, self._unmake_id(myid)))
            else:
                out = self._fix_json(what)
                out['id'] = self._make_uri(container, self._unmake_id(myid))           
                try:
                    del out['@context']
                except:
                    pass
                included.append(out)

        last_page = totalItems / page_size
        first = "{0}?include={1}&page=0".format(uri, include)
        last = "{0}?include={1}&page={2}".format(uri, include, last_page)
        me = "{0}?include={1}&page={2}".format(uri, include, page)
        curi = "{0}?include={1}".format(uri, include)

        resp = {"@context": "http://www.w3.org/ns/anno.jsonld",
                "id": me,            
                "type": "AnnotationPage",
                "partOf": {
                    "id": curi,
                    "total": totalItems
                },
                "items" : included} 

        if me != first:
            resp['partOf']['first'] = first
        if page != last_page:
            resp['next'] = '{0}?include={1}&page={2}'.format(uri, include, page+1)
            resp['partOf']['last'] = last
        if page:
            resp['prev'] = '{0}?include={1}&page={2}'.format(uri, include, page-1)
        return self._conneg(resp, me)

    def get_container_projection(self, container, coll, metadata):
        uri = self._make_uri(container)
        include = request.query.get('include', self.server_prefers)

        cursor = coll.find({'_id': {'$ne' : self._container_desc_id}}, {'_id':1})
        totalItems = cursor.count()
        page_size = getattr(self, "{0}_page_size".format(include))        
        me = "{0}?include={1}".format(uri, include)

        resp = {"@context": "http://www.w3.org/ns/anno.jsonld",
                "id": me,            
                "items" : totalItems} 
        resp.update(metadata)

        last = totalItems/page_size;
        resp['first'] = "{0}?include={1}&page=0".format(uri, include)
        resp['last'] = "{0}?include={1}&page={2}".format(uri, include, last)

        return self._conneg(resp, me)


    def get_container_base(self, container, coll, metadata):
        uri = self._make_uri(container)        

        prefer = request.headers.get('Prefer', '')
        include = ''
        if prefer:
            prefs = self._parse_prefer(prefer)
            for p in prefs:
                if p[0] == ['return', 'representation']:
                    if p[1]['include'] == 'http://www.w3.org/ns/ldp#PreferMinimalContainer':                       
                        # No members
                        include = 'none'
                        paged = False
                    elif p[1]['include'] == 'http://www.w3.org/ns/oa#PreferContainedURIs':
                        # include only URIs
                        include = 'uri'
                    elif p[1]['include'] == 'http://www.w3.org/ns/oa#PreferContainedDescriptions':
                        # include full descriptions
                        include = 'description'
                    else:
                        continue
                    response['Preference-Applied'] = "return=representation"

        cursor = coll.find({'_id': {'$ne' : self._container_desc_id}}, {'_id':1})
        totalItems = cursor.count()

        if not include:
            # Make a sensible default, lacking a client preference
            include = self.server_prefers
            max_prefers = getattr(self, "{0}_page_size".format(include))
            paged = totalItems > max_prefers
        elif include == 'uri':
            # Determine whether we need pages
            paged = totalItems > self.uri_page_size
        elif include == 'description':
            paged = totalItems > self.description_page_size

        # Allow collection to determine its orderedness
        t = metadata['type']        

        if not paged:
            resp = {"@context": ["http://www.w3.org/ns/anno.jsonld",
                    "http://www.w3c.org/ns/ldp.jsonld"],
                    "id": uri,            
                    "total" : totalItems} 
            resp.update(metadata)

            if include != 'none':
                if include == 'description':
                    cursor = coll.find({'_id': {'$ne' : self._container_desc_id}})
                included = []
                for what in cursor:
                    myid = what['_id']
                    out = self._fix_json(what)
                    out['id'] = self._make_uri(container, self._unmake_id(myid))           
                    included.append(out)
                resp['contains'] = included
            return self._conneg(resp, uri)
        else:
            # Redirect to new URI, as we're paged
            newuri = "{0}?include={1}".format(uri, include)
            redirect(newuri, 303)

    def get_container(self, container):
        # reroute to appropriate handler
        coll = self._collection(container)
        metadata = coll.find_one({"_id": self._container_desc_id})
        if metadata == None:
            abort(404, "Unknown container")

        self.add_link_header('http://www.w3.org/ns/ldp#BasicContainer', {'rel':'type'})
        self.add_link_header('http://www.w3.org/TR/annotation-protocol/', {'rel': 'http://www.w3.org/ns/ldp#constrainedBy'})

        if request.query.get('page', ''):
            # We're a paged
            self.add_link_header('http://www.w3.org/ns/oa#AnnotationPage', {'rel':'type'})
            return self.get_container_page(container, coll, metadata)
        elif request.query.get('include', ''):
            # We're a projection
            return self.get_container_projection(container, coll, metadata)
        else:
            # We're the full container
            self.add_link_header('http://www.w3.org/ns/oa#AnnotationCollection', {'rel':'type'})
            return self.get_container_base(container, coll, metadata)

    def put_container(self, container):
        # Grab the body and put it into magic __container_metadata__
        js = self._fix_json()
        coll = self._collection(container)
        metadata = coll.find_one({"_id": self._container_desc_id})

        if metadata == None:
            metadata = js
            metadata["_id"] = self._container_desc_id
            try:
                del metadata['id']
            except:
                pass
            current = coll.insert_one(metadata)
            response.status = 201
        else:
            metadata.update(js)
            coll.replace_one({"_id": self._container_desc_id}, js)
            current = metadata
            response.status = 200

        uri = self._make_uri(container)
        return self._conneg(js, uri)        

    def delete_container(self, container):
        coll = self._collection(container)
        coll.drop()
        response.status = 204
        return ""

    def get_resource(self, container, resource):
        coll = self._collection(container)
        data = coll.find_one({"_id": self._make_id(container, resource)})
        if not data:
            abort(404)
        uri = self._make_uri(container, resource)
        return self._conneg(data, uri)

    def post_container(self, container):
        coll = self._collection(container)
        js = self._fix_json(via=True)
        myid = self._make_id(container)
        uri = self._make_uri(container, myid)
        js = self.decorate_annotation(js)
        js["_id"] = myid
        inserted = coll.insert_one(js)
        response.status = 201
        return self._conneg(js, uri)

    def post_resource(self, container, resource):
        abort(400, "Cannot POST to an individual resource, use PUT or POST to a container")

    def check_if_match(self, coll, container, resource):
        if 'if-match' in request.headers:
            check = request.headers['if-match']
            data = coll.find_one({"_id": self._make_id(container, resource)})
            if not data:
                abort(404)

            uri = self._make_uri(container, resource)
            out = self._jsonify(data, uri)
            h = hashlib.md5()
            h.update(out)
            current = h.hexdigest()           
            if check != current:
                # Collision
                abort(412)
        elif self.require_if_match:
            abort(412, "No If-Match header for PUT")
        return True

    def put_resource(self, container, resource):
        # Update individual Annotation
        coll = self._collection(container)
        js = self._fix_json()
        self.check_if_match(coll, container, resource) 
        coll.replace_one({"_id": self._make_id(container, resource)}, js)
        response.status = 202
        uri = self._make_uri(container, resource)
        return self._conneg(js, uri)

    def patch_resource(self, container, resource):
        coll = self._collection(container)
        self.check_if_match(coll, container, resource) 
        coll.update_one({"_id": self._make_id(container, resource)},
                          {"$set": request._json})
        response.status = 202
        return self.get_resource(container, resource)

    def delete_resource(self, container, resource):
        coll = self._collection(container)
        uri = self._make_uri(container, resource) 
        self.check_if_match(coll, container, resource)
        coll.delete_one({"_id": self._make_id(container, resource)})
        response.status = 204
        return ""

    def head_container(self, container):
        val = self.get_container(container)
        response.headers['Content-Length'] = len(val)
        return ""

    def head_resource(self, container, resource):
        val = self.get_resource(container, resource)
        response.headers['Content-Length'] = len(val)
        return ""

    def dispatch_views(self):
        methods = ["get", "head", "post", "put", "patch", "delete", "options"]
        for m in methods:
            self.app.route('/%s<container:re:.*>/' % self.url_prefix,
                [m], getattr(self, "%s_container" % m, self.not_implemented))
            self.app.route('/%s<container:re:.*>/<resource>' % self.url_prefix,
                [m], getattr(self, "%s_resource" % m, self.not_implemented))

    def before_request(self):
        # Process incoming application/ld+json as application/json
        self._handle_ld_json()

    def after_request(self):
        # Add CORS and other static headers
        methods = 'PUT, PATCH, GET, POST, DELETE, OPTIONS, HEAD'
        response.headers['Access-Control-Allow-Origin'] = '*'
        response.headers['Access-Control-Allow-Methods'] = methods
        response.headers['Allow'] = methods
        response.headers['Vary'] = "Accept"

    def not_implemented(self, *args, **kwargs):
        """Returns not implemented status."""
        abort(501)

    def empty_response(self, *args, **kwargs):
        """Empty response"""

    options_container = empty_response
    options_resource = empty_response

    def error(self, error, message=None):
        # Make a little error message in JSON-LD
        return self._jsonify({"error": error.status_code,
                        "message": error.body or message}, "")

    def get_error_handler(self):
        return {
            500: partial(self.error, message="Internal Server Error"),
            404: partial(self.error, message="Not Found"),
            501: partial(self.error, message="Not Implemented"),
            405: partial(self.error, message="Method Not Allowed"),
            403: partial(self.error, message="Forbidden"),
            412: partial(self.error, message="Precondition Failed"),
            400: partial(self.error, message="Client Error")
        }

    def get_bottle_app(self):
        self.app = Bottle()
        self.dispatch_views()
        self.app.hook('before_request')(self.before_request)
        self.app.hook('after_request')(self.after_request)
        self.app.error_handler = self.get_error_handler()
        return self.app


def main():
    from optparse import OptionParser
    parser = OptionParser()
    parser.add_option("--bind", dest="address",
                      help="Binds an address to listen")
    parser.add_option("--mongodb-host", dest="mongodb_host",
                      help="MongoDB host", default="localhost")
    parser.add_option("--mongodb-port", dest="mongodb_port",
                      help="MongoDB port", default=27017)
    parser.add_option("-d", "--database", dest="database",
                      help="MongoDB database name", default="mango")
    parser.add_option('-p', '--prefix', dest="url_prefix", 
                      help="URL Prefix in API pattern", default="")
    parser.add_option('-s', '--sort-keys', dest="sort_keys", default=True,
                       help="Should json output have sorted keys?")
    parser.add_option('--compact-json', dest="compact_json", default=False,
                       help="Should json output have compact whitespace?")
    parser.add_option('--indent-json', dest="indent_json", default=2, type=int,
                       help="Number of spaces to indent json output")
    parser.add_option('--json-ld', dest="json_ld", default=True,
                       help="Should return json-ld media type instead of json?")
    parser.add_option('--debug', dest="debug", default=True)

    options, args = parser.parse_args()

    host, port = (options.address or 'localhost'), 8000
    if ':' in host:
        host, port = host.rsplit(':', 1)

    # make booleans
    debug = options.debug in ['True', True, 1]
    sort_keys = options.sort_keys in ['True', True, '1']
    compact_json = options.compact_json in ['True', True, '1']
    jsonld = options.json_ld in ['True', True, '1']

    mr = MangoServer(
        host=options.mongodb_host,
        port=options.mongodb_port,
        database=options.database,
        sort_keys=sort_keys,
        compact_json=compact_json,
        indent_json=options.indent_json,
        url_host = "http://%s:%s" % (host, port),
        url_prefix=options.url_prefix,
        json_ld=jsonld
    )

    run(host=host, port=port, app=mr.get_bottle_app(), debug=debug)

if __name__ == "__main__":
    main()
