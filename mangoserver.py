
# Design Notes
# All URLs that end in a / are containers
# All containers are Mongo collections
# All resources are in the appropriate container
# Container _id is from prefix to / (not included)
# Resource _id is just the trailing segment

import json
from functools import partial
from bson import ObjectId
from pymongo import Connection

from bottle import Bottle, route, run, request, response, abort, error

import uuid
import datetime

class MongoEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, ObjectId):
            return str(obj)
        elif isinstance(obj, datetime.datetime):
            return obj.isoformat()
        return super(MongoEncoder, self).default(obj)


class LdpServer(object):

    def __init__(self, database="ldp", host='localhost', port=27017,
                 sort_keys=True, compact_json=False, indent_json=2,
                 url_host="http://localhost:8000/", url_prefix=""):

        # Mongo Connection
        self.mongo_host = host
        self.mongo_port = port
        self.mongo_db = database
        self.connection = self._connect(database, host, port)

        # JSON Serialization options
        self.sort_keys = sort_keys
        self.compact_json = compact_json
        self.indent_json = indent_json

        self.url_host = url_host
        self.url_prefix = url_prefix

        self._container_desc_id = "__container_metadata__"


    def _jsonify(self, what, uri):
        what['@id'] = uri
        try:
            del what['_id']
        except:
            pass            
        if self.compact_json:
            me = MongoEncoder(sort_keys=self.sort_keys, separators=(',',':'))
        else:
            me = MongoEncoder(sort_keys=self.sort_keys, indent=self.indent_json)
        return me.encode(what)

    def _connect(self, database, host=None, port=None):
        return Connection(host=host, port=port)[database]

    def _collection(self, container):
        if not self.connection:
            self.connection = self._connect(self.mongo_db, self.mongo_host, self.mongo_port)
        return self.connection[container]

    def _make_uri(self, container, resource=""):
        return "%s/%s%s/%s" % (self.url_host, self.url_prefix, container, resource)

    def _make_id(self, container, resource=""):
        if not resource:
            # Create new id
            resource = str(uuid.uuid4())
            slug = request.headers.get('slug', '')
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
        if request.headers.get('Content-Type', '').strip().startswith("application/ld+json"):
            # put the json into request.json, like application/json does automatically
            b = request._get_body_string()
            if b:
                request.json = json.loads(b)

    def _fix_json(self, js={}):
        if not js:
            try:    
                js = request.json
            except:
                abort(400, "JSON is not well formed")
        if js.has_key('_id'):
            del js['_id']
        if js.has_key('@id'):
            del js['@id']
        return js

    def get_container(self, container):
        coll = self._collection(container)
        metadata = coll.find_one({"_id": self._container_desc_id})
        if metadata == None:
            abort(404)

        # Implement LDP Paging here
        limit = 1000
        offset = 0

        cursor = coll.find({}, {'_id':1})

        if not limit:
            included = []
        else:
            objects = list(cursor.skip(offset).limit(limit))
            included = []

            for what in objects:
                mid = what['_id']
                if mid == self._container_desc_id:
                    # Don't include metadata as contained resources
                    continue
                out = self._fix_json(what)
                out['@id'] = self._make_uri(container, self._unmake_id(mid))           
                included.append(out)

        resp = {"@context": "http://www.w3.org/ns/anno.jsonld",
                "@type": "ldp:BasicContainer",
                "as:totalItems" : cursor.count()-1,
                "ldp:contains": included}
        resp.update(metadata)

        # Add required headers
        response.headers['Link'] = '<http://www.w3.org/ns/ldp#BasicContainer>;rel="type",<http://www.w3.org/ns/ldp#Resource>;rel="type"'

        uri = self._make_uri(container)
        return self._jsonify(resp, uri)

    def put_container(self, container):
        # Grab the body and put it into magic __container_metadata__
        js = self._fix_json()
        coll = self._collection(container)
        metadata = coll.find_one({"_id": self._container_desc_id})

        if metadata == None:
            metadata = js
            metadata["_id"] = self._container_desc_id
            current = coll.insert(metadata)
            response.status = 201
        else:
            metadata.update(js)
            coll.update({"_id": self._container_desc_id}, js)
            current = metadata
            response.status = 200

        uri = self._make_uri(container)
        return self._jsonify(js, uri)        

    def delete_container(self, container):
        coll = self._collection(container)
        coll.drop()
        response.status = 204
        return {}

    def get_resource(self, container, resource):
        coll = self._collection(container)
        data = coll.find_one({"_id": self._make_id(container, resource)})
        if not data:
            abort(404)

        uri = self._make_uri(container, resource)
        return self._jsonify(data, uri)

    def post_container(self, container):
        coll = self._collection(container)

        js = self._fix_json()
        myid = self._make_id(container)
        uri = self._make_uri(container, myid)
        js["_id"] = myid
        inserted = coll.insert(js)
        response.status = 201
        return self._jsonify(js, uri)

    def put_resource(self, container, resource):
        coll = self._collection(rtype)
        js = self._fix_json()
        coll.update({"_id": self._make_id(container, resource)}, js)
        response.status = 202
        uri = self._make_uri(container, resource)
        return self._jsonify(js, uri)

    def post_resource(self, container, resource):
        abort(400)

    def patch_resource(self, container, resource):
        coll = self._collection(container)
        coll.update({"_id": ObjectId(self._make_id(container, resource))},
                          {"$set": request.json})
        response.status = 202
        return self.get_resource(container, resource)

    def delete_resource(self, container, resource):
        coll = self._collection(container)
        coll.remove({"_id": self._make_id(container, resource)})
        response.status = 204
        return {}

    def dispatch_views(self):
        methods = ["get", "post", "put", "patch", "delete", "options"]
        for m in methods:
            self.app.route('/%s<container:re:.*>/' % self.url_prefix,
                [m], getattr(self, "%s_container" % m, self.not_implemented))
            self.app.route('/%s<container:re:.*>/<resource>' % self.url_prefix,
                [m], getattr(self, "%s_resource" % m, self.not_implemented))

    def before_request(self):
        self._handle_ld_json()


    def after_request(self):
        """A bottle hook for json responses."""

        #response['content_type'] = "application/ld+json"
        response["content_type"] = "application/json"
        methods = 'PUT, PATCH, GET, POST, DELETE, OPTIONS'
        headers = 'Origin, Accept, Content-Type, X-Requested-With'
        response.headers['Access-Control-Allow-Origin'] = '*'
        response.headers['Access-Control-Allow-Methods'] = methods
        response.headers['Access-Control-Allow-Headers'] = headers


    def not_implemented(self, *args, **kwargs):
        """Returns not implemented status."""
        abort(501)

    def empty_response(self, *args, **kwargs):
        """Empty response"""

    options_single = empty_response
    options_multiple = empty_response


    def error(self, error, message=None):
        """Returns the error response."""
        return self._jsonify({"error": error.status_code,
                        "message": error.body or message}, "")

    def get_error_handler(self):
        """Customized errors"""
        return {
            500: partial(self.error, message="Internal Server Error."),
            404: partial(self.error, message="Document Not Found."),
            501: partial(self.error, message="Not Implemented."),
            405: partial(self.error, message="Method Not Allowed."),
            403: partial(self.error, message="Forbidden."),
            400: self.error
        }

    def get_bottle_app(self):
        """Returns bottle instance"""
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
                      help="MongoDB database name", default="ldp")
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

    debug = options.debug in ['True', True, 1]
    sort_keys = options.sort_keys in ['True', True, '1']
    compact_json = options.compact_json in ['True', True, '1']
    indent = options.indent_json
    jsonld = options.json_ld in ['True', True, '1']
    url_prefix = options.url_prefix

    mr = LdpServer(
        host=options.mongodb_host,
        port=options.mongodb_port,
        database=options.database,
        sort_keys=sort_keys,
        compact_json=compact_json,
        indent_json=indent,
        url_host = "http://%s:%s" % (host, port),
        url_prefix=url_prefix
    )

    run(host=host, port=port, app=mr.get_bottle_app(), debug=debug)

if __name__ == "__main__":
    main()
