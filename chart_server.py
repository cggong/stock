import http.server
import urllib.parse
import json
class ChartServerHandler(http.server.SimpleHTTPRequestHandler):
    def do_GET(self):
        if 'html' in self.path:
            return http.server.SimpleHTTPRequestHandler.do_GET(self)
        # path like 'http://123.123.123.123:32155/get_coordinate_lines?width=300&height=150'
        qs = urllib.parse.urlparse(self.path)
        # qs like ParseResult(scheme='http', netloc='123.123.123.123:32155', path='/get_coordinate_lines', params='', query='width=300&height=150', fragment='')
        query_obj = urllib.parse.parse_qsl(qs.query) 
        # query_obj like [('width', '300'), ('height', '150')]
        func = qs.path[1:]
        ret = {'width': 400}
        self.send_response(200)
        self.send_header('Content-type', 'application-json')
        self.end_headers()
        self.wfile.write(json.dumps(ret).encode())

server = http.server.HTTPServer(('localhost', 32155), ChartServerHandler)
print('Starting server at port 32155')
server.serve_forever()

# if ERROR:  OSError: [Errno 48] Address already in use:
# netstat -vanp tcp | grep 32155
# see something like 
# tcp4       0      0  127.0.0.1.32155        *.*                    LISTEN
# 131072 131072  61432      0
# Then sudo kill -9 61432

