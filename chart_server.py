import http.server
import urllib.parse
import json
from functools import partial

# Need to connect to PostgreSQL. Use psycopg2. 
# code sample: https://wiki.postgresql.org/wiki/Using_psycopg2_with_PostgreSQL
import psycopg2
import sys

def pg_connect():
    conn_string = "host='localhost' dbname='postgres' user='postgres' password='wunan'"
    print("Connecting to database\n ->%s" % (conn_string))
    # get a connection, if a connect cannot be made an exception will be raised here
    conn = psycopg2.connect(conn_string)
    # conn.cursor will return a cursor object, you can use this cursor to perform queries
    cursor = conn.cursor()
    print("Connected!\n")
    return cursor

def fetchdata(cursor, func, query_obj):
    records = cursor.execute("SELECT * FROM {}({})".format(func, ','.join(['{} := {}'.format(name, value) for name, value in query_obj])))
    return cursor.fetchall()
 
class ChartServerHandler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, cursor, *args, **kwargs):
        self.cursor = cursor
        super().__init__(*args, **kwargs)

    def do_GET(self):
        # if my SQL call contains a '.', then it will be in trouble. 
        if '.' in self.path:
            return http.server.SimpleHTTPRequestHandler.do_GET(self)
        # path like 'http://123.123.123.123:32155/get_coordinate_lines?width=300&height=150'
        qs = urllib.parse.urlparse(self.path)
        # qs like ParseResult(scheme='http', netloc='123.123.123.123:32155', path='/get_coordinate_lines', params='', query='width=300&height=150', fragment='')
        query_obj = urllib.parse.parse_qsl(qs.query) 
        # query_obj like [('width', '300'), ('height', '150')]
        func = qs.path[1:]
        ret = fetchdata(self.cursor, func, query_obj)
        # ret = {'width': 400}
        self.send_response(200)
        self.send_header('Content-type', 'application-json')
        self.end_headers()
        self.wfile.write(json.dumps(ret).encode())


def main():
    handler = partial(ChartServerHandler, pg_connect())
    server = http.server.HTTPServer(('localhost', 32155), handler)
    print('Starting server at port 32155')
    server.serve_forever()

if __name__ == '__main__':
    main()

# if ERROR:  OSError: [Errno 48] Address already in use:
# netstat -vanp tcp | grep 32155
# see something like 
# tcp4       0      0  127.0.0.1.32155        *.*                    LISTEN
# 131072 131072  61432      0
# Then sudo kill -9 61432

