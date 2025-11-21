import http.server
import json
import os
import shlex
import socketserver
import subprocess
import logging
from typing import Dict
from urllib.parse import urlparse, parse_qs

PORT = 8081

logging.getLogger().setLevel(10)

class SimpleHTTPRequestHandler(http.server.SimpleHTTPRequestHandler):
    def do_GET(self):
        parsed_path = urlparse(self.path)
        query_params = parse_qs(parsed_path.query)

        response = f"<html><body>"
        response += f"<h1>Requested URI: {parsed_path.path}</h1>"
        response += f"<h2>HTTP Method: GET</h2>"
        response += f"<h3>Query Parameters: {query_params}</h3>"
        response += "</body></html>"

        self.reply(200, response)

    def do_POST(self):
        parsed_path = urlparse(self.path)
        if parsed_path.path != "/dqd-sdtmig-3.4":
            return self.reply(404, "Unknown route specified {}".format(parsed_path.path))
        query_params = parse_qs(parsed_path.query)
        id = str(query_params['id'][0])
        result_filename = "result-" + id + ".xlsx"
        log_filename = "var/dqd-sdtm-" + id + ".log"

        cmd = "bash -c 'rm -rf /data; mkdir /data; cp var/*.xpt /data; python3.12 /app/core.py validate -s sdtmig -v 3-4 -d /data -o /data/result-{id} -of XLSX; cp /data/result-{id}.xlsx var/' > {logs} 2>&1".format(id=id, logs=log_filename)
        logging.info(cmd)
        args = shlex.split(cmd)
        p = subprocess.run(args, capture_output=True)
        logs = str(p.stdout.decode('utf-8'))
        logging.info(logs)

        if p.returncode > 0:
            return self.reply(500, "Error running DQD:\n{}".format(logs))

        response: Dict[str, str] = {"result": result_filename, "log": log_filename}

        self.reply(200, json.dumps(response))

    def reply(self, code: int, response: str):
        self.send_response(code)
        self.send_header("Content-type", "text/html")
        self.end_headers()
        self.wfile.write(response.encode())

if __name__ == "__main__":
    with socketserver.TCPServer(("", PORT), SimpleHTTPRequestHandler) as httpd:
        logging.info(f"Serving on port {PORT}")
        httpd.serve_forever()
