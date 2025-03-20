import http.server
import json
import os
import shlex
import socketserver
import subprocess
import logging
from typing import Dict
from urllib.parse import urlparse, parse_qs

PORT = 8080

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
        if parsed_path.path != "/dqd-omop-5.4":
            return self.reply(404, "Unknown route specified {}".format(parsed_path.path))
        query_params = parse_qs(parsed_path.query)
        filename = '/data/' + str(query_params['file'][0])
        if not os.path.isfile(filename):
            return self.reply(404, "File {} not found".format(filename))
        id = str(query_params['id'][0])
        result_filename = "results-" + id + ".json"
        log_filename = "qc-" + id + ".log"

        cmd = "bash -c 'export LD_LIBRARY_PATH=/usr/lib/jvm/java-21-openjdk-amd64/lib/server/; cd /data/; Rscript /app/qc.r "+filename+" /data/ " + result_filename + " > " + log_filename + " 2>&1'"
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
        print(f"Serving on port {PORT}")
        # httpd.timeout = 1800    # timeout to generate results.json
        httpd.serve_forever()
