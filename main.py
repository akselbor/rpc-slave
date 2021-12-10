import socket
import requests
import argparse
import threading
import time
import subprocess
from flask import Flask, jsonify, request


def handshake(master_addr):
    requests.post(
        f'{master_addr}/register',
        json={
            'hostname': hostname,
        }
    )


def send(master_addr, task, action, json):
    requests.post(
        f'{master_addr}/{hostname}/{task}/{action}/result',
        json=json,
    )


def completed(master_addr, task):
    requests.post(
        f'{master_addr}/{hostname}/{task}/done'
    )


def error(master_addr, task, action, json):
    requests.post(
        f'{master_addr}/{hostname}/{task}/{action}/error',
        json=json,
    )


def execute_powershell(body):
    pass


def execute_shell(body):
    pass


def execute_python(body):
    pass


def execute(task, actions):
    for i, action in enumerate(actions):
        kind = action['type']
        body = action['source']
        try:
            result = {
                'powershell': execute_powershell,
                'shell': execute_shell,
                'python': execute_python
            }[kind](body)

            send(master, task, i, result)
        except Exception as e:
            error(master, task, i, jsonify(e))
            return

    completed(master, task)


global master
global secret


parser = argparse.ArgumentParser()
parser.add_argument('--master', type=str, required=True)

args = parser.parse_args()

app = Flask(__name__)


@app.route('/execute', methods=['POST'])
def receive():
    content = request.json

    # if content['secret'] != secret:
    #    raise ValueError('incorrect secret')

    task = content['task']
    actions = content['actions']

    if actions:
        threading.Thread(target=execute, args=(task, actions)).start()
        return '', 202
    else:
        return '', 500


threading.Thread(target=lambda: app.run(port=8080)).start()

time.sleep(5)

try:
    master = args.master
    hostname = socket.gethostname()
    secret = handshake(master)
except Exception as e:
    print(f'Failed to initialize: {e}')
    exit(1)
