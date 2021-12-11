import socket
import requests
import argparse
import threading
import time
import subprocess
import uuid
from typing import Optional, List, Tuple
from flask import Flask, jsonify, request


class Channel:
    def __init__(self, address):
        self.address = address
        self.hostname = socket.gethostname()

        # Performa a handshake, to notify the master node that this is available for work
        response = requests.post(
            f'{address}/register',
            json={
                'hostname': self.hostname,
            }
        )

        response.raise_for_status()

    def send(task, action, json):
        """Send the result of an action to the master node."""
        response = requests.post(
            f'{self.address}/{self.hostname}/{task}/{action}/result',
            json=json,
        )

        response.raise_for_status()

    def completed(self, task):
        """Notify the master node that we completed a task."""
        response = requests.post(
            f'{self.address}/{self.hostname}/{task}/done'
        )

        response.raise_for_status()

    def error(self, task, action, json):
        """Notify the master node that a task failed during execution."""
        response = requests.post(
            f'{self.address}/{self.hostname}/{task}/{action}/error',
            json=json,
        )

        response.raise_for_status()

    def poll(self) -> Optional['Task']:
        """Poll the master node to see if it has any more work available for us."""
        response = requests.post(
            f'{self.address}/{self.hostname}/poll'
        )

        response.raise_for_status()
        return self._parse_task(response.json())

    def _parse_task(self, content: dict) -> Optional['Task']:
        """Attempts to parse a response into a Task"""
        # {
        #   "task": str,
        #   "actions": [{
        #       "type": "powershell" | "shell" | "python",
        #       "source": str
        #   }]
        # }
        if 'task' not in content or 'actions' not in content:
            return None

        # From this point on, we assume that everything is correct
        name = content['task']
        actions = [
            Action(
                kind=action['type'],
                source=action['source']
            )
            for action in content['actions']
        ]

        return Task(name, actions)


class Action:
    def __init__(self, kind: str, source: str):
        self.kind = kind
        self.source = source

    def execute(self) -> Tuple[bool, dict]:
        pass

    @staticmethod
    def powershell(source: str):
        pass

    @staticmethod
    def shell(source: str):
        pass

    @staticmethod
    def python(source: str):
        file = temp_file(source, 'py')


class Task:
    def __init__(self, name: str, actions: List[Action]):
        self.name = name
        self.actions = actions


def temp_file(content, extension):
    """Write `content` to a temp file with a random name ending in `extension`, returning the file name of the generated file"""
    name = f'{uuid.uuid4().hex}.{extension}'

    with open(name, 'w+') as f:
        f.write(content)

    return name


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


parser = argparse.ArgumentParser()
parser.add_argument('--master', type=str, required=True,
                    help="Base address of master node")
parser.add_argument('--poll', type=int, required=False,
                    default=10_000, help="Polling interval used (in ms)")

args = parser.parse_args()

try:
    channel = Channel(args.master)
except Exception as e:
    print(f'Failed to establish connection to master node: {e}')
    exit(1)

while True:
    # Check if the master node has any
    task = channel.poll()

    # We will only wait in cases where there are no available tasks
    if task is None:
        time.sleep(args.poll / 1000)
        continue

    # Execute each action specified in the task,
    # short-circuiting if any of them fails
    for i, action in enumerate(task.actions):
        error, content = action.execute()

        if error:
            channel.error(task.name, i, content)
            break

        channel.send(i, content)

    # Notify the master node that we completed the task
    channel.completed(task.name)
