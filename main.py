import socket
import requests
import argparse
import threading
import time
import subprocess
import uuid
import pip
from tqdm import tqdm
from typing import Optional, List, Tuple


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

    def send(self, task, action, json):
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
        if self.kind == 'python_eval':
            return Action.eval_python(self.source)

        if self.kind == 'python_exec':
            return Action.exec_python(self.source)

        if self.kind == 'requirements':
            return Action.live_install(self.source)

        if self.kind == 'powershell':
            return Action.powershell(self.source)

        if self.kind == 'shell':
            return Action.shell(self.source)

        if self.kind == 'python':
            return Action.python(self.source)

        return True, f"unkown type '{self.kind}'"

    @staticmethod
    def powershell(source: str):
        return True, 'Not yet implemented'

    @staticmethod
    def shell(source: str):
        try:
            subprocess.run(source, shell=True, check=True)
            return False, None
        except Exception as e:
            return False, e

    @staticmethod
    def python(source: str):
        return True, 'Not yet implemented'

    @staticmethod
    def eval_python(source: str):
        """Eval a python expression within the confinements of the running process"""
        try:
            return False, eval(source)
        except Exception as e:
            return True, e

    @staticmethod
    def exec_python(source: str):
        """Executes a python statement or program, returning None"""
        try:
            return False, exec(source)
        except Exception as e:
            return True, e

    @staticmethod
    def live_install(source: str):
        requirements = source.splitlines()
        code = pip.main(['install', *requirements])
        print(f'pip code = {code}')
        return code != 0, ''


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


def loop():
    """A generator that always returns None"""
    while True:
        yield None


it = tqdm(loop(), position=0, leave=False)
for _ in it:
    # Check if the master node has any
    task = channel.poll()
    # We will only wait in cases where there are no available tasks
    if task is None:
        it.set_description('<awaiting new task>')
        time.sleep(args.poll / 1000)
        continue

    it.set_description(f'{task.name}')
    # Execute each action specified in the task,
    # short-circuiting if any of them fails
    for i, action in tqdm(list(enumerate(task.actions)), position=1, leave=False):
        print(f'action {i}')
        error, content = action.execute()
        print(error)
        print(content)

        if error:
            channel.error(task.name, i, content)
            break

        channel.send(task.name, i, content)

    # Notify the master node that we completed the task
    channel.completed(task.name)
