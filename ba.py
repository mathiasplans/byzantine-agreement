import rpyc
from rpyc.utils.server import ThreadedServer
from rpyc.utils.helpers import classpartial
import datetime
import time
from functools import wraps
import random
import sys
import _thread
import asyncio

N = int(sys.argv[1])
others = []

p_upper = 5
cs_upper = 10

def parallel(f):
    def wrapped(*args, **kwargs):
        return asyncio.get_event_loop().run_in_executor(None, f, *args, **kwargs)

    return wrapped

# Server for the general
class Serv(rpyc.Service):
    def __init__(self, p):
        self.p = p
        pass

    def exposed_new_leader(self, leader_port):
        self.p.set_leader(leader_port)

    def exposed_get_id(self):
        return self.p.id

    def exposed_get_leader_port(self):
        return self.p.primary_port

# Client for the general
class Process:
    def __init__(self, id, port):
        self.id = id
        self.port = port
        self.lock = _thread.allocate_lock()
        self.loop = None
        self.primary = False
        self.primary_port = -1
        self.faulty = False
        self.killed = False

        partialserv = classpartial(Serv, self);
        self.ts = ThreadedServer(partialserv, port=port)

    def rpyc_start(self):
        self.ts.start()
        _thread.exit() # Will be called when ThreadServer exits

    def discover_leader(self):
        # If there are other processes, ask for the leader first
        for p in others:
            if p == self.port:
                continue

            try:
                c = rpyc.connect("localhost", p)
                self.primary_port = c.root.get_leader_port()
                c.close()

                if self.primary_port != -1:
                    break

            except Exception as e:
                # print(e)
                pass

    def start(self):
        self.discover_leader()

        _thread.start_new_thread(self.rpyc_start, ())
        _thread.start_new_thread(self.run, ())

    def kill(self):
        self.ts.close()
        self.killed = True

    def set_leader(self, leader_port):
        self.primary_port = leader_port

    # Election is for life, therefore only happens
    # when there is no leader currently.
    def elect(self):
        # See if this node has the lowest ID
        lowest = True
        for p in others:
            if p == self.port:
                continue

            try:
                c = rpyc.connect("localhost", p)
                other_id = c.root.get_id()
                c.close()

                if other_id < self.id:
                    lowest = False
                    break

            except Exception as e:
                # print(e)
                pass

        # If the election was won, let everyone know
        if lowest:
            for p in others:
                if p == self.port:
                    continue

                try:
                    c = rpyc.connect("localhost", p)
                    c.root.new_leader(self.port)
                    c.close()

                except Exception:
                    pass

            self.primary = True

        else:
            self.primary = False

    def run(self):
        # Create an event loop for this thread
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)

        while True:
            if self.killed:
                _thread.exit()

            if self.primary:
                time.sleep(1)

            else:
                time.sleep(0.1) # TODO

                # Check if primary is alive
                try:
                    c = rpyc.connect("localhost", self.primary_port)
                    c.close()

                except Exception:
                    # Start the election process
                    self.elect()


def id_to_index(processes, ID):
    last_i = -1

    lower = 0
    upper = len(processes) - 1
    i = upper // 2

    while last_i != i:
        last_i = i
        if processes[i].id == ID:
            return i

        elif processes[i].id > ID:
            upper = i - 1

        else:
            lower = i + 1

        i = lower + (upper - lower) // 2

    return None

def gen_processes(start_port):
    id = 1
    port = start_port

    while True:
        yield (port, Process(id, port))
        id += 1
        port += 1


if __name__ == '__main__':
    pgen = gen_processes(18812)
    processes = []
    for i in range(N):
        port, p = next(pgen)
        processes.append(p)
        others.append(port)

    for i in range(N):
        processes[i].start()

    running = True
    while running:
        command_string = input()

        cmd = command_string.split(" ")

        command = cmd[0]

        if command == "Exit":
            running = False

        elif command == "g-state":
            if len(cmd) == 3:
                pid = id_to_index(processes, int(cmd[1]))
                if pid == None:
                    continue # TODO

                processes[pid].faulty = cmd[2] == "faulty"

            for p in processes:
                s = "F" if p.faulty else "NF"
                print(f"G{p.id}, state={s}")

        elif command == "g-kill":
            if len(cmd) == 1:
                continue

            pid = id_to_index(processes, int(cmd[1]))
            if pid == None:
                continue # TODO

            processes[pid].kill()

            del processes[pid]

        elif command == "g-add":
            if len(cmd) == 1:
                continue

            n = int(cmd[1])

            for i in range(n):
                port, p = next(pgen)
                processes.append(p)
                others.append(port)
                p.start()

        elif command == "List":
            try:
                for p in processes:
                    #debug = f"[{p.lamport}, {p.wanted_ts}]" if False else ""
                    print(f"P{p.id}, {p.primary}")
            except:
                print("Error")
