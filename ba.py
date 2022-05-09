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

    def exposed_order(self, command):
        self.p.command = command

    def exposed_get_order(self):
        # Faulty node will return randomly
        if self.p.faulty:
            if random.randint(0, 1) == 0:
                return "attack"

            else:
                return "retreat"

        # Non-faulty node will return truthfully
        else:
            # Wait until the command has been received
            while self.p.command == None:
                time.sleep(0.2)

            return self.p.command

    def exposed_get_majority(self):
        while self.p.majority == None:
            time.sleep(0.2)

        return self.p.majority

# Client for the general
class Process:
    def __init__(self, id, port):
        self.id = id
        self.port = port
        self.loop = None
        self.primary = False
        self.primary_port = -1
        self.faulty = False
        self.killed = False

        self.command = None
        self.majority = None

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

    def others(self):
        for p in others:
            if p == self.port:
                continue

            yield p

    # Election is for life, therefore only happens
    # when there is no leader currently.
    def elect(self):
        # See if this node has the lowest ID
        lowest = True
        for p in self.others():
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
            for p in self.others():
                try:
                    c = rpyc.connect("localhost", p)
                    c.root.new_leader(self.port)
                    c.close()

                except Exception:
                    pass

            self.primary = True

        else:
            self.primary = False

    def get_majority(self, command):
        n_attack = 0
        n_retreat = 0

        if command == "attack":
            n_attack += 1

        else:
            n_retreat += 1

        for p in self.others():
            # No need to ask the primary, we already have that info
            if p == self.primary_port:
                continue

            try:
                c = rpyc.connect("localhost", p)
                o = c.root.get_order()
                if o == "attack":
                    n_attack += 1

                else:
                    n_retreat += 1

                pass

            except Exception as e:
                pass

        if n_attack > n_retreat:
            self.majority = "attack"

        elif n_retreat > n_attack:
            self.majority = "retreat"

        else:
            self.majority = "undefined"

    def get_majorities(self, majority):
        n_attack = 0
        n_retreat = 0
        n_undefined = 0

        # Include itself in the query
        for p in others:
            try:
                c = rpyc.connect("localhost", p)
                m = c.root.get_majority()

                if m == "attack":
                    n_attack += 1

                elif m == "retreat":
                    n_retreat += 1

                else:
                    n_undefined += 1

                pass

            except Exception as e:
                # print(e)
                pass

        return (n_attack, n_retreat, n_undefined)

    def quorum(self, nr_faulty):
        n_attack, n_retreat, n_undefined = self.get_majorities(self.majority);
        total = n_attack + n_retreat + n_undefined
        k = (total - 1) // 3
        needed = 2 * k + 1 # (3k + 1) - k

        if total <= 3:
            needed = total - 1

        if total == 1:
            needed = 1

        quorum_text = f"{needed} out of {total} quorum suggests"
        quorum_fail = f"{n_undefined} out of {total} quorum not consistent"

        faulty_text = "Non-faulty nodes in the system"
        if nr_faulty > 0:
            faulty_text = f"{nr_faulty} faulty node(s) in the system"

        decision = ""

        if needed <= n_retreat:
            decision = f"retreat! {faulty_text} - {quorum_text} retreat"

        elif needed <= n_attack:
            decision = f"attack! {faulty_text} - {quorum_text} attack"

        else:
            decision = f"cannot be determined - not enough generals in the system! {faulty_text} - {quorum_fail}"

        print(f"Execute order: {decision}")

    # Should only be called by the primary
    def order(self, command):
        assert self.primary

        self.command = command

        for p in self.others():
            try:
                c = rpyc.connect("localhost", p)

                # Faulty leader will send randomly
                if self.faulty:
                    if random.randint(0, 1) == 0:
                        c.root.order("attack")

                    else:
                        c.root.order("retreat")

                # Non-faulty leader will send truthfully
                else:
                    c.root.order(command)

                c.close()

            except Exception as e:
                pass

        # Primary does not exchange info with other nodes
        self.majority = command

    def wait_majority(self):
        while self.majority == None:
            time.sleep(0.1)

    def clear(self):
        self.command = None
        self.majority = None

    def run(self):
        # Create an event loop for this thread
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)

        while True:
            time.sleep(0.1)

            if self.killed:
                _thread.exit()

            if not self.primary:
                # Check if primary is alive
                try:
                    c = rpyc.connect("localhost", self.primary_port)
                    c.close()

                except Exception:
                    # Start the election process
                    self.elect()


                # Get the majority from other nodes if command is received
                if self.command != None and self.majority == None:
                    self.get_majority(self.command)


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

        elif command == "actual-order":
            if len(cmd) == 1:
                continue

            # The first process in the processes list is always the leader
            processes[0].order(cmd[1])

            # List the status
            nr_faulty = 0
            for p in processes:
                p.wait_majority()
                status = "primary" if p.primary else "secondary"
                s = "F" if p.faulty else "NF"
                print(f"G{p.id}, {status}, majority={p.majority}, state={s}")

                if p.faulty:
                    nr_faulty += 1

            # Final decision
            processes[0].quorum(nr_faulty)

            # Clear the status
            for p in processes:
                p.clear()

        elif command == "g-state":
            if len(cmd) == 3:
                pid = id_to_index(processes, int(cmd[1]))
                if pid == None:
                    continue # TODO

                processes[pid].faulty = cmd[2] == "faulty"

            for p in processes:
                s = "F" if p.faulty else "NF"
                primarity = ", primary" if p.primary else ", secondary"
                primarity = primarity if len(cmd) != 3 else ""
                print(f"G{p.id}{primarity}, state={s}")

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
