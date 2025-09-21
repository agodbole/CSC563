import threading
import time
from xmlrpc.server import SimpleXMLRPCServer
import xmlrpc.client


class LamportClock:
    def __init__(self, port):
        self.port = port
        self.clock = 0
        self.lock = threading.Lock()

        # XML-RPC server setup
        self.server = SimpleXMLRPCServer(('localhost', self.port), allow_none=True)
        self.server.register_function(self.receive_message, "receive_message")

    def receive_message(self, message):
        with self.lock:
            old_clock = self.clock
            self.clock = max(self.clock, message['clock']) + 1
            print(f"Process {self.port}: Received from {message['sender_port']} with clock {message['clock']}, my clock was {old_clock}, clock is now {self.clock}")
        return True

    def _increment_clock(self):
        while True:
            with self.lock:
                self.clock += 1
                print(f"Process {self.port}: Internal event, clock is now {self.clock}")
            time.sleep(1)

    def send_message(self, remote_port):
        with xmlrpc.client.ServerProxy(f"http://localhost:{remote_port}/") as remote_server:
            with self.lock:
                message = {
                    'sender_port': self.port,
                    'clock': self.clock
                }
            try:
                remote_server.receive_message(message)
                print(f"Process {self.port}: Sent to {remote_port} with clock {self.clock}")
            except ConnectionRefusedError as ce:
                print(f"Process at {remote_port} is dead!!")


    def start(self):
        server_thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        increment_thread = threading.Thread(target=self._increment_clock, daemon=True)

        server_thread.start()
        increment_thread.start()

        print(f"Process on port {self.port} started...")


if __name__ == "__main__":
    lamport_process = LamportClock(8001)
    lamport_process.start()

    time.sleep(3)  # Wait for clocks to increment
    print("\n--- Sending message from 8001 to 8002. ---")
    lamport_process.send_message(8002)

    time.sleep(10)
    print("\n--- Sending message from 8001 to 8003. ---")
    lamport_process.send_message(8003)

    time.sleep(2)
    print("\n--- Sending message from 8001 to 8002. ---")
    lamport_process.send_message(8002)

