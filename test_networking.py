"""
Test script to verify networking framing and P2P connection logic headlessly.
"""
import time
import queue
import unittest
from p2p_chat import P2PNetworkManager, get_local_ip

class TestP2PNetworking(unittest.TestCase):
    def test_local_ip(self):
        ip = get_local_ip()
        print(f"Detected IP: {ip}")
        self.assertTrue(len(ip.split('.')) == 4)

    def test_p2p_chat_exchange(self):
        q1 = queue.Queue()
        q2 = queue.Queue()

        node1 = P2PNetworkManager(q1)
        node2 = P2PNetworkManager(q2)

        # Start listeners on ports 51001 and 51002
        ok1, msg1 = node1.start_listener(51001)
        ok2, msg2 = node2.start_listener(51002)

        self.assertTrue(ok1, f"Node 1 failed to start: {msg1}")
        self.assertTrue(ok2, f"Node 2 failed to start: {msg2}")

        time.sleep(0.1)

        # Node 1 sends message to Node 2
        node1.send_message_async("127.0.0.1", 51002, "Alice", "Hello Bob!")

        # Wait for delivery
        time.sleep(0.3)

        # Check Node 1 received success event
        events1 = []
        while not q1.empty():
            events1.append(q1.get_nowait())

        # Check Node 2 received chat_received event
        events2 = []
        while not q2.empty():
            events2.append(q2.get_nowait())

        node1.stop_listener()
        node2.stop_listener()

        print("Node 1 events:", events1)
        print("Node 2 events:", events2)

        # Verify chat sent on Node 1
        has_sent = any(e[0] == "chat_sent_success" and e[3] == "Hello Bob!" for e in events1)
        self.assertTrue(has_sent, "Node 1 should have confirmed chat_sent_success")

        # Verify chat received on Node 2
        has_recv = any(e[0] == "chat_received" and e[1] == "Alice" and e[3] == "Hello Bob!" for e in events2)
        self.assertTrue(has_recv, "Node 2 should have received chat from Alice")

    def test_ping_exchange(self):
        q1 = queue.Queue()
        q2 = queue.Queue()

        node1 = P2PNetworkManager(q1)
        node2 = P2PNetworkManager(q2)

        node1.start_listener(51003)
        node2.start_listener(51004)

        time.sleep(0.1)

        node1.send_ping_async("127.0.0.1", 51004, "Alice")
        time.sleep(0.3)

        events1 = []
        while not q1.empty():
            events1.append(q1.get_nowait())

        events2 = []
        while not q2.empty():
            events2.append(q2.get_nowait())

        node1.stop_listener()
        node2.stop_listener()

        print("Ping events 1:", events1)
        print("Ping events 2:", events2)

        has_ping_reply = any(e[0] == "system_msg" and "Ping to 127.0.0.1:51004 succeeded" in e[1] for e in events1)
        self.assertTrue(has_ping_reply, "Node 1 should receive ping success")

        has_ping_rx = any(e[0] == "ping_received" and e[1] == "Alice" for e in events2)
        self.assertTrue(has_ping_rx, "Node 2 should receive ping notification")

if __name__ == "__main__":
    unittest.main()
