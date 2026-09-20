"""
Modern Cross-Platform Peer-to-Peer (P2P) Desktop Chat Application in Python.
Uses CustomTkinter (with native Tkinter fallback) and raw Python TCP socket networking.
"""

import os
import sys
import time
import json
import struct
import socket
import select
import queue
import threading
from datetime import datetime

# Attempt to import CustomTkinter, fall back gracefully to Tkinter if unavailable
try:
    import customtkinter as ctk
    CTK_AVAILABLE = True
except ImportError:
    CTK_AVAILABLE = False
    import tkinter as tk
    from tkinter import ttk, messagebox


# ----------------------------------------------------------------------
# Network Protocol Helpers (Length-Prefixed UTF-8 JSON)
# ----------------------------------------------------------------------

def get_local_ip() -> str:
    """
    Detect the machine's primary local LAN IP address using a non-blocking
    outbound UDP socket probe to a public DNS server (no packets sent).
    """
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.settimeout(0.5)
            # Connecting to a public IP triggers OS routing table lookup
            s.connect(("8.8.8.8", 80))
            return s.getsockname()[0]
    except Exception:
        try:
            return socket.gethostbyname(socket.gethostname())
        except Exception:
            return "127.0.0.1"


def send_framed_json(sock: socket.socket, payload: dict) -> None:
    """
    Encode a dictionary as UTF-8 JSON, prefix with 4-byte big-endian length header,
    and send entirely across the TCP socket.
    """
    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    header = struct.pack("!I", len(data))
    sock.sendall(header + data)


def recv_exact(sock: socket.socket, n_bytes: int) -> bytes:
    """
    Read exactly n_bytes from a TCP socket or raise ConnectionResetError.
    Handles TCP fragmentation across packet boundaries.
    """
    buf = bytearray()
    while len(buf) < n_bytes:
        chunk = sock.recv(n_bytes - len(buf))
        if not chunk:
            raise ConnectionResetError("Connection closed before all bytes were received.")
        buf.extend(chunk)
    return bytes(buf)


def recv_framed_json(sock: socket.socket) -> dict:
    """
    Read a 4-byte length-prefixed JSON frame from a TCP socket.
    """
    header = recv_exact(sock, 4)
    length = struct.unpack("!I", header)[0]
    if length > 10 * 1024 * 1024:  # 10MB safety sanity limit
        raise ValueError(f"Payload size {length} bytes exceeds maximum allowed limit.")
    raw_json = recv_exact(sock, length)
    return json.loads(raw_json.decode("utf-8"))


# ----------------------------------------------------------------------
# Backend Network Listener & Transmitter Manager
# ----------------------------------------------------------------------

class P2PNetworkManager:
    """
    Handles TCP socket listening (daemon thread) and non-blocking outbound
    transmissions with timeout and framing safety.
    """

    def __init__(self, event_queue: queue.Queue):
        self.event_queue = event_queue
        self.listener_socket = None
        self.listener_thread = None
        self.is_listening = False
        self._stop_event = threading.Event()
        self.active_port = 50050

    def start_listener(self, port: int) -> tuple[bool, str]:
        """
        Start or restart the background TCP listener on 0.0.0.0 at the specified port.
        """
        self.stop_listener()
        self._stop_event.clear()

        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            # Allow immediate re-binding without waiting for TIME_WAIT
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind(("0.0.0.0", port))
            sock.listen(10)
            sock.settimeout(0.5)  # Short timeout for loop checking _stop_event

            self.listener_socket = sock
            self.active_port = port
            self.is_listening = True

            self.listener_thread = threading.Thread(
                target=self._listen_loop,
                daemon=True,
                name="P2P-ListenerThread"
            )
            self.listener_thread.start()

            return True, f"Listening on port {port}"
        except Exception as ex:
            self.is_listening = False
            return False, str(ex)

    def stop_listener(self) -> None:
        """
        Gracefully stop the background TCP listener.
        """
        self.is_listening = False
        self._stop_event.set()
        if self.listener_socket:
            try:
                self.listener_socket.close()
            except Exception:
                pass
            self.listener_socket = None

        if self.listener_thread and self.listener_thread.is_alive():
            self.listener_thread.join(timeout=1.0)
            self.listener_thread = None

    def _listen_loop(self) -> None:
        """
        Background listener loop accepting incoming peer connections.
        """
        while not self._stop_event.is_set():
            try:
                client_sock, client_addr = self.listener_socket.accept()
            except socket.timeout:
                continue
            except OSError:
                # Socket was closed
                break
            except Exception as ex:
                if not self._stop_event.is_set():
                    self.event_queue.put(("system_msg", f"Listener error: {ex}", "error"))
                break

            # Handle connection in a worker thread to keep listener receptive
            handler_thread = threading.Thread(
                target=self._handle_client,
                args=(client_sock, client_addr),
                daemon=True
            )
            handler_thread.start()

    def _handle_client(self, client_sock: socket.socket, client_addr: tuple) -> None:
        """
        Processes an incoming TCP peer connection.
        """
        client_sock.settimeout(3.0)
        try:
            payload = recv_framed_json(client_sock)
            msg_type = payload.get("type", "chat")

            if msg_type == "ping":
                # Respond with ACK
                ack_payload = {
                    "type": "ack",
                    "sender": "System",
                    "timestamp": datetime.now().strftime("%H:%M:%S"),
                    "text": f"Pong! Listener at {client_sock.getsockname()[0]}:{self.active_port} is active."
                }
                try:
                    send_framed_json(client_sock, ack_payload)
                except Exception:
                    pass
                self.event_queue.put((
                    "ping_received",
                    payload.get("sender", client_addr[0]),
                    payload.get("timestamp", datetime.now().strftime("%H:%M:%S")),
                    client_addr
                ))

            elif msg_type == "chat":
                # Send optional ACK before queueing
                ack_payload = {
                    "type": "ack",
                    "sender": "System",
                    "timestamp": datetime.now().strftime("%H:%M:%S"),
                    "text": "Delivered"
                }
                try:
                    send_framed_json(client_sock, ack_payload)
                except Exception:
                    pass

                self.event_queue.put((
                    "chat_received",
                    payload.get("sender", "Peer"),
                    payload.get("timestamp", datetime.now().strftime("%H:%M:%S")),
                    payload.get("text", "")
                ))

            elif msg_type == "ack":
                self.event_queue.put((
                    "ack_received",
                    payload.get("sender", "Peer"),
                    payload.get("timestamp", datetime.now().strftime("%H:%M:%S")),
                    payload.get("text", "ACK")
                ))

        except Exception as ex:
            # Silently ignore broken handshake or log if useful
            pass
        finally:
            try:
                client_sock.close()
            except Exception:
                pass

    def send_message_async(self, target_ip: str, target_port: int, nickname: str, text: str) -> None:
        """
        Dispatches a chat message to the peer in a non-blocking background thread.
        """
        payload = {
            "type": "chat",
            "sender": nickname,
            "timestamp": datetime.now().strftime("%H:%M:%S"),
            "text": text
        }
        threading.Thread(
            target=self._transmit_payload,
            args=(target_ip, target_port, payload, "chat"),
            daemon=True
        ).start()

    def send_ping_async(self, target_ip: str, target_port: int, nickname: str) -> None:
        """
        Dispatches a reachability ping to the peer in a non-blocking background thread.
        """
        payload = {
            "type": "ping",
            "sender": nickname,
            "timestamp": datetime.now().strftime("%H:%M:%S"),
            "text": "Ping"
        }
        threading.Thread(
            target=self._transmit_payload,
            args=(target_ip, target_port, payload, "ping"),
            daemon=True
        ).start()

    def _transmit_payload(self, target_ip: str, target_port: int, payload: dict, operation_type: str) -> None:
        """
        Connects via TCP with a 2.5-second timeout, transmits framed payload,
        and reports outcome back to UI event queue.
        """
        start_time = time.time()
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.settimeout(2.5)
                sock.connect((target_ip, target_port))
                send_framed_json(sock, payload)

                # Wait briefly for optional ACK
                ack_received = False
                ack_info = ""
                try:
                    ack_data = recv_framed_json(sock)
                    if ack_data.get("type") == "ack":
                        ack_received = True
                        ack_info = ack_data.get("text", "")
                except Exception:
                    pass

                latency_ms = int((time.time() - start_time) * 1000)

                if operation_type == "chat":
                    self.event_queue.put((
                        "chat_sent_success",
                        payload["sender"],
                        payload["timestamp"],
                        payload["text"]
                    ))
                elif operation_type == "ping":
                    self.event_queue.put((
                        "system_msg",
                        f"Ping to {target_ip}:{target_port} succeeded ({latency_ms}ms). Response: {ack_info or 'OK'}",
                        "success"
                    ))

        except socket.timeout:
            self.event_queue.put((
                "system_msg",
                f"Failed to reach peer at {target_ip}:{target_port} - Connection timed out (peer may be offline or port blocked).",
                "error"
            ))
        except ConnectionRefusedError:
            self.event_queue.put((
                "system_msg",
                f"Failed to reach peer at {target_ip}:{target_port} - Connection refused (peer is not listening on this port).",
                "error"
            ))
        except socket.gaierror:
            self.event_queue.put((
                "system_msg",
                f"Invalid IP address format: '{target_ip}'",
                "error"
            ))
        except Exception as ex:
            self.event_queue.put((
                "system_msg",
                f"Delivery error to {target_ip}:{target_port} - {type(ex).__name__}: {ex}",
                "error"
            ))


# ----------------------------------------------------------------------
# Modern CustomTkinter GUI Implementation
# ----------------------------------------------------------------------

if CTK_AVAILABLE:
    class ModernP2PChatApp(ctk.CTk):
        def __init__(self):
            super().__init__()

            # App Theme & Window Configuration
            ctk.set_appearance_mode("Dark")
            ctk.set_default_color_theme("blue")
            self.title("P2P Direct Socket Chat")
            self.geometry("900x720")
            self.minsize(760, 580)

            # Colors & Styles
            self.COLOR_SENT_BG = "#1e40af"       # Indigo/Blue bubble
            self.COLOR_RECV_BG = "#27272a"       # Sleek Zinc bubble
            self.COLOR_PANEL_BG = "#18181b"      # Surface panel
            self.COLOR_CARD_BG = "#27272a"       # Inner sub-card
            self.COLOR_SYS_TEXT = "#a1a1aa"      # System gray
            self.COLOR_SUCCESS = "#22c55e"       # Green
            self.COLOR_ERROR = "#ef4444"         # Red
            self.COLOR_ACCENT = "#3b82f6"        # Vibrant Blue

            # Networking State
            self.local_ip = get_local_ip()
            self.ui_queue = queue.Queue()
            self.net_manager = P2PNetworkManager(self.ui_queue)

            # Build UI Components
            self._build_layout()

            # Start event queue consumer
            self.after(50, self._process_ui_queue)

            # Auto-start listener on default port
            self._start_listener_from_ui()

            # Bind window close cleanup
            self.protocol("WM_DELETE_WINDOW", self._on_close)

        def _build_layout(self):
            self.grid_columnconfigure(0, weight=1)
            self.grid_rowconfigure(1, weight=1)

            # ---------------- Top Dashboard Panel ----------------
            self.top_panel = ctk.CTkFrame(self, corner_radius=12, fg_color=self.COLOR_PANEL_BG)
            self.top_panel.grid(row=0, column=0, padx=16, pady=(16, 8), sticky="nsew")
            self.top_panel.grid_columnconfigure(0, weight=1)
            self.top_panel.grid_columnconfigure(1, weight=1)

            # Header Title & Status
            header_row = ctk.CTkFrame(self.top_panel, fg_color="transparent")
            header_row.grid(row=0, column=0, columnspan=2, padx=16, pady=(12, 6), sticky="ew")
            
            title_lbl = ctk.CTkLabel(
                header_row,
                text="⚡ P2P Direct Mesh Chat",
                font=ctk.CTkFont(size=18, weight="bold")
            )
            title_lbl.pack(side="left")

            self.status_badge = ctk.CTkLabel(
                header_row,
                text="● Listener Inactive",
                text_color=self.COLOR_ERROR,
                font=ctk.CTkFont(size=13, weight="bold")
            )
            self.status_badge.pack(side="right")

            # Local Node Card (Left)
            local_card = ctk.CTkFrame(self.top_panel, corner_radius=10, fg_color=self.COLOR_CARD_BG)
            local_card.grid(row=1, column=0, padx=(14, 7), pady=(0, 14), sticky="nsew")

            lbl_local_title = ctk.CTkLabel(
                local_card,
                text="My Node Configuration",
                font=ctk.CTkFont(size=12, weight="bold"),
                text_color=self.COLOR_ACCENT
            )
            lbl_local_title.grid(row=0, column=0, columnspan=3, padx=12, pady=(8, 4), sticky="w")

            # Local IP Display
            lbl_ip_title = ctk.CTkLabel(local_card, text="Local IP:", font=ctk.CTkFont(size=12))
            lbl_ip_title.grid(row=1, column=0, padx=(12, 4), pady=4, sticky="w")

            self.entry_local_ip = ctk.CTkEntry(local_card, height=28, font=ctk.CTkFont(size=12))
            self.entry_local_ip.insert(0, self.local_ip)
            self.entry_local_ip.configure(state="readonly")
            self.entry_local_ip.grid(row=1, column=1, padx=4, pady=4, sticky="ew")

            self.btn_copy_ip = ctk.CTkButton(
                local_card,
                text="Copy",
                width=55,
                height=28,
                command=self._copy_ip_to_clipboard
            )
            self.btn_copy_ip.grid(row=1, column=2, padx=(4, 12), pady=4)

            # Local Port & Listener Toggle
            lbl_port_title = ctk.CTkLabel(local_card, text="My Port:", font=ctk.CTkFont(size=12))
            lbl_port_title.grid(row=2, column=0, padx=(12, 4), pady=(4, 10), sticky="w")

            self.entry_local_port = ctk.CTkEntry(local_card, height=28, font=ctk.CTkFont(size=12), width=90)
            self.entry_local_port.insert(0, "50050")
            self.entry_local_port.grid(row=2, column=1, padx=4, pady=(4, 10), sticky="w")

            self.btn_toggle_listen = ctk.CTkButton(
                local_card,
                text="Restart Port",
                width=100,
                height=28,
                fg_color="#059669",
                hover_color="#047857",
                command=self._start_listener_from_ui
            )
            self.btn_toggle_listen.grid(row=2, column=2, padx=(4, 12), pady=(4, 10))

            local_card.grid_columnconfigure(1, weight=1)

            # Target Peer Card (Right)
            target_card = ctk.CTkFrame(self.top_panel, corner_radius=10, fg_color=self.COLOR_CARD_BG)
            target_card.grid(row=1, column=1, padx=(7, 14), pady=(0, 14), sticky="nsew")

            lbl_target_title = ctk.CTkLabel(
                target_card,
                text="Target Peer & Identity",
                font=ctk.CTkFont(size=12, weight="bold"),
                text_color=self.COLOR_ACCENT
            )
            lbl_target_title.grid(row=0, column=0, columnspan=4, padx=12, pady=(8, 4), sticky="w")

            # Nickname Entry
            lbl_nick = ctk.CTkLabel(target_card, text="Nickname:", font=ctk.CTkFont(size=12))
            lbl_nick.grid(row=1, column=0, padx=(12, 4), pady=4, sticky="w")

            self.entry_nick = ctk.CTkEntry(target_card, height=28, font=ctk.CTkFont(size=12))
            self.entry_nick.insert(0, f"User_{self.local_ip.split('.')[-1]}")
            self.entry_nick.grid(row=1, column=1, columnspan=2, padx=4, pady=4, sticky="ew")

            self.btn_ping = ctk.CTkButton(
                target_card,
                text="📡 Ping",
                width=75,
                height=28,
                fg_color="#4f46e5",
                hover_color="#4338ca",
                command=self._send_ping
            )
            self.btn_ping.grid(row=1, column=3, padx=(4, 12), pady=4)

            # Target IP & Port
            lbl_target_ip = ctk.CTkLabel(target_card, text="Peer IP:", font=ctk.CTkFont(size=12))
            lbl_target_ip.grid(row=2, column=0, padx=(12, 4), pady=(4, 10), sticky="w")

            self.entry_target_ip = ctk.CTkEntry(target_card, height=28, font=ctk.CTkFont(size=12), placeholder_text="127.0.0.1 or LAN IP")
            self.entry_target_ip.insert(0, "127.0.0.1")
            self.entry_target_ip.grid(row=2, column=1, padx=4, pady=(4, 10), sticky="ew")

            lbl_target_port = ctk.CTkLabel(target_card, text="Port:", font=ctk.CTkFont(size=12))
            lbl_target_port.grid(row=2, column=2, padx=(6, 2), pady=(4, 10), sticky="w")

            self.entry_target_port = ctk.CTkEntry(target_card, height=28, font=ctk.CTkFont(size=12), width=75, placeholder_text="50051")
            self.entry_target_port.insert(0, "50051")
            self.entry_target_port.grid(row=2, column=3, padx=(2, 12), pady=(4, 10), sticky="w")

            target_card.grid_columnconfigure(1, weight=1)

            # ---------------- Central Chat Stream ----------------
            self.chat_frame = ctk.CTkScrollableFrame(self, corner_radius=12, fg_color=self.COLOR_PANEL_BG)
            self.chat_frame.grid(row=1, column=0, padx=16, pady=4, sticky="nsew")
            self.chat_frame.grid_columnconfigure(0, weight=1)

            # ---------------- Bottom Message Bar ----------------
            self.bottom_bar = ctk.CTkFrame(self, corner_radius=12, fg_color=self.COLOR_PANEL_BG)
            self.bottom_bar.grid(row=2, column=0, padx=16, pady=(8, 16), sticky="ew")
            self.bottom_bar.grid_columnconfigure(0, weight=1)

            self.entry_msg = ctk.CTkEntry(
                self.bottom_bar,
                placeholder_text="Type a message... (Press Enter to send)",
                height=42,
                font=ctk.CTkFont(size=13),
                corner_radius=8
            )
            self.entry_msg.grid(row=0, column=0, padx=(12, 8), pady=10, sticky="ew")
            self.entry_msg.bind("<Return>", lambda event: self._send_chat_message())

            self.btn_send = ctk.CTkButton(
                self.bottom_bar,
                text="Send ➤",
                width=100,
                height=42,
                font=ctk.CTkFont(size=13, weight="bold"),
                corner_radius=8,
                command=self._send_chat_message
            )
            self.btn_send.grid(row=0, column=1, padx=(0, 12), pady=10)

        def _copy_ip_to_clipboard(self):
            self.clipboard_clear()
            self.clipboard_append(self.local_ip)
            self.btn_copy_ip.configure(text="Copied!")
            self.after(1500, lambda: self.btn_copy_ip.configure(text="Copy"))

        def _start_listener_from_ui(self):
            port_str = self.entry_local_port.get().strip()
            try:
                port = int(port_str)
                if not (1024 <= port <= 65535):
                    raise ValueError("Port must be between 1024 and 65535")
            except ValueError as ve:
                self._render_system_bubble(f"Invalid listener port '{port_str}': {ve}", "error")
                return

            success, msg = self.net_manager.start_listener(port)
            if success:
                self.status_badge.configure(
                    text=f"🟢 Listening on :{port}",
                    text_color=self.COLOR_SUCCESS
                )
                self._render_system_bubble(f"Listener successfully active on 0.0.0.0:{port}", "success")
            else:
                self.status_badge.configure(
                    text="🔴 Listener Error",
                    text_color=self.COLOR_ERROR
                )
                self._render_system_bubble(f"Failed to bind listener on port {port}: {msg}", "error")

        def _get_target_info(self) -> tuple[str, int, str]:
            target_ip = self.entry_target_ip.get().strip()
            target_port_str = self.entry_target_port.get().strip()
            nickname = self.entry_nick.get().strip() or "Anonymous"

            if not target_ip:
                raise ValueError("Target IP cannot be empty.")
            try:
                target_port = int(target_port_str)
                if not (1 <= target_port <= 65535):
                    raise ValueError("Target port must be between 1 and 65535.")
            except ValueError:
                raise ValueError(f"Invalid target port '{target_port_str}'.")

            return target_ip, target_port, nickname

        def _send_ping(self):
            try:
                target_ip, target_port, nickname = self._get_target_info()
            except ValueError as ex:
                self._render_system_bubble(str(ex), "error")
                return

            self._render_system_bubble(f"Pinging {target_ip}:{target_port}...", "info")
            self.net_manager.send_ping_async(target_ip, target_port, nickname)

        def _send_chat_message(self):
            text = self.entry_msg.get().strip()
            if not text:
                return

            try:
                target_ip, target_port, nickname = self._get_target_info()
            except ValueError as ex:
                self._render_system_bubble(str(ex), "error")
                return

            self.entry_msg.delete(0, "end")
            self.net_manager.send_message_async(target_ip, target_port, nickname, text)

        # ---------------- Chat Bubble Rendering ----------------

        def _render_chat_bubble(self, sender: str, timestamp: str, text: str, is_me: bool):
            """
            Renders a sleek message bubble in the scrollable chat frame.
            """
            container = ctk.CTkFrame(self.chat_frame, fg_color="transparent")
            container.pack(fill="x", padx=8, pady=4)

            # Sent messages right-aligned, received messages left-aligned
            align_side = "right" if is_me else "left"
            bg_color = self.COLOR_SENT_BG if is_me else self.COLOR_RECV_BG

            bubble = ctk.CTkFrame(container, corner_radius=12, fg_color=bg_color)
            bubble.pack(side=align_side, anchor="e" if is_me else "w")

            # Header inside bubble: Name and Timestamp
            header_frame = ctk.CTkFrame(bubble, fg_color="transparent")
            header_frame.pack(fill="x", padx=12, pady=(6, 2))

            name_color = "#93c5fd" if is_me else "#38bdf8"
            lbl_name = ctk.CTkLabel(
                header_frame,
                text="You" if is_me else sender,
                font=ctk.CTkFont(size=11, weight="bold"),
                text_color=name_color
            )
            lbl_name.pack(side="left")

            lbl_time = ctk.CTkLabel(
                header_frame,
                text=f"  {timestamp}",
                font=ctk.CTkFont(size=10),
                text_color="#94a3b8"
            )
            lbl_time.pack(side="left")

            # Message content
            lbl_content = ctk.CTkLabel(
                bubble,
                text=text,
                font=ctk.CTkFont(size=13),
                wraplength=520,
                justify="left"
            )
            lbl_content.pack(padx=12, pady=(0, 8), anchor="w")

            self._scroll_to_bottom()

        def _render_system_bubble(self, text: str, level: str = "info"):
            """
            Renders a centered system status notification or error alert.
            """
            container = ctk.CTkFrame(self.chat_frame, fg_color="transparent")
            container.pack(fill="x", padx=8, pady=3)

            pill_bg = "#27272a"
            if level == "error":
                pill_color = "#f87171"
            elif level == "success":
                pill_color = "#4ade80"
            else:
                pill_color = "#94a3b8"

            pill = ctk.CTkFrame(container, corner_radius=10, fg_color=pill_bg)
            pill.pack(side="top", anchor="center", pady=2)

            lbl = ctk.CTkLabel(
                pill,
                text=f"ℹ {text}",
                font=ctk.CTkFont(size=11, slant="italic"),
                text_color=pill_color,
                wraplength=650,
                justify="center"
            )
            lbl.pack(padx=14, pady=4)

            self._scroll_to_bottom()

        def _scroll_to_bottom(self):
            # Allow geometry updates then scroll
            self.chat_frame._parent_canvas.yview_moveto(1.0)
            self.after(20, lambda: self.chat_frame._parent_canvas.yview_moveto(1.0))

        # ---------------- Thread-Safe Queue Consumer ----------------

        def _process_ui_queue(self):
            """
            Drains the thread-safe event queue to update the UI on Tkinter's main loop.
            """
            while not self.ui_queue.empty():
                try:
                    event = self.ui_queue.get_nowait()
                    event_type = event[0]

                    if event_type == "chat_sent_success":
                        _, sender, timestamp, text = event
                        self._render_chat_bubble(sender, timestamp, text, is_me=True)

                    elif event_type == "chat_received":
                        _, sender, timestamp, text = event
                        self._render_chat_bubble(sender, timestamp, text, is_me=False)

                    elif event_type == "system_msg":
                        _, msg, level = event
                        self._render_system_bubble(msg, level)

                    elif event_type == "ping_received":
                        _, sender, timestamp, client_addr = event
                        self._render_system_bubble(
                            f"Received Ping probe from {sender} ({client_addr[0]}:{client_addr[1]}) at {timestamp}",
                            "info"
                        )

                    elif event_type == "ack_received":
                        _, sender, timestamp, text = event
                        self._render_system_bubble(f"Peer ACK received from {sender}: {text}", "success")

                except queue.Empty:
                    break
                except Exception as ex:
                    print(f"Error processing UI event: {ex}", file=sys.stderr)

            # Schedule next poll
            self.after(50, self._process_ui_queue)

        def _on_close(self):
            """
            Window close cleanup handler.
            """
            self.net_manager.stop_listener()
            self.destroy()


# ----------------------------------------------------------------------
# Fallback Standard Tkinter GUI Implementation
# ----------------------------------------------------------------------

else:
    class FallbackTkinterP2PChatApp(tk.Tk):
        def __init__(self):
            super().__init__()
            self.title("P2P Direct Socket Chat (Standard Tkinter)")
            self.geometry("820x650")
            self.minsize(700, 500)
            self.configure(bg="#1e1e2e")

            self.local_ip = get_local_ip()
            self.ui_queue = queue.Queue()
            self.net_manager = P2PNetworkManager(self.ui_queue)

            self._build_layout()
            self.after(50, self._process_ui_queue)
            self._start_listener_from_ui()
            self.protocol("WM_DELETE_WINDOW", self._on_close)

        def _build_layout(self):
            # Top Panel
            top_frame = tk.Frame(self, bg="#2b2b3b", padx=10, pady=10)
            top_frame.pack(fill="x", padx=10, pady=10)

            title_lbl = tk.Label(top_frame, text="⚡ P2P Direct Mesh Chat", font=("Arial", 14, "bold"), fg="#ffffff", bg="#2b2b3b")
            title_lbl.pack(anchor="w")

            self.status_lbl = tk.Label(top_frame, text="● Listener Inactive", font=("Arial", 10, "bold"), fg="#ff5555", bg="#2b2b3b")
            self.status_lbl.pack(anchor="e")

            # Config Grid
            grid_frame = tk.Frame(top_frame, bg="#2b2b3b")
            grid_frame.pack(fill="x", pady=6)

            tk.Label(grid_frame, text="My IP:", fg="#ffffff", bg="#2b2b3b").grid(row=0, column=0, sticky="w", padx=4, pady=2)
            self.entry_local_ip = tk.Entry(grid_frame, width=15)
            self.entry_local_ip.insert(0, self.local_ip)
            self.entry_local_ip.configure(state="readonly")
            self.entry_local_ip.grid(row=0, column=1, padx=4, pady=2)

            tk.Label(grid_frame, text="My Port:", fg="#ffffff", bg="#2b2b3b").grid(row=0, column=2, sticky="w", padx=4, pady=2)
            self.entry_local_port = tk.Entry(grid_frame, width=8)
            self.entry_local_port.insert(0, "50050")
            self.entry_local_port.grid(row=0, column=3, padx=4, pady=2)

            btn_listen = tk.Button(grid_frame, text="Restart Port", command=self._start_listener_from_ui, bg="#059669", fg="#ffffff")
            btn_listen.grid(row=0, column=4, padx=6, pady=2)

            # Target Info
            tk.Label(grid_frame, text="Nickname:", fg="#ffffff", bg="#2b2b3b").grid(row=1, column=0, sticky="w", padx=4, pady=2)
            self.entry_nick = tk.Entry(grid_frame, width=15)
            self.entry_nick.insert(0, f"User_{self.local_ip.split('.')[-1]}")
            self.entry_nick.grid(row=1, column=1, padx=4, pady=2)

            tk.Label(grid_frame, text="Peer IP/Port:", fg="#ffffff", bg="#2b2b3b").grid(row=1, column=2, sticky="w", padx=4, pady=2)
            self.entry_target_ip = tk.Entry(grid_frame, width=14)
            self.entry_target_ip.insert(0, "127.0.0.1")
            self.entry_target_ip.grid(row=1, column=3, padx=2, pady=2)

            self.entry_target_port = tk.Entry(grid_frame, width=6)
            self.entry_target_port.insert(0, "50051")
            self.entry_target_port.grid(row=1, column=4, padx=2, pady=2)

            btn_ping = tk.Button(grid_frame, text="📡 Ping", command=self._send_ping, bg="#4f46e5", fg="#ffffff")
            btn_ping.grid(row=1, column=5, padx=6, pady=2)

            # Chat Display
            self.chat_text = tk.Text(self, bg="#181825", fg="#cdd6f4", font=("Consolas", 11), wrap="word", padx=8, pady=8)
            self.chat_text.pack(fill="both", expand=True, padx=10, pady=5)
            self.chat_text.tag_configure("me", foreground="#89b4fa", justify="right")
            self.chat_text.tag_configure("peer", foreground="#a6e3a1", justify="left")
            self.chat_text.tag_configure("system", foreground="#f38ba8", font=("Consolas", 10, "italic"), justify="center")
            self.chat_text.tag_configure("sys_success", foreground="#a6e3a1", font=("Consolas", 10, "italic"), justify="center")
            self.chat_text.configure(state="disabled")

            # Input Bottom Bar
            bottom_frame = tk.Frame(self, bg="#2b2b3b", padx=10, pady=8)
            bottom_frame.pack(fill="x", padx=10, pady=10)

            self.entry_msg = tk.Entry(bottom_frame, font=("Arial", 11))
            self.entry_msg.pack(side="left", fill="x", expand=True, padx=(0, 10))
            self.entry_msg.bind("<Return>", lambda e: self._send_chat_message())

            btn_send = tk.Button(bottom_frame, text="Send", width=10, bg="#3b82f6", fg="#ffffff", command=self._send_chat_message)
            btn_send.pack(side="right")

        def _start_listener_from_ui(self):
            try:
                port = int(self.entry_local_port.get().strip())
                success, msg = self.net_manager.start_listener(port)
                if success:
                    self.status_lbl.configure(text=f"🟢 Listening on :{port}", fg="#4ade80")
                    self._append_chat(f"Listening on port {port}", "sys_success")
                else:
                    self.status_lbl.configure(text="🔴 Listener Error", fg="#ff5555")
                    self._append_chat(f"Listener error: {msg}", "system")
            except Exception as e:
                self._append_chat(f"Invalid port: {e}", "system")

        def _send_ping(self):
            try:
                tip = self.entry_target_ip.get().strip()
                tport = int(self.entry_target_port.get().strip())
                nick = self.entry_nick.get().strip() or "User"
                self._append_chat(f"Pinging {tip}:{tport}...", "system")
                self.net_manager.send_ping_async(tip, tport, nick)
            except Exception as e:
                self._append_chat(f"Ping config error: {e}", "system")

        def _send_chat_message(self):
            text = self.entry_msg.get().strip()
            if not text:
                return
            try:
                tip = self.entry_target_ip.get().strip()
                tport = int(self.entry_target_port.get().strip())
                nick = self.entry_nick.get().strip() or "User"
                self.entry_msg.delete(0, "end")
                self.net_manager.send_message_async(tip, tport, nick, text)
            except Exception as e:
                self._append_chat(f"Error: {e}", "system")

        def _append_chat(self, text: str, tag: str):
            self.chat_text.configure(state="normal")
            self.chat_text.insert("end", text + "\n\n", tag)
            self.chat_text.configure(state="disabled")
            self.chat_text.see("end")

        def _process_ui_queue(self):
            while not self.ui_queue.empty():
                try:
                    event = self.ui_queue.get_nowait()
                    event_type = event[0]
                    if event_type == "chat_sent_success":
                        _, sender, ts, text = event
                        self._append_chat(f"[{ts}] You: {text}", "me")
                    elif event_type == "chat_received":
                        _, sender, ts, text = event
                        self._append_chat(f"[{ts}] {sender}: {text}", "peer")
                    elif event_type == "system_msg":
                        _, msg, level = event
                        tag = "sys_success" if level == "success" else "system"
                        self._append_chat(f"[System]: {msg}", tag)
                    elif event_type == "ping_received":
                        _, sender, ts, addr = event
                        self._append_chat(f"[{ts}] Ping from {sender} ({addr[0]}:{addr[1]})", "sys_success")
                    elif event_type == "ack_received":
                        _, sender, ts, text = event
                        self._append_chat(f"[{ts}] ACK from {sender}: {text}", "sys_success")
                except queue.Empty:
                    break
            self.after(50, self._process_ui_queue)

        def _on_close(self):
            self.net_manager.stop_listener()
            self.destroy()


# ----------------------------------------------------------------------
# Application Entrypoint
# ----------------------------------------------------------------------

def main():
    if CTK_AVAILABLE:
        app = ModernP2PChatApp()
    else:
        app = FallbackTkinterP2PChatApp()
    app.mainloop()


if __name__ == "__main__":
    main()
