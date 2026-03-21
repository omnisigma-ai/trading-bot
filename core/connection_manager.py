"""
Connection Manager
------------------
Maintains a persistent IB Gateway connection with automatic reconnection.

Uses ib_insync's disconnectedEvent to detect drops and reconnects with
exponential backoff (5s → 60s). All components share a single IB instance.
"""
import asyncio

from ib_insync import IB

from notifications import telegram_notifier as notify


class ConnectionManager:
    """Persistent IB connection with auto-reconnect."""

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 4002,
        client_id: int = 1,
        bot_token: str = "",
        chat_id: str = "",
        reconnect_delay_initial: int = 5,
        reconnect_delay_max: int = 60,
    ):
        self.ib = IB()
        self.host = host
        self.port = port
        self.client_id = client_id
        self.bot_token = bot_token
        self.chat_id = chat_id

        self._reconnect_delay = reconnect_delay_initial
        self._initial_delay = reconnect_delay_initial
        self._max_delay = reconnect_delay_max
        self._connected = False
        self._reconnect_count = 0
        self._shutting_down = False

        # Callbacks for components to react to connection changes
        self._on_reconnect_callbacks: list = []

        # Wire up disconnect handler
        self.ib.disconnectedEvent += self._on_disconnect

    @property
    def connected(self) -> bool:
        return self._connected and self.ib.isConnected()

    @property
    def reconnect_count(self) -> int:
        return self._reconnect_count

    def on_reconnect(self, callback) -> None:
        """Register a callback to be called after successful reconnection."""
        self._on_reconnect_callbacks.append(callback)

    async def connect(self) -> None:
        """Initial connection. Raises on failure."""
        await self.ib.connectAsync(
            self.host, self.port, clientId=self.client_id,
        )
        self._connected = True
        self._reconnect_delay = self._initial_delay
        print(f"[IB] Connected to Gateway at {self.host}:{self.port}")

    async def disconnect(self) -> None:
        """Graceful shutdown."""
        self._shutting_down = True
        self.ib.disconnect()
        self._connected = False

    def _on_disconnect(self) -> None:
        """Handle unexpected disconnection — schedule reconnect."""
        self._connected = False

        if self._shutting_down:
            print("[IB] Disconnected (shutdown)")
            return

        print("[IB] Connection lost — scheduling reconnect...")
        notify.notify_error(
            self.bot_token, self.chat_id,
            "IB Gateway disconnected — reconnecting...",
        )

        # Schedule reconnect as an asyncio task
        loop = asyncio.get_event_loop()
        loop.create_task(self._reconnect_loop())

    async def _reconnect_loop(self) -> None:
        """Reconnect with exponential backoff."""
        while not self._connected and not self._shutting_down:
            print(f"[IB] Reconnecting in {self._reconnect_delay}s...")
            await asyncio.sleep(self._reconnect_delay)

            try:
                await self.ib.connectAsync(
                    self.host, self.port, clientId=self.client_id,
                )
                self._connected = True
                self._reconnect_count += 1
                self._reconnect_delay = self._initial_delay
                print(f"[IB] Reconnected (attempt #{self._reconnect_count})")

                notify.notify_error(
                    self.bot_token, self.chat_id,
                    f"IB Gateway reconnected (#{self._reconnect_count})",
                )

                # Notify all registered components
                for cb in self._on_reconnect_callbacks:
                    try:
                        result = cb()
                        if asyncio.iscoroutine(result):
                            await result
                    except Exception as e:
                        print(f"[IB] Reconnect callback error: {e}")

            except Exception as e:
                self._reconnect_delay = min(
                    self._reconnect_delay * 2, self._max_delay,
                )
                print(f"[IB] Reconnect failed: {e} — retrying in {self._reconnect_delay}s")
