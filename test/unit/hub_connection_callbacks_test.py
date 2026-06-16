from concurrent.futures import Future
import unittest

from signalrcore.hub.base_hub_connection import BaseHubConnection
from signalrcore.hub.executor_hub_connection import ExecutorHubConnection
from signalrcore.messages.completion_message import CompletionMessage
from signalrcore.messages.invocation_message import InvocationMessage
from signalrcore.messages.stream_item_message import StreamItemMessage
from signalrcore.subject import Subject


class DummyTransport(object):
    def __init__(self):
        self.sent = []

    def send(self, message):
        self.sent.append(message)

    def is_running(self):
        return True


class ManualExecutor(object):
    def __init__(self):
        self.calls = []

    def submit(self, fn, *args, **kwargs):
        future = Future()
        self.calls.append((future, fn, args, kwargs))
        return future

    def run_next(self):
        future, fn, args, kwargs = self.calls.pop(0)
        try:
            future.set_result(fn(*args, **kwargs))
        except Exception as ex:
            future.set_exception(ex)
        return future


class HubConnectionCallbacksTest(unittest.TestCase):
    def test_on_return_value_is_used_as_client_result(self):
        connection = BaseHubConnection(url="http://example.test")
        connection.transport = DummyTransport()
        connection.on("GetMessage", lambda args: "Hello")

        connection.on_message([
            InvocationMessage("1", "GetMessage", [])
        ])

        self.assertEqual(1, len(connection.transport.sent))
        completion = connection.transport.sent[0]
        self.assertIsInstance(completion, CompletionMessage)
        self.assertEqual("1", completion.invocation_id)
        self.assertEqual("Hello", completion.result)
        self.assertIsNone(completion.error)

    def test_on_uses_first_non_none_result(self):
        connection = BaseHubConnection(url="http://example.test")
        connection.transport = DummyTransport()
        connection.on("GetMessage", lambda args: None)
        connection.on("GetMessage", lambda args: "Hello")

        connection.on_message([
            InvocationMessage("1", "GetMessage", [])
        ])

        completion = connection.transport.sent[0]
        self.assertEqual("Hello", completion.result)

    def test_executor_connection_defers_callback_until_future_runs(self):
        executor = ManualExecutor()
        connection = ExecutorHubConnection(
            executor=executor,
            url="http://example.test")
        connection.transport = DummyTransport()
        called = []

        def handler(args):
            called.append(args)
            return "Hello"

        connection.on("GetMessage", handler)

        connection.on_message([
            InvocationMessage("1", "GetMessage", ["arg"])
        ])

        self.assertEqual([], called)
        self.assertEqual([], connection.transport.sent)
        self.assertEqual(1, len(executor.calls))

        executor.run_next()

        self.assertEqual([["arg"]], called)
        completion = connection.transport.sent[0]
        self.assertEqual("Hello", completion.result)

    def test_subject_uses_connection_send_helper(self):
        connection = BaseHubConnection(url="http://example.test")
        connection.transport = DummyTransport()
        subject = Subject()
        sent = []

        def locked_send(message):
            sent.append(message)

        connection._send = locked_send

        connection.invoke("UploadStream", subject)
        subject.next("item")
        subject.complete()

        self.assertEqual(3, len(sent))
        self.assertEqual([], connection.transport.sent)
        self.assertIsInstance(sent[1], StreamItemMessage)


if __name__ == "__main__":
    unittest.main()
