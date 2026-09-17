import threading
from typing import Callable

from .base_hub_connection import BaseHubConnection
from ..helpers import Helpers
from ..messages.invocation_message import InvocationMessage


class ExecutorHubConnection(BaseHubConnection):
    def __init__(self, executor, **kwargs):
        if executor is None or not callable(getattr(executor, "submit", None)):
            raise TypeError("executor must expose a callable submit method")
        if not callable(getattr(executor, "shutdown", None)):
            raise TypeError("executor must expose a callable shutdown method")
        super(ExecutorHubConnection, self).__init__(**kwargs)
        self.executor = executor
        self._executor_shutdown = False
        self._executor_shutdown_lock = threading.Lock()

    def stop(self) -> None:
        with self._executor_shutdown_lock:
            shutdown_executor = not self._executor_shutdown
            self._executor_shutdown = True
        try:
            super(ExecutorHubConnection, self).stop()
        finally:
            if shutdown_executor:
                self.executor.shutdown(wait=True, cancel_futures=True)

    def _run_handler(self, handler: Callable, arguments):
        return self._invoke_handler(handler, arguments)

    def _run_handlers_for_result(self, handlers, arguments):
        for handler in handlers:
            result = self._run_handler(handler, arguments)
            if result is not None:
                return result
        return None

    def _log_callback_failure(self, future):
        try:
            future.result()
        except Exception:
            self.logger.exception("Executor hub handler raised an exception")

    def _complete_invocation_from_future(self, invocation_id, future):
        try:
            self._send_completion(invocation_id, result=future.result())
        except Exception as ex:
            self.logger.exception(
                "Executor result handler raised an exception")
            self._send_completion(invocation_id, error=str(ex))

    def _handle_invocation_message(
            self, message: InvocationMessage) -> None:  # 1
        fired_handlers = list(self.handlers.get(message.target, []))

        if len(fired_handlers) == 0:
            if message.invocation_id is not None:
                self.logger.warning(
                    f"Server requested a result for '{message.target}' "
                    f"but no handler is registered")
                self._send_completion(message.invocation_id)
            else:
                self.logger.info(
                    f"Event '{message.target}' hasn't fired any handler")
            return

        if message.invocation_id is not None:
            future = self.executor.submit(
                self._run_handlers_for_result,
                fired_handlers,
                message.arguments)
            future.add_done_callback(
                lambda done: self._complete_invocation_from_future(
                    message.invocation_id,
                    done))
            return

        for handler in fired_handlers:
            future = self.executor.submit(
                self._run_handler,
                handler,
                message.arguments)
            future.add_done_callback(self._log_callback_failure)


class AuthExecutorHubConnection(ExecutorHubConnection):
    def __init__(self, auth_function, headers=None, **kwargs):
        super(AuthExecutorHubConnection, self).__init__(
            headers=headers,
            **kwargs)
        self.auth_function = auth_function

    def start(self):
        try:
            Helpers.get_logger().debug("Starting connection ...")
            self.token = self.auth_function()
            Helpers.get_logger()\
                .debug("auth function result {0}".format(self.token))
            self.headers["Authorization"] = "Bearer " + self.token
            return super(AuthExecutorHubConnection, self).start()
        except Exception as ex:
            Helpers.get_logger().warning(self.__class__.__name__)
            Helpers.get_logger().warning(str(ex))
            raise ex
