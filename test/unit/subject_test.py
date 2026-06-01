from unittest import TestCase
from unittest.mock import Mock
from signalrcore.subject import Subject
from signalrcore.messages.invocation_message import \
    InvocationClientStreamMessage


class TestSubject(TestCase):
    def _configure_subject(self, subject):
        subject.connection = Mock()
        subject.connection.transport = Mock()
        subject.target = "UploadStream"
        subject.invocation_id = "invocation-id"
        return subject

    def test_start_sends_empty_arguments_by_default(self):
        subject = self._configure_subject(Subject())
        subject.start()

        sent_message = subject.connection.transport.send.call_args[0][0]
        self.assertIsInstance(sent_message, InvocationClientStreamMessage)
        self.assertEqual(sent_message.arguments, [])

    def test_start_sends_arguments_from_constructor(self):
        subject = self._configure_subject(Subject(arguments=["arg1", 2]))
        subject.start()

        sent_message = subject.connection.transport.send.call_args[0][0]
        self.assertIsInstance(sent_message, InvocationClientStreamMessage)
        self.assertEqual(sent_message.stream_ids, ["invocation-id"])
        self.assertEqual(sent_message.target, "UploadStream")
        self.assertEqual(sent_message.arguments, ["arg1", 2])
