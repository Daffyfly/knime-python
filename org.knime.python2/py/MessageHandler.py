# -*- coding: utf-8 -*-

# ------------------------------------------------------------------------
#  Copyright by KNIME AG, Zurich, Switzerland
#  Website: http://www.knime.com; Email: contact@knime.com
#
#  This program is free software; you can redistribute it and/or modify
#  it under the terms of the GNU General Public License, Version 3, as
#  published by the Free Software Foundation.
#
#  This program is distributed in the hope that it will be useful, but
#  WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program; if not, see <http://www.gnu.org/licenses>.
#
#  Additional permission under GNU GPL version 3 section 7:
#
#  KNIME interoperates with ECLIPSE solely via ECLIPSE's plug-in APIs.
#  Hence, KNIME and ECLIPSE are both independent programs and are not
#  derived from each other. Should, however, the interpretation of the
#  GNU GPL Version 3 ("License") under any applicable laws result in
#  KNIME and ECLIPSE being a combined program, KNIME AG herewith grants
#  you the additional permission to use and propagate KNIME together with
#  ECLIPSE with only the license terms in place for ECLIPSE applying to
#  ECLIPSE and the GNU GPL Version 3 applying for KNIME, provided the
#  license terms of ECLIPSE themselves allow for the respective use and
#  propagation of ECLIPSE together with KNIME.
#
#  Additional permission relating to nodes for KNIME that extend the Node
#  Extension (and in particular that are based on subclasses of NodeModel,
#  NodeDialog, and NodeView) and that only interoperate with KNIME through
#  standard APIs ("Nodes"):
#  Nodes are deemed to be separate and independent programs and to not be
#  covered works.  Notwithstanding anything to the contrary in the
#  License, the License does not apply to Nodes, you are not required to
#  license Nodes under the License, and you are granted a license to
#  prepare and propagate Nodes, in each case even if such Nodes are
#  propagated with or for interoperation with KNIME.  The owner of a Node
#  may freely choose the license terms applicable to such Node, including
#  when such Node is propagated with or for interoperation with KNIME.
# ------------------------------------------------------------------------
from concurrent import futures
import CommandMessageHandler
import threading
import multiprocessing

# Deals with all incoming and outgoing message communication.
class MessageHandler:

    # Constructor
    def __init__(self, kernel, number_threads=None):
        if number_threads is None:
            number_threads = multiprocessing.cpu_count() * 2
        self._kernel = kernel
        self._pool = futures.ThreadPoolExecutor(number_threads)
        self._waiting_for_answers = dict()
        self._waiting_for_answers_lock = threading.Lock()
        self._running = True

    # Listens for incoming messages and redirects them to the correct recipient.
    # This can either be the AnswerFuture created for a request waiting for
    # its response or a CommandMessageHandler.
    def main_loop(self):
        try:
            while self._running:
                message = self._kernel.read_message()
                message_id = message.get_id()
                self._waiting_for_answers_lock.acquire()
                in_waiting_for_answers = message_id in self._waiting_for_answers
                if in_waiting_for_answers:
                    self._waiting_for_answers[message_id].set_answer(message)
                self._waiting_for_answers_lock.release()
                if not in_waiting_for_answers:
                    handler = CommandMessageHandler.get_command_message_handler(message)
                    self._pool.submit(handler.execute, self._kernel)
        finally:
            self._pool.shutdown()
            exit()

    # Send out a message to Java using the shared socket.
    # @param message a CommandMessage
    def send_message(self, message):
        answer = None
        if message.is_data_request():
            self._waiting_for_answers_lock.acquire()
            answer = AnswerFuture()
            self._waiting_for_answers[message.get_id()] = answer
            self._waiting_for_answers_lock.release()
        self._kernel.write_message(message)
        return answer

    # Shutdown this message handler.
    def shutdown(self):
        self._running = False

# A class that works like an implementation of Java's Future interface.
# get_answer blocks the executing thread until an answer message is set using
# set_answer.

class AnswerFuture:

    def __init__(self):
        self._answer_message = None
        self._condition = threading.Condition()

    def set_answer(self, message):
        self._condition.acquire()
        self._answer_message = message
        self._condition.notify()
        self._condition.release()

    def get_answer(self):
        self._condition.acquire()
        while self._answer_message is None:
            self._condition.wait()
        answer = self._answer_message
        self._condition.release()
        return answer
