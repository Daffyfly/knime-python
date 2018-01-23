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

import PayloadHandler
from CommandMessage import *
import debug_util
import os

class CommandMessageHandler:

    def __init__(self, command_message):
        self._command_message = command_message
        self._payload_handler = PayloadHandler.PayloadHandler(self._command_message.get_payload())

    def get_command_message(self):
        return self._command_message

    def get_payload_handler(self):
        return self._payload_handler

    def execute(self, kernel_):
        raise NotImplementedError("Abstract class CommandHandler does not provide an Implementation for execute().")


class ExecuteCommandMessageHandler(CommandMessageHandler):

    def __init__(self, command_message):
        CommandMessageHandler.__init__(self, command_message)

    def execute(self, kernel_):
        source_code = self.get_payload_handler().read_string()
        id = self.get_command_message().get_id()
        debug_util.debug_msg('executing: ' + source_code + '\n')
        output, error = kernel_.execute(source_code)
        debug_util.debug_msg('executing done!')
        kernel_.write_message(CommandMessage.OutputMessage(id, output, error))


class PutFlowVariablesCommandMessageHandler(CommandMessageHandler):

    def __init__(self, command_message):
        CommandMessageHandler.__init__(self, command_message)

    def execute(self, kernel_):
        # TODO
        flow_variables = collections.OrderedDict()
        name = kernel_.read_string()
        data_bytes = kernel_.read_bytearray()
        data_frame = kernel_.bytes_to_data_frame(data_bytes)
        kernel_.fill_flow_variables_from_data_frame(flow_variables, data_frame)
        kernel_.put_variable(name, flow_variables)
        kernel_.write_dummy()

class GetFlowVariablesCommandMessageHandler(CommandMessageHandler):

    def __init__(self, command_message):
        CommandMessageHandler.__init__(self, command_message)

    def execute(self, kernel_):
        # TODO
        name = kernel_.read_string()
        current_variables = kernel_.get_variable(name)
        data_frame = kernel_.flow_variables_dict_to_data_frame(current_variables)
        data_bytes = kernel_.data_frame_to_bytes(data_frame)
        kernel_.write_bytearray(data_bytes)


class PutTableCommandMessageHandler(CommandMessageHandler):

    def __init__(self, command_message):
        CommandMessageHandler.__init__(self, command_message)

    def execute(self, kernel_):
        # TODO
        name = kernel_.read_string()
        data_bytes = kernel_.read_bytearray()
        data_frame = kernel_.bytes_to_data_frame(data_bytes)
        kernel_.put_variable(name, data_frame)
        kernel_.write_message(SuccessMessage())


class AppendToTableCommandMessageHandler(CommandMessageHandler):

    def __init__(self, command_message):
        CommandMessageHandler.__init__(self, command_message)

    def execute(self, kernel_):
        # TODO
        name = kernel_.read_string()
        data_bytes = kernel_.read_bytearray()
        data_frame = kernel_.bytes_to_data_frame(data_bytes)
        kernel_.append_to_table(name, data_frame)
        kernel_.write_message(SuccessMessage())


class GetTableSizeCommandMessageHandler(CommandMessageHandler):

    def __init__(self, command_message):
        CommandMessageHandler.__init__(self, command_message)

    def execute(self, kernel_):
        # TODO
        name = kernel_.read_string()
        data_frame = kernel_.get_variable(name)
        kernel_.write_integer(len(data_frame))


class GetTableCommandMessageHandler(CommandMessageHandler):

    def __init__(self, command_message):
        CommandMessageHandler.__init__(self, command_message)

    def execute(self, kernel_):
        # TODO
        debug_util.debug_msg('getTable\n')
        name = kernel_.read_string()
        data_frame = kernel_.get_variable(name)
        import pandas
        if type(data_frame) != pandas.core.frame.DataFrame:
            raise TypeError("Expected pandas.DataFrame, got: " + str(type(data_frame)) + "\nPlease make sure your"
                                                                                         " output_table is"
                                                                                         " a pandas.DataFrame.")
        data_bytes = kernel_.data_frame_to_bytes(data_frame)
        kernel_.write_message(SuccessMessage())
        kernel_.write_bytearray(data_bytes)


class GetTableChunkCommandMessageHandler(CommandMessageHandler):

    def __init__(self, command_message):
        CommandMessageHandler.__init__(self, command_message)

    def execute(self, kernel_):
        # TODO
        debug_util.debug_msg('getTableChunk\n')
        name = kernel_.read_string()
        start = kernel_.read_integer()
        end = kernel_.read_integer()
        data_frame = kernel_.get_variable(name)
        import pandas
        if type(data_frame) != pandas.core.frame.DataFrame:
            raise TypeError("Expected pandas.DataFrame, got: " + str(type(data_frame)) + "\nPlease make sure your"
                                                                                         " output_table is a"
                                                                                         " pandas.DataFrame.")
        data_frame_chunk = data_frame[start:end+1]
        data_bytes = kernel_.data_frame_to_bytes(data_frame_chunk, start)
        kernel_.write_message(SuccessMessage())
        kernel_.write_bytearray(data_bytes)


class ListVariablesCommandMessageHandler(CommandMessageHandler):

    def __init__(self, command_message):
        CommandMessageHandler.__init__(self, command_message)

    def execute(self, kernel_):
        # TODO
        variables = kernel_.list_variables()
        data_frame = DataFrame(variables)
        data_bytes = kernel_.data_frame_to_bytes(data_frame)
        kernel_.write_bytearray(data_bytes)


class ResetCommandMessageHandler(CommandMessageHandler):

    def __init__(self, command_message):
        CommandMessageHandler.__init__(self, command_message)

    def execute(self, kernel_):
        # TODO
        kernel_.reset()
        kernel_.write_dummy()


class HasAutoCompleteCommandMessageHandler(CommandMessageHandler):

    def __init__(self, command_message):
        CommandMessageHandler.__init__(self, command_message)

    def execute(self, kernel_):
        # TODO
        if kernel_.has_auto_complete():
            value = 1
        else:
            value = 0
        kernel_.write_integer(value)


class AutoCompleteCommandMessageHandler(CommandMessageHandler):

    def __init__(self, command_message):
        CommandMessageHandler.__init__(self, command_message)

    def execute(self, kernel_):
        # TODO
        source_code = kernel_.read_string()
        line = kernel_.read_integer()
        column = kernel_.read_integer()
        suggestions = kernel_.auto_complete(source_code, line, column)
        data_frame = DataFrame(suggestions)
        data_bytes = kernel_.data_frame_to_bytes(data_frame)
        kernel_.write_bytearray(data_bytes)


class GetImageCommandMessageHandler(CommandMessageHandler):

    def __init__(self, command_message):
        CommandMessageHandler.__init__(self, command_message)

    def execute(self, kernel_):
        # TODO
        name = kernel_.read_string()
        image = kernel_.get_variable_or_default(name, None)
        if _python3:
            if type(image) is bytes:
                data_bytes = image
            else:
                data_bytes = bytearray()
        else:
            if type(image) is str:
                data_bytes = image
            else:
                data_bytes = ''
        kernel_.write_bytearray(data_bytes)


class GetObjectCommandMessageHandler(CommandMessageHandler):

    def __init__(self, command_message):
        CommandMessageHandler.__init__(self, command_message)

    def execute(self, kernel_):
        # TODO
        name = kernel_.read_string()
        data_object = kernel_.get_variable(name)
        o_bytes = bytearray(pickle.dumps(data_object))
        o_type = type(data_object).__name__
        o_representation = kernel_.object_to_string(data_object)
        data_frame = DataFrame([{'bytes': o_bytes, 'type': o_type, 'representation': o_representation}])
        data_bytes = kernel_.data_frame_to_bytes(data_frame)
        kernel_.write_bytearray(data_bytes)


class PutObjectCommandMessageHandler(CommandMessageHandler):

    def __init__(self, command_message):
        CommandMessageHandler.__init__(self, command_message)

    def execute(self, kernel_):
        # TODO
        name = kernel_.read_string()
        data_bytes = kernel_.read_bytearray()
        data_object = pickle.loads(data_bytes)
        kernel_.put_variable(name, data_object)
        kernel_.write_dummy()


class AddSerializerCommandMessageHandler(CommandMessageHandler):

    def __init__(self, command_message):
        CommandMessageHandler.__init__(self, command_message)

    def execute(self, kernel_):
        # TODO
        s_id = kernel_.read_string()
        s_type = kernel_.read_string()
        s_path = kernel_.read_string()
        kernel_._type_extension_manager.add_serializer(s_id, s_type, s_path)
        kernel_.write_dummy()


class AddDeserializerCommandMessageHandler(CommandMessageHandler):

    def __init__(self, command_message):
        CommandMessageHandler.__init__(self, command_message)

    def execute(self, kernel_):
        # TODO
        d_id = kernel_.read_string()
        d_path = kernel_.read_string()
        kernel_._type_extension_manager.add_deserializer(d_id, d_path)
        kernel_.write_dummy()


class ShutdownCommandMessageHandler(CommandMessageHandler):

    def __init__(self, command_message):
        CommandMessageHandler.__init__(self, command_message)

    def execute(self, kernel_):
        # TODO
        kernel_._cleanup()
        exit()


class PutSqlCommandMessageHandler(CommandMessageHandler):

    def __init__(self, command_message):
        CommandMessageHandler.__init__(self, command_message)

    def execute(self, kernel_):
        # TODO
        name = kernel_.read_string()
        data_bytes = kernel_.read_bytearray()
        data_frame = kernel_.bytes_to_data_frame(data_bytes)
        db_util = DBUtil(data_frame)
        kernel_._exec_env[name] = db_util
        kernel_._cleanup_object_names.append(name)
        kernel_.write_dummy()


class GetSqlCommandMessageHandler(CommandMessageHandler):

    def __init__(self, command_message):
        CommandMessageHandler.__init__(self, command_message)

    def execute(self, kernel_):
        # TODO
        name = kernel_.read_string()
        db_util = kernel_.get_variable(name)
        db_util._writer.commit()
        query = db_util.get_output_query()
        kernel_.write_string(query)


class SetCustomModulePathsCommandMessageHandler(CommandMessageHandler):

    def __init__(self, command_message):
        CommandMessageHandler.__init__(self, command_message)

    def execute(self, kernel_):
        # TODO
        path = kernel_.read_string()
        sys.path.append(path)
        kernel_.write_dummy()
        
class GetPidCommandMessageHandler(CommandMessageHandler):
    
    def __init__(self, command_message):
        CommandMessageHandler.__init__(self, command_message)
        
    def execute(self, kernel_):
        pid = os.getpid()
        response = IdMessage(self._command_message.get_id(), pid)
        kernel_.write_message(response)



_command_message_handlers = {'execute': ExecuteCommandMessageHandler,
                             'putFlowVariables': PutFlowVariablesCommandMessageHandler,
                             'getFlowVariables': GetFlowVariablesCommandMessageHandler,
                             'putTable': PutTableCommandMessageHandler,
                             'appendToTable': AppendToTableCommandMessageHandler,
                             'getTableSize': GetTableSizeCommandMessageHandler,
                             'getTable': GetTableCommandMessageHandler,
                             'getTableChunk': GetTableChunkCommandMessageHandler,
                             'listVariables': ListVariablesCommandMessageHandler,
                             'reset': ResetCommandMessageHandler,
                             'hasAutoComplete': HasAutoCompleteCommandMessageHandler,
                             'autoComplete': AutoCompleteCommandMessageHandler,
                             'getImage': GetImageCommandMessageHandler,
                             'getObject': GetObjectCommandMessageHandler,
                             'putObject': PutObjectCommandMessageHandler,
                             'addSerializer': AddSerializerCommandMessageHandler,
                             'addDeserializer': AddDeserializerCommandMessageHandler,
                             'shutdown': ShutdownCommandMessageHandler,
                             'putSql': PutSqlCommandMessageHandler,
                             'getSql': GetSqlCommandMessageHandler,
                             'setCustomModulePaths': SetCustomModulePathsCommandMessageHandler,
                             'getpid': GetPidCommandMessageHandler}


def get_command_message_handler(command_message):
    return _command_message_handlers[command_message.get_command()](command_message)
