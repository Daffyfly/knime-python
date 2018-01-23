# -*- coding: utf-8 -*-
from pandas.core.dtypes.missing import isnull
import PayloadBuilder
import struct

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


# Used for defining messages that can be sent to Java
class CommandMessage(object):

    def __init__(self, header, payload):
        self._header = header
        self._options = dict()
        self._payload = payload
        self._id = None
        for option in header.split("@"):
            if not option:
                continue
            key = option[:option.find("=")]
            if key.lower() == "id":
                self._id = int(option[option.find("=")+1:])
            else:
                self._options[option[:option.find("=")]] = option[option.find("=")+1:]
        if self.forbidden_signs_in_options():
            raise AttributeError("Forbidden character (@ or =) in message options detected!")
        if "command" not in self._options.keys():
            raise AttributeError("No command specified for message: " + header)
        if self._id is None:
            raise AttributeError("No id specified for message: " + header)
    
    def is_data_request(self):
        return ("request" in self._options.keys()) and (self._options["request"].lower() in ["true", "1"])
    
    def get_id(self):
        return self._id
    
    def get_command(self):
        return self._options["command"]
    
    def get_payload(self):
        return self._payload
    
    def get_header(self):
        return '@id=' + str(self._id) + ''.join(['@' + key + '=' + str(self._options[key]) for key in self._options])
    
    def forbidden_signs_in_options(self):
        for key in self._options:
            if key.find("@") > 0 or key.find("=") > 0:
                return True
            if self._options[key].find("@") > 0 or self._options[key].find("=") > 0:
                return True
        return False
    

# Used for indicating the successful termination of a command        
class SuccessMessage(CommandMessage):
    def __init__(self, id_):
        CommandMessage.__init__(self, '@id=' + str(id_) + '@command=success', None)


# Used for requesting a serializer from java. The value may either be the
# python type that is to be serialized or the extension id
class SerializerRequest(CommandMessage):
    def __init__(self, id_, val):
        CommandMessage.__init__(self, '@id=' + str(id_) + '@command=serializer_request@request=true', val)


# Used for requesting a deserializer from java. The value should be the extension id.       
class DeserializerRequest(CommandMessage):
    def __init__(self, id_, val):
        CommandMessage.__init__(self, '@id=' + str(id_) + '@command=deserializer_request@request=true', val)


class ExecuteResponseMessage(CommandMessage):

    def __init__(self, id_, output, error):
        header = '@id=' + str(id_) + '@command=execute_response'
        payload_builder = PayloadBuilder.PayloadBuilder()
        payload_builder.add_string(output)
        payload_builder.add_string(error)
        CommandMessage.__init__(self, header, payload_builder.get_payload())


class GenericIntegerMessage(CommandMessage):

    def __init__(self, id_, command, integer):
        header = '@id=' + str(id_) + '@command=' + command
        payload_builder = PayloadBuilder.PayloadBuilder()
        payload_builder.add_integer(integer)
        CommandMessage.__init__(self, header, payload_builder.get_payload())


class GenericStringMessage(CommandMessage):

    def __init__(self, id_, command, string):
        header = '@id=' + str(id_) + '@command=' + command
        payload_builder = PayloadBuilder.PayloadBuilder()
        payload_builder.add_string(string)
        CommandMessage.__init__(self, header, payload_builder.get_payload())


class GenericBytesMessage(CommandMessage):

    def __init__(self, id_, command, data_bytes):
        header = '@id=' + str(id_) + '@command=' + command
        payload_builder = PayloadBuilder.PayloadBuilder()
        payload_builder.add_bytes(data_bytes)
        CommandMessage.__init__(self, header, payload_builder.get_payload())
