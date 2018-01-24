/*
 * ------------------------------------------------------------------------
 *
 *  Copyright by KNIME AG, Zurich, Switzerland
 *  Website: http://www.knime.com; Email: contact@knime.com
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License, Version 3, as
 *  published by the Free Software Foundation.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses>.
 *
 *  Additional permission under GNU GPL version 3 section 7:
 *
 *  KNIME interoperates with ECLIPSE solely via ECLIPSE's plug-in APIs.
 *  Hence, KNIME and ECLIPSE are both independent programs and are not
 *  derived from each other. Should, however, the interpretation of the
 *  GNU GPL Version 3 ("License") under any applicable laws result in
 *  KNIME and ECLIPSE being a combined program, KNIME AG herewith grants
 *  you the additional permission to use and propagate KNIME together with
 *  ECLIPSE with only the license terms in place for ECLIPSE applying to
 *  ECLIPSE and the GNU GPL Version 3 applying for KNIME, provided the
 *  license terms of ECLIPSE themselves allow for the respective use and
 *  propagation of ECLIPSE together with KNIME.
 *
 *  Additional permission relating to nodes for KNIME that extend the Node
 *  Extension (and in particular that are based on subclasses of NodeModel,
 *  NodeDialog, and NodeView) and that only interoperate with KNIME through
 *  standard APIs ("Nodes"):
 *  Nodes are deemed to be separate and independent programs and to not be
 *  covered works.  Notwithstanding anything to the contrary in the
 *  License, the License does not apply to Nodes, you are not required to
 *  license Nodes under the License, and you are granted a license to
 *  prepare and propagate Nodes, in each case even if such Nodes are
 *  propagated with or for interoperation with KNIME.  The owner of a Node
 *  may freely choose the license terms applicable to such Node, including
 *  when such Node is propagated with or for interoperation with KNIME.
 * ---------------------------------------------------------------------
 *
 * History
 *   Sep 27, 2017 (clemens): created
 */
package org.knime.python2.kernel;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Message class for wrapping command or status strings received from python.
 *
 * @author Clemens von Schwerin, KNIME GmbH, Konstanz, Germany
 */
public class CommandMessage {

    private int m_msgId;

    private byte[] m_payload;

    /**
     * All header options except the id as key/value pairs
     */
    protected Map<String,String> m_options;

    public final static String COMMAND_KEY = "command";

    public final static String ID_KEY = "id";

    public final static String REQUEST_KEY = "request";

    /**
     * Constructor.
     *
     * @param command a command used for identifying how to process the message
     * @param value the message payload
     * @param isRequest true if the message is a request meaning the python process is waiting for an appropriate
     *            response false otherwise
     */
    public CommandMessage(final int msgId, final String command, final byte[] value, final boolean isRequest, final Optional<Map<String,String>> furtherOptions) {
        m_msgId = msgId;
        m_options = new HashMap<String,String>();
        m_options.put(COMMAND_KEY, command);
        m_payload = value;
        if(isRequest) {
            m_options.put(REQUEST_KEY, "true");
        }
        if(furtherOptions.isPresent()) {
            m_options.putAll(furtherOptions.get());
        }
        if(forbiddenSignsInMap()) {
            throw new IllegalArgumentException("Illegal character (@ or =) detected in options!");
        }
    }

    public CommandMessage(final String header, final byte[] payload) {

        String[] options = header.split("@");
        String key, value;
        boolean idSet = false;
        m_options = new HashMap<String,String>();
        for(String option:options) {
            if(option.isEmpty()) {
                continue;
            }
            key = option.substring(0, option.indexOf("="));
            value = option.substring(option.indexOf("=") + 1);
            if(key.contentEquals(ID_KEY)) {
                m_msgId = Integer.parseInt(value);
                idSet = true;
            } else {
                m_options.put(key, value);
            }
        }
        if(forbiddenSignsInMap()) {
            throw new IllegalArgumentException("Illegal character (@ or =) detected in options!");
        }
        if(!m_options.containsKey(COMMAND_KEY)) {
            throw new IllegalArgumentException("No command in message " + header);
        }
        if(!idSet) {
            throw new IllegalArgumentException("No id in message " + header);
        }
        m_payload = payload;
    }

    private boolean forbiddenSignsInMap() {
        for(String key:m_options.keySet()) {
            if(m_options.get(key).contains("@") || m_options.get(key).contains("@")) {
                return true;
            }
            if(key.contains("@") || key.contains("@")) {
                return true;
            }
        }
        return false;
    }

    public int getId() {
        return m_msgId;
    }

    public String getCommand() {
        return m_options.get(COMMAND_KEY);
    }

    public boolean isRequest() {
        String isRequest = m_options.get(REQUEST_KEY);
        if(isRequest == null) {
            return false;
        }
        return Boolean.parseBoolean(isRequest) || isRequest.contentEquals("1");
    }

    public byte[] getPayload() {
        return m_payload;
    }

    public String getOption(final String key) {
        return m_options.get(key);
    }

    public String getHeader() {
        String header = "@id=" + m_msgId;
        for(String key:m_options.keySet()) {
            header += "@" + key + "=" + m_options.get(key);
        }
        return header;
    }

    /**
     * @return the value-based representation of this message
     */
    /*@Override
    public String toString() {
        return String.join(":", isRequest() ? "r" : "s", m_command, m_value);
    }*/

    public static class PayloadDecoder {

        private ByteBuffer m_buff;

        public PayloadDecoder(final byte[] bytes) {
            m_buff = ByteBuffer.wrap(bytes);
        }

        public String nextString() {
            int len = m_buff.getInt();
            byte[] bstr = new byte[len];
            m_buff.get(bstr);
            return new String(bstr, StandardCharsets.UTF_8);
        }

        public byte[] nextBytes() {
            int len = m_buff.getInt();
            byte[] bstr = new byte[len];
            m_buff.get(bstr);
            return bstr;
        }

        public int nextInt() {
            return m_buff.getInt();
        }
    }

    public static class PayloadEncoder {
        private ByteBuffer m_buff;
        private int m_position;

        public PayloadEncoder() {
            m_buff = ByteBuffer.allocate(1024);
            m_position = 0;
        }

        public void makeSpace(final int size) {
            while(m_buff.capacity() - m_position < size) {
                ByteBuffer tmp = m_buff;
                m_buff = ByteBuffer.allocate(tmp.capacity() * 2);
                tmp.position(0);
                m_buff.put(tmp);
                m_buff.position(m_position);
            }
        }

        public void putString(final String s) {
            byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
            putBytes(bytes);
        }

        public void putBytes(final byte[] bytes) {
            makeSpace(bytes.length + 4);
            m_buff.putInt(m_position,bytes.length);
            m_position += 4;
            m_buff.position(m_position);
            m_buff.put(bytes);
            m_position += bytes.length;
        }

        public void putInt(final int i) {
            makeSpace(4);
            m_buff.putInt(m_position,i);
            m_position += 4;
        }

        public byte[] get() {
            byte[] payload = new byte[m_position];
            m_buff.position(0);
            m_buff.get(payload);
            return payload;
        }
    }
}
