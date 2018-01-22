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

    /**
     * The key for the command field in the options map.
     */
    public final static String COMMAND_KEY = "command";

    /**
     * The key for the id field in the options map.
     */
    public final static String ID_KEY = "id";

    /**
     * The key for the request field in the options map.
     */
    public final static String REQUEST_KEY = "request";

    /**
     * Constructor.
     *
     * @param msgId a id unique to a single message or a request/response message pair
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

    /**
     * Constructor.
     *
     * @param header the message header consining annotations in the form @<field>=... .
     * At least id and command annotations need to be given.
     * @param payload the message payload as a bytearray. For encoding the payload a {@link PayloadEncoder} should be
     * used.
     */
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

    /**
     * Check if there are any forbidden signs in the options map. @ and = are not allowed since they are used in the
     * annotation syntax.
     * @return true if any forbbiden signs were found, false otherwise
     */
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

    /**
     * Gets the message id.
     *
     * @return the id
     */
    public int getId() {
        return m_msgId;
    }

    /**
     * Gets the command.
     *
     * @return the command
     */
    public String getCommand() {
        return m_options.get(COMMAND_KEY);
    }

    /**
     * Checks if is request.
     *
     * @return true, if is request
     */
    public boolean isRequest() {
        String isRequest = m_options.get(REQUEST_KEY);
        if(isRequest == null) {
            return false;
        }
        return Boolean.parseBoolean(isRequest) || isRequest.contentEquals("1");
    }

    /**
     * Gets the payload.
     *
     * @return the payload
     */
    public byte[] getPayload() {
        return m_payload;
    }

    /**
     * Get a message option from header. It has to be specified as @<key>=<value> on python side.
     * @param key the annotation key
     * @return the annotation value
     */
    public String getOption(final String key) {
        return m_options.get(key);
    }

    /**
     * Gets the header.
     *
     * @return the header
     */
    public String getHeader() {
        String header = "@id=" + m_msgId;
        for(String key:m_options.keySet()) {
            header += "@" + key + "=" + m_options.get(key);
        }
        return header;
    }

    /**
     * Utility class for decoding the message payload.
     */
    public static class PayloadDecoder {

        private ByteBuffer m_buff;

        /**
         * Constructor.
         * @param bytes the payload bytearray
         */
        public PayloadDecoder(final byte[] bytes) {
            m_buff = ByteBuffer.wrap(bytes);
        }

        /**
         * Get the next encoded String.
         * @return a String
         */
        public String nextString() {
            int len = m_buff.getInt();
            byte[] bstr = new byte[len];
            m_buff.get(bstr);
            return new String(bstr, StandardCharsets.UTF_8);
        }

        /**
         * Get the next encoded bytearray.
         * @return a bytearray
         */
        public byte[] nextBytes() {
            int len = m_buff.getInt();
            byte[] bstr = new byte[len];
            m_buff.get(bstr);
            return bstr;
        }

        /**
         * Get the next encoded integer.
         * @return an integer
         */
        public int nextInt() {
            return m_buff.getInt();
        }
    }

    /**
     * Utility class for encoding a message payload.
     * Schema:  variable size types: (length: int32)(object)
     *          fixed size types: (object)
     */
    public static class PayloadEncoder {
        private ByteBuffer m_buff;
        private int m_position;

        /**
         * Constructor. Allocate initial buffer of 1024 byte.
         */
        public PayloadEncoder() {
            m_buff = ByteBuffer.allocate(1024);
            m_position = 0;
        }

        /**
         * If the buffer has less capacity then size double the buffers capacity.
         * @param size the size of the next entry to write
         */
        private void makeSpace(final int size) {
            while(m_buff.capacity() - m_position < size) {
                ByteBuffer tmp = m_buff;
                m_buff = ByteBuffer.allocate(tmp.capacity() * 2);
                tmp.position(0);
                m_buff.put(tmp);
                m_buff.position(m_position);
            }
        }

        /**
         * Encode a string.
         * @param s a string
         */
        public void putString(final String s) {
            byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
            putBytes(bytes);
        }

        /**
         * Encode a bytearray.
         * @param bytes a bytearray.
         */
        public void putBytes(final byte[] bytes) {
            makeSpace(bytes.length + 4);
            m_buff.putInt(m_position,bytes.length);
            m_position += 4;
            m_buff.position(m_position);
            m_buff.put(bytes);
            m_position += bytes.length;
        }

        /**
         * Encode an integer.
         * @param i an integer
         */
        public void putInt(final int i) {
            makeSpace(4);
            m_buff.putInt(m_position,i);
            m_position += 4;
        }

        /**
         * Get the encoded payload.
         * @return the encoded payload.
         */
        public byte[] get() {
            byte[] payload = new byte[m_position];
            m_buff.position(0);
            m_buff.get(payload);
            return payload;
        }
    }
}
