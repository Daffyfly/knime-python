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
 *   Jun 28, 2017 (marcel): created
 */
package org.knime.python2.testing;

import java.util.Optional;

import org.eclipse.core.runtime.preferences.IEclipsePreferences;
import org.eclipse.core.runtime.preferences.InstanceScope;
import org.junit.Before;
import org.junit.Test;
import org.knime.core.node.NodeLogger;
import org.knime.python2.PythonPreferencePage;
import org.knime.python2.kernel.AbstractPythonToJavaMessageHandler;
import org.knime.python2.kernel.CommandMessage;
import org.knime.python2.kernel.CommandMessage.PayloadDecoder;
import org.knime.python2.kernel.CommandMessage.PayloadEncoder;
import org.knime.python2.kernel.Messages;
import org.knime.python2.kernel.PythonKernel;
import org.knime.python2.kernel.PythonKernelOptions;
import org.knime.python2.kernel.PythonKernelOptions.PythonVersionOption;

/**
 * Test class for KNIME Python's message driven communication interface.
 *
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 */
public class MessagingTest {

	// TODO: extend test to cover Python 2

	private static final NodeLogger LOGGER = NodeLogger.getLogger(MessagingTest.class);

	private static final String CUSTOM_PYTHON3_EXEC_PATH = null; // change path here if needed

	/**
	 * Just in case the default "python3" command does not point to an environment that contains all required libs.
	 *
	 * @throws Exception
	 */
	@Before
	public void setCustomPython3ExecutablePath() throws Exception {
		if (CUSTOM_PYTHON3_EXEC_PATH != null) {
			final IEclipsePreferences prefs = InstanceScope.INSTANCE.getNode("org.knime.python2");
			prefs.put(PythonPreferencePage.PYTHON_3_PATH_CFG, CUSTOM_PYTHON3_EXEC_PATH);
			prefs.flush();
		}
	}

	/**
	 * This test covers the processing of two nested requests (one inner request, one outer request) from Python.
	 * <P>
	 * Java executes some Python code which triggers an outer request on Python side (cf. Keras' "train network" code
	 * which leads to Python sending a "training data"-request). Let's say, for some reason, the Java handler of the
	 * outer request then triggers a second request on Python side during handling (cf. "putTable" triggers a
	 * "deserializer"-request when providing the requested training data) - this deadlocks at the moment!
	 *
	 * @throws Exception
	 */
	@Test
	public void testNestedRequests() throws Exception {
		final String importsCode = "from CommandMessage import CommandMessage\n" + //
				"from CommandMessage import next_message_id\n" + //
				"from PayloadBuilder import PayloadBuilder\n" + //
				"from PayloadHandler import PayloadHandler\n\n";

		final PythonKernelOptions kernelOptions = new PythonKernelOptions();
		kernelOptions.setPythonVersionOption(PythonVersionOption.PYTHON3);
		try (final PythonKernel kernel = new PythonKernel(kernelOptions)) {
			final Messages messages = kernel.getMessages();
			messages.registerMessageHandler(new AbstractPythonToJavaMessageHandler("outer_request") {

				// (2) Handle outer request:

				@Override
				protected void handle(final CommandMessage msg) throws Exception {
					LOGGER.debug("Handling outer request (" + msg.getId() + ", " + msg.getCommand() + ", "
							+ msg.isRequest() + ")...");

					final PayloadDecoder dec = new PayloadDecoder(msg.getPayload());
					final String requestString = dec.nextString();
					assert requestString.equals("I'm the outer request :)");

					// (3) Trigger inner request:

					LOGGER.debug("Triggering inner request...");

					final String innerRequestCode = //
							importsCode + //
					"class InnerRequest(CommandMessage)\n:" + //
					"   def __init__(self):\n" + //
					"       pb = PayloadBuilder()\n" + //
					"       pb.add_string('I\\'m the inner request :)')\n" + //
					"       super().__init__('@id=' + str(next_message_id()) + '@command=inner_request@request=true', pb.get_payload())\n"
									+ "\n" + //
					"inner_response = request_from_java(InnerRequest()).get_answer()\n" + //
					"assert PayloadHandler(outer_response.get_payload()).read_string() == 'I\\'m the inner response :)'\n";

					final String[] output = kernel.execute(innerRequestCode);
					if (!output[1].isEmpty()) {
						throw new RuntimeException("Unexpected error on Python side: " + output[1]);
					}

					LOGGER.debug("Triggered inner request.");

					final PayloadEncoder enc = new PayloadEncoder();
					enc.putString("I'm the outer response :)");
					final CommandMessage response = new CommandMessage(msg.getId(), msg.getCommand() + "_response",
							enc.get(), false, Optional.empty());
					messages.answer(msg, response);

					LOGGER.debug("Handled outer request (" + msg.getId() + ", " + msg.getCommand() + ", "
							+ msg.isRequest() + ").");
				}
			});
			messages.registerMessageHandler(new AbstractPythonToJavaMessageHandler("inner_request") {

				// (4) Handle inner request:

				@Override
				protected void handle(final CommandMessage msg) throws Exception {
					LOGGER.debug("Handling inner request (" + msg.getId() + ", " + msg.getCommand() + ", "
							+ msg.isRequest() + ")...");

					final PayloadDecoder dec = new PayloadDecoder(msg.getPayload());
					final String requestString = dec.nextString();
					assert requestString.equals("I'm the inner request :)");

					final PayloadEncoder enc = new PayloadEncoder();
					enc.putString("I'm the inner response :)");
					final CommandMessage response = new CommandMessage(msg.getId(), msg.getCommand() + "_response",
							enc.get(), false, Optional.empty());
					messages.answer(msg, response);

					LOGGER.debug("Handled inner request (" + msg.getId() + ", " + msg.getCommand() + ", "
							+ msg.isRequest() + ").");
				}
			});

			// (1) Trigger outer request:

			LOGGER.debug("Triggering outer request...");

			final String outerRequestCode = //
					importsCode + //
							"class OuterRequest(CommandMessage):\n" + //
							"   def __init__(self):\n" + //
							"       pb = PayloadBuilder()\n" + //
							"       pb.add_string('I\\'m the outer request :)')\n" + //
							"       super().__init__('@id=' + str(next_message_id()) + '@command=outer_request@request=true', pb.get_payload())\n"
							+ "\n" + //
							"outer_response = request_from_java(OuterRequest()).get_answer()\n" + //
							"assert PayloadHandler(outer_response.get_payload()).read_string() == 'I\\'m the outer response :)'\n";

			final String[] output = kernel.execute(outerRequestCode);
			if (!output[1].isEmpty()) {
				throw new RuntimeException("Unexpected error on Python side: " + output[1]);
			}

			LOGGER.debug("Triggered outer request.");
		}
	}
}
