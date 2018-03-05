package org.vincent.python2;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;

import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeCreationContext;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.python2.kernel.PythonKernel;
import org.knime.python2.nodes.PythonNodeModel;
import org.knime.python2.port.PickledObjectPortObject;

/**
 * This is the model implementation.
 *
 *
 * @author Patrick Winter, KNIME AG, Zurich, Switzerland
 */
class Python3ObjectInScriptOutNodeModel extends PythonNodeModel<Python3ObjectInScriptOutNodeConfig> {

    /**
     * Constructor for the node model.
     */
    protected Python3ObjectInScriptOutNodeModel() {
        super(new PortType[]{PickledObjectPortObject.TYPE,PickledObjectPortObject.TYPE,PickledObjectPortObject.TYPE}, new PortType[]{BufferedDataTable.TYPE});
    }

    protected Python3ObjectInScriptOutNodeModel(final NodeCreationContext context) {
        this();
        URI uri;
        try {
            uri = context.getUrl().toURI();
        } catch (final URISyntaxException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
        if ((!uri.getScheme().equals("knime")) || (!uri.getHost().equals("LOCAL"))) {
            throw new RuntimeException("Only pickle files in the local workspace are supported.");
        }
        getConfig().setSourceCode(PythonObjectInScriptOutNodeConfig.getDefaultSourceCode(uri.getPath()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObject[] execute(final PortObject[] inData, final ExecutionContext exec) throws Exception {
        BufferedDataTable dt = null;
        try (final PythonKernel kernel = new PythonKernel(getKernelOptions())) {
            kernel.putFlowVariables(Python3ObjectInScriptOutNodeConfig.getVariableNames().getFlowVariables(),
                getAvailableFlowVariables().values());

            PickledObjectPortObject object = (PickledObjectPortObject) inData[0];
            kernel.putObject(Python3ObjectInScriptOutNodeConfig.getVariableNames().getInputObjects()[0], object.getPickledObject());
            object = (PickledObjectPortObject) inData[1];
            kernel.putObject(Python3ObjectInScriptOutNodeConfig.getVariableNames().getInputObjects()[1], object.getPickledObject());
            object = (PickledObjectPortObject)   inData[2];
            kernel.putObject(Python3ObjectInScriptOutNodeConfig.getVariableNames().getInputObjects()[2], object.getPickledObject());
            final String[] output = kernel.execute(getConfig().getSourceCode(), exec);
            setExternalOutput(new LinkedList<String>(Arrays.asList(output[0].split("\n"))));
            setExternalErrorOutput(new LinkedList<String>(Arrays.asList(output[1].split("\n"))));
            exec.createSubProgress(0.9).setProgress(1);
            final Collection<FlowVariable> variables =
                kernel.getFlowVariables(Python3ObjectInScriptOutNodeConfig.getVariableNames().getFlowVariables());
            dt = kernel.getDataTable(Python3ObjectInScriptOutNodeConfig.getVariableNames().getOutputTables()[0], exec, exec.createSubProgress(0.3));
            exec.createSubProgress(0.1).setProgress(1);
            addNewVariables(variables);
        }
        return new PortObject[]{dt};
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObjectSpec[] configure(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        return new PortObjectSpec[]{null};
    }

    @Override
    protected Python3ObjectInScriptOutNodeConfig createConfig() {
        return new Python3ObjectInScriptOutNodeConfig();
    }

}
