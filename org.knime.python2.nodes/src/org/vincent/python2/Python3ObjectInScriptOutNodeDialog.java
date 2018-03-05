package org.vincent.python2;

import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.DataAwareNodeDialogPane;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.python2.config.PythonSourceCodeOptionsPanel;
import org.knime.python2.config.PythonSourceCodePanel;
import org.knime.python2.generic.templates.SourceCodeTemplatesPanel;
import org.knime.python2.kernel.FlowVariableOptions;
import org.knime.python2.port.PickledObject;
import org.knime.python2.port.PickledObjectPortObject;

/**
 * <code>NodeDialog</code> for the node.
 *
 *
 * @author Patrick Winter, KNIME AG, Zurich, Switzerland
 */
class Python3ObjectInScriptOutNodeDialog extends DataAwareNodeDialogPane {

    PythonSourceCodePanel m_sourceCodePanel;

    PythonSourceCodeOptionsPanel m_sourceCodeOptionsPanel;

    SourceCodeTemplatesPanel m_templatesPanel;

    /**
     * Create the dialog for this node.
     */
    protected Python3ObjectInScriptOutNodeDialog() {
        m_sourceCodePanel = new PythonSourceCodePanel(Python3ObjectInScriptOutNodeConfig.getVariableNames(),
            FlowVariableOptions.parse(getAvailableFlowVariables()));
        m_sourceCodeOptionsPanel = new PythonSourceCodeOptionsPanel(m_sourceCodePanel);
        m_templatesPanel = new SourceCodeTemplatesPanel(m_sourceCodePanel, "python-objectreader");
        addTab("Script", m_sourceCodePanel, false);
        addTab("Options", m_sourceCodeOptionsPanel, true);
        addTab("Templates", m_templatesPanel, true);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) throws InvalidSettingsException {
        final PythonObjectInScriptOutNodeConfig config = new PythonObjectInScriptOutNodeConfig();
        m_sourceCodePanel.saveSettingsTo(config);
        m_sourceCodeOptionsPanel.saveSettingsTo(config);
        config.saveTo(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadSettingsFrom(final NodeSettingsRO settings,final PortObjectSpec[] specs) throws NotConfigurableException {
        final Python3ObjectInScriptOutNodeConfig config = new Python3ObjectInScriptOutNodeConfig();
        config.loadFromInDialog(settings);
        m_sourceCodePanel.loadSettingsFrom(config, specs);
        m_sourceCodeOptionsPanel.loadSettingsFrom(config);
        m_sourceCodePanel.updateFlowVariables(
            getAvailableFlowVariables().values().toArray(new FlowVariable[getAvailableFlowVariables().size()]));
        m_sourceCodePanel.updateData(new BufferedDataTable[0], new PickledObject[]{null, null, null});
        m_sourceCodeOptionsPanel.loadSettingsFrom(config);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadSettingsFrom(final NodeSettingsRO settings, final PortObject[] input) throws NotConfigurableException {
        loadSettingsFrom(settings, new PortObjectSpec[0]);
        PickledObject pickledObject1 = null;
        PickledObject pickledObject2 = null;
        PickledObject pickledObject3 = null;
        if (input[0] != null) {
            pickledObject1 = ((PickledObjectPortObject)input[0]).getPickledObject();
        }
        if (input[1] != null) {
            pickledObject2 = ((PickledObjectPortObject)input[1]).getPickledObject();
        }
        if (input[2] != null) {
            pickledObject3 = ((PickledObjectPortObject)input[2]).getPickledObject();
        }
        m_sourceCodePanel.updateData(new BufferedDataTable[0], new PickledObject[]{pickledObject1,pickledObject2,pickledObject3});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean closeOnESC() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onOpen() {
        m_sourceCodePanel.open();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onClose() {
        m_sourceCodePanel.close();
    }

}
