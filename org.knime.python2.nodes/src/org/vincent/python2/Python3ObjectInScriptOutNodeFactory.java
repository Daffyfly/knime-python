package org.vincent.python2;

import org.knime.base.node.util.exttool.ExtToolStderrNodeView;
import org.knime.base.node.util.exttool.ExtToolStdoutNodeView;
import org.knime.core.node.ContextAwareNodeFactory;
import org.knime.core.node.NodeCreationContext;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeView;

/**
 * <code>NodeFactory</code> for the node.
 *
 *
 * @author Patrick Winter, KNIME AG, Zurich, Switzerland
 */
public class Python3ObjectInScriptOutNodeFactory extends ContextAwareNodeFactory<Python3ObjectInScriptOutNodeModel> {

    /**
     * {@inheritDoc}
     */
    @Override
    public Python3ObjectInScriptOutNodeModel createNodeModel() {
        return new Python3ObjectInScriptOutNodeModel();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getNrNodeViews() {
        return 2;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NodeView<Python3ObjectInScriptOutNodeModel> createNodeView(final int viewIndex,
        final Python3ObjectInScriptOutNodeModel nodeModel) {
        if (viewIndex == 0) {
            return new ExtToolStdoutNodeView<Python3ObjectInScriptOutNodeModel>(nodeModel);
        } else if (viewIndex == 1) {
            return new ExtToolStderrNodeView<Python3ObjectInScriptOutNodeModel>(nodeModel);
        }
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean hasDialog() {
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NodeDialogPane createNodeDialogPane() {
        return new Python3ObjectInScriptOutNodeDialog();
    }

    @Override
    public Python3ObjectInScriptOutNodeModel createNodeModel(final NodeCreationContext context) {
        return new Python3ObjectInScriptOutNodeModel(context);
    }

}
