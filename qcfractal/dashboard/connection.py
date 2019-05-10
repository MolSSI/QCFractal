from flask import current_app, g
import qcportal as ptl

def get_client():
    if 'connection' not in g:
        uri = current_app.config['QCPORTAL_URI']
        if uri is None:
            raise KeyError("QCPROTAL_URI must be set")

        g.connection = ptl.FractalClient(uri, verify=current_app.config['QCPORTAL_VERIFY'])

    return g.connection
