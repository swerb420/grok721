"""Visualization dashboard stubs using Plotly Dash.
This pared-down version exposes only the essentials needed to
run unit tests that expect the module to exist."""

from typing import Dict
import dash
from dash import html


class VisualizationEngine:
    def __init__(self, db=None):
        self.db = db
        self.app = dash.Dash(__name__)
        self.app.layout = html.Div([html.H1("Dashboard")])

    def run_dashboard(self, port: int = 8050, debug: bool = True):
        self.app.run_server(port=port, debug=debug)
