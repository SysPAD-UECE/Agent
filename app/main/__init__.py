from flask.app import Flask
from flask_cors import CORS

from .config import config_by_name

app = Flask(__name__)
CORS(app)


def create_app(config_name: str) -> Flask:
    app.config.from_object(config_by_name[config_name])

    return app
