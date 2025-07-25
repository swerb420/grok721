import sys
import types
import importlib
import pytest


@pytest.fixture(scope="session", autouse=True)
def stub_optional_dependencies():
    """Insert lightweight stubs for optional third party packages."""
    # apify_client
    if 'apify_client' not in sys.modules:
        apify_client = types.ModuleType('apify_client')
        apify_client.ApifyClient = lambda token: None
        sys.modules['apify_client'] = apify_client

    # telegram
    if 'telegram' not in sys.modules:
        telegram = types.ModuleType('telegram')
        telegram.Bot = lambda token: None
        ext = types.ModuleType('telegram.ext')
        ext.Updater = lambda *a, **k: None
        ext.CallbackQueryHandler = lambda *a, **k: None
        ext.CommandHandler = lambda *a, **k: None
        telegram.ext = ext
        sys.modules['telegram'] = telegram
        sys.modules['telegram.ext'] = ext

    # apscheduler background scheduler
    if 'apscheduler.schedulers.background' not in sys.modules:
        background = types.ModuleType('background')
        background.BackgroundScheduler = lambda *a, **k: None
        schedulers = types.ModuleType('schedulers')
        schedulers.background = background
        apscheduler = types.ModuleType('apscheduler')
        apscheduler.schedulers = schedulers
        sys.modules['apscheduler'] = apscheduler
        sys.modules['apscheduler.schedulers'] = schedulers
        sys.modules['apscheduler.schedulers.background'] = background

    # transformers sentiment pipeline
    if 'transformers' not in sys.modules:
        transformers = types.ModuleType('transformers')
        transformers.pipeline = lambda *a, **k: (lambda text: [{'label': 'POSITIVE', 'score': 1.0}])
        transformers.AutoTokenizer = types.SimpleNamespace(from_pretrained=lambda *a, **k: None)
        transformers.AutoModelForSequenceClassification = types.SimpleNamespace(from_pretrained=lambda *a, **k: None)
        sys.modules['transformers'] = transformers

    # requests
    if 'requests' not in sys.modules:
        requests = types.ModuleType('requests')

        def _resp():
            class R:
                def json(self):
                    return {}
            return R()

        requests.get = lambda *a, **k: _resp()
        requests.post = lambda *a, **k: _resp()
        sys.modules['requests'] = requests

    # sklearn
    if 'sklearn' not in sys.modules:
        skl = types.ModuleType('sklearn')
        decomposition = types.ModuleType('sklearn.decomposition')
        class Dummy:
            def __init__(self, *a, **k):
                pass
        decomposition.LatentDirichletAllocation = Dummy
        feature = types.ModuleType('sklearn.feature_extraction.text')
        feature.CountVectorizer = Dummy
        skl.decomposition = decomposition
        skl.feature_extraction = types.SimpleNamespace(text=feature)
        sys.modules['sklearn'] = skl
        sys.modules['sklearn.decomposition'] = decomposition
        sys.modules['sklearn.feature_extraction'] = types.ModuleType('sklearn.feature_extraction')
        sys.modules['sklearn.feature_extraction.text'] = feature

    # pandas_gbq
    if 'pandas_gbq' not in sys.modules:
        pandas_gbq = types.ModuleType('pandas_gbq')
        pandas_gbq.read_gbq = lambda *a, **k: []
        sys.modules['pandas_gbq'] = pandas_gbq

    # psutil
    if 'psutil' not in sys.modules:
        psutil = types.ModuleType('psutil')
        psutil.virtual_memory = lambda: types.SimpleNamespace(available=0)
        sys.modules['psutil'] = psutil

    # If utils was imported before stubbing requests, ensure it uses the stub
    if 'utils' in sys.modules:
        utils = sys.modules['utils']
        utils.requests = sys.modules['requests']

    yield


@pytest.fixture
def main_module(stub_optional_dependencies):
    """Import the main module after stubs are in place."""
    if 'main' in sys.modules:
        return importlib.reload(sys.modules['main'])
    return importlib.import_module('main')


@pytest.fixture
def gas_module(stub_optional_dependencies):
    if 'pipelines.gas' in sys.modules:
        return importlib.reload(sys.modules['pipelines.gas'])
    return importlib.import_module('pipelines.gas')


@pytest.fixture
def dune_module(stub_optional_dependencies):
    if 'pipelines.dune' in sys.modules:
        return importlib.reload(sys.modules['pipelines.dune'])
    return importlib.import_module('pipelines.dune')


@pytest.fixture
def tweets_module(stub_optional_dependencies):
    if 'pipelines.tweets' in sys.modules:
        return importlib.reload(sys.modules['pipelines.tweets'])
    return importlib.import_module('pipelines.tweets')


@pytest.fixture
def db_module(stub_optional_dependencies):
    if 'pipelines.db' in sys.modules:
        return importlib.reload(sys.modules['pipelines.db'])
    return importlib.import_module('pipelines.db')


@pytest.fixture
def options_module(stub_optional_dependencies):
    if 'pipelines.options' in sys.modules:
        return importlib.reload(sys.modules['pipelines.options'])
    return importlib.import_module('pipelines.options')


@pytest.fixture
def advanced_module(stub_optional_dependencies):
    if 'advanced_pipeline' in sys.modules:
        return importlib.reload(sys.modules['advanced_pipeline'])
    return importlib.import_module('advanced_pipeline')
