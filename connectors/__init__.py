from connectors.http_file import HttpCsvConnector
from connectors.local_file import LocalFileConnector

CONNECTOR_REGISTRY = {
    "http_csv": HttpCsvConnector,
    "local_file": LocalFileConnector,
}
