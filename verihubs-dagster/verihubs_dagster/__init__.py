from dagster import Definitions, load_assets_from_modules
from . import assets_load_csv

all_assets = load_assets_from_modules([assets_load_csv])

defs = Definitions(
    assets=all_assets
)