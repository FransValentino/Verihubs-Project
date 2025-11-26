from dagster import Definitions, load_assets_from_modules

from verihubs_dagster import assets_load_csv, assets_create_visualization  # noqa: TID252

all_assets = load_assets_from_modules([assets_load_csv, assets_create_visualization])

defs = Definitions(
    assets=all_assets,
)
