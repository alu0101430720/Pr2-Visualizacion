from dagster import Definitions, load_assets_from_modules
import assets
import checks_p5

all_assets = load_assets_from_modules([assets, checks_p5])

defs = Definitions(
    assets=all_assets,
)