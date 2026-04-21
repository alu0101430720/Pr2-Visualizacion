from dagster import Definitions, load_assets_from_modules, load_asset_checks_from_modules
import assets
import checks_p5

all_assets = load_assets_from_modules([assets])
all_checks = load_asset_checks_from_modules([checks_p5])

defs = Definitions(
    assets=all_assets,
    asset_checks=all_checks,
)