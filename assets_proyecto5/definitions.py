from dagster import Definitions, load_assets_from_modules, load_asset_checks_from_modules
import assets
import checks_p5
import plots_assets
import plots_assets_contratos_canarias

all_assets = load_assets_from_modules([assets, plots_assets, plots_assets_contratos_canarias])
all_checks = load_asset_checks_from_modules([checks_p5])

defs = Definitions(
    assets=all_assets,
    asset_checks=all_checks,
)