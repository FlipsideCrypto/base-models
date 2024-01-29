{{ config(
    materialized = 'view'
) }}

SELECT
    LOWER('0x46e6b214b524310239732D51387075E0e70970bf') AS compound_market_address,
    'Compound WETH' AS compound_market_name,
    'cWETHv3' AS compound_market_symbol,
    18 AS compound_market_decimals,
    LOWER('0x2Ae3F1Ec7F1F5012CFEab0185bfc7aa3cf0DEc22') AS underlying_asset_address,
    'Coinbase Wrapped Staked ETH' AS underlying_asset_name,
    'cbETH' AS underlying_asset_symbol,
    18 AS underlying_asset_decimals
UNION ALL
SELECT
    LOWER('0x9c4ec768c28520B50860ea7a15bd7213a9fF58bf') AS compound_market_address,
    'Compound USDbC' AS compound_market_name,
    'cUSDbCv3' AS compound_market_symbol,
    6 AS compound_market_decimals,
    LOWER('0xd9aAEc86B65D86f6A7B5B1b0c42FFA531710b6CA') AS underlying_asset_address,
    'USD Base Coin' AS underlying_asset_name,
    'USDbC' AS underlying_asset_symbol,
    6 AS underlying_asset_decimals
