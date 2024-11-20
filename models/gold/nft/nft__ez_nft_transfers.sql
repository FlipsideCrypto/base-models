{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'NFT' } } }
) }} 

SELECT
    block_timestamp,
    block_number,
    tx_hash,
    1 AS tx_position,
    -- new
    event_index,
    intra_event_index,
    event_type,
    --deprecate
    IFF(
        event_type = 'mint',
        TRUE,
        FALSE
    ) AS is_mint,
    -- new
    contract_address AS nft_address,
    -- deprecate
    contract_address,
    -- new
    project_name,
    --deprecate
    project_name AS NAME,
    -- new
    from_address AS nft_from_address,
    --deprecate
    to_address AS nft_to_address,
    --deprecate
    from_address,
    -- new
    to_address,
    -- new
    tokenId,
    -- deprecate
    tokenId AS token_id,
    -- new
    erc1155_value,
    -- deprecate
    erc1155_value AS quantity,
    -- new
    IFF(
        erc1155_value IS NOT NULL,
        'erc721',
        'erc1155'
    ) AS token_standard,
    -- new
    '0x' AS origin_function_signature,
    --new
    '0x' AS origin_from_address,
    --new
    '0x' AS origin_to_address,
    --new
    COALESCE (
        nft_transfers_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_hash','event_index','intra_event_index']
        ) }}
    ) AS ez_nft_transfers_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver__nft_transfers') }}
