SELECT
    idTransactionCart AS idTransacaoProduto,
    idTransaction AS idTransacao,
    NameProduct AS descNomeProduto,
    QuantityProduct AS nrQuantidadeProduto

FROM bronze.upsell.transactions_product