SELECT DATE('{dt_ref}') AS dtRef,
       t2.descNomeProduto,
       count(DISTINCT t1.idTransacao) AS nrQuantidadeTrasacoes,
       count(DISTINCT t1.idCliente) AS nrQuantidadeClientes,
       count(DISTINCT t1.idTransacao) / count(DISTINCT t1.idCliente) AS nrQuantidadeTransacaoCliente,
       sum(t1.nrPontosTransacao) AS nrQuantidadePontos,
       sum(CASE WHEN t1.nrPontosTransacao > 0 THEN t1.nrPontosTransacao ELSE 0 END) AS nrQuantidadePontosPos,
       sum(CASE WHEN t1.nrPontosTransacao < 0 THEN t1.nrPontosTransacao ELSE 0 END) AS nrQuantidadePontosNeg

FROM silver.upsell.transacoes AS t1

LEFT JOIN silver.upsell.transacao_produto AS t2
ON t1.idTransacao = t2.idTransacao

WHERE DATE(t1.dtTransacao) <= '{dt_ref}'
AND DATE(t1.dtTransacao) > '{dt_ref}' - INTERVAL 28 DAY

GROUP BY dtRef, t2.descNomeProduto GROUPING SETS ((dtRef, t2.descNomeProduto), (dtRef))
ORDER BY dtRef, t2.descNomeProduto