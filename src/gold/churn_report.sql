WITH tb_new AS (

  SELECT DISTINCT
        date('{dt_ref}')AS dtRef,
        t1.idCliente

  FROM silver.upsell.transacoes AS t1

  WHERE DATE(t1.dtTransacao) <= '{dt_ref}'
  AND DATE(t1.dtTransacao) > '{dt_ref}' - INTERVAL 28 DAY

),

tb_old AS (

  SELECT DISTINCT
        date('{dt_ref}' - INTERVAL 28 DAY)AS dtRef,
        t1.idCliente

  FROM silver.upsell.transacoes AS t1

  WHERE DATE(t1.dtTransacao) <= '{dt_ref}' - INTERVAL 28 DAY
  AND DATE(t1.dtTransacao) > '{dt_ref}' - INTERVAL 56 DAY
  
)

select date('{dt_ref}') AS dtRef,
       count(t1.idCliente) AS qtdeBaseOld,
       count(t2.idCliente) AS qtdeBaseNewNotChurn,
       count(t1.idCliente) - count(t2.idCliente)  AS nrQtdeChurn,
       1 - count(t2.idCliente) / count(t1.idCliente) AS ChurnRate

FROM tb_old as t1

LEFT JOIN tb_new AS t2
ON t1.idCliente = t2.idCliente

GROUP BY ALL