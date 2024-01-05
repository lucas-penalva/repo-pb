--Apresente a query para listar código, nome e data de nascimento dos dependentes do vendedor com menor valor total bruto em vendas (não sendo zero). 
--As colunas presentes no resultado devem ser cddep, nmdep, dtnasc e valor_total_vendas. Observação: Apenas vendas com status concluído.
   
 with menorVendedor as (
    select
        t.cdvdd,
        coalesce(sum(t2.qtd * t2.vrunt), 0) as valor_total_vendas
    from
        tbvendedor t
    left join
        tbvendas t2 on t.cdvdd = t2.cdvdd
    where
        t2.status = 'Concluído'
    group by
        t.cdvdd
    having
        valor_total_vendas > 0
    order by
        valor_total_vendas
    limit 1
)

select
    td.cddep,
    td.nmdep,
    td.dtnasc,
    valor_total_vendas
from
    tbdependente td
inner join
    menorVendedor mv on td.cdvdd = mv.cdvdd
order by
    td.cddep 