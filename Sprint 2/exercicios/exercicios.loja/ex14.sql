--Apresente a query para listar o gasto médio por estado da federação.
-- As colunas presentes no resultado devem ser estado e gastomedio. Considere apresentar a coluna gastomedio arredondada na segunda casa decimal e ordenado de forma decrescente.
--Observação: Apenas vendas com status concluído.

select
    t.estado,
    round(avg(t.qtd * t.vrunt), 2) as gastomedio
from
    tbvendas t
where
    t.status = 'Concluído'
group by
    t.estado
order by
    gastomedio desc;