--A comissão de um vendedor é definida a partir de um percentual sobre o total de vendas (quantidade * valor unitário) por ele realizado.
--O percentual de comissão de cada vendedor está armazenado na coluna perccomissao, tabela tbvendedor. 
--Com base em tais informações, calcule a comissão de todos os vendedores, considerando todas as vendas armazenadas na base de dados com status concluído.
--As colunas presentes no resultado devem ser vendedor, valor_total_vendas e comissao. O valor de comissão deve ser apresentado em ordem decrescente arredondado
--na segunda casa decimal.

select 
    t.nmvdd as vendedor,
    round(sum(tbv.qtd * tbv.vrunt), 2) as valor_total_vendas,
    round(sum(tbv.qtd * tbv.vrunt * t.perccomissao) / 100, 2) as comissao
from tbvendedor t 
left join
	tbvendas tbv on t.cdvdd = tbv.cdvdd
left join
	tbestoqueproduto tep on tep.cdpro = tbv.cdpro
where
	tbv.status = 'Concluído'
group by 
    t.cdvdd, 
    t.nmvdd, 
    t.perccomissao
order by
	comissao desc;