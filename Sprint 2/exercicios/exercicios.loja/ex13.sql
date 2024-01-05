--Apresente a query para listar os 10 produtos menos vendidos pelos canais de E-Commerce ou Matriz (Considerar apenas vendas concluídas).
--As colunas presentes no resultado devem ser cdpro, nmcanalvendas, nmpro e quantidade_vendas.

select
	t.cdpro,
	t.nmcanalvendas, 
	t.nmpro,
	sum(t.qtd) as quantidade_vendas
from tbvendas ts
where t.status = 'Concluído' and t.nmcanalvendas IN ('Matriz', 'Ecommerce')
group by
	t.cdpro, t.nmcanalvendas, t.nmpro  
order by 
	quantidade_vendas;