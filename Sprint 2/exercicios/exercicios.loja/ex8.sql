--Apresente a query para listar o código e o nome do vendedor com maior número de vendas (contagem), e que estas vendas estejam com o status concluída.
--As colunas presentes no resultado devem ser, portanto, cdvdd e nmvdd.

select 
	t.cdvdd,
	t.nmvdd
from
	tbvendedor t
left join 
	tbvendas t2 on t.cdvdd = t2.cdvdd
where
	status = 'Concluído'
group by
	t.cdvdd, t.nmvdd 
order by
	count(t2.cdven) desc
limit 1;