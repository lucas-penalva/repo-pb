--Apresente a query para listar o código e nome cliente com maior gasto na loja.
--As colunas presentes no resultado devem ser cdcli, nmcli e gasto, esta última representando o somatório das vendas (concluídas) atribuídas ao cliente.

SELECT 	
	t.cdcli,
	t.nmcli,
	sum(t.qtd * t.vrunt) as gasto
from tbvendas t 
group by
	t.cdcli, nmcli 
order by
	gasto desc
limit 1;