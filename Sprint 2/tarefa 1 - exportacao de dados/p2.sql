--Exportar o resultado da query que obtém as 5 editoras com maior quantidade de livros na biblioteca para um arquivo CSV.
--Utilizar o caractere | (pipe) como separador.
--Lembre-se que o conteúdo do seu arquivo deverá respeitar a sequência de colunas e seus respectivos nomes de cabeçalho que listamos abaixo:
--CodEditora NomeEditora QuantidadeLivros

select
	e.codeditora as CodEditora,
	e.nome as NomeEditora,
	count(l.cod) as QuantidadeLivros	
from
	editora e 	
left join
	livro l on e.codeditora = l.editora
group by
	e.codeditora, e.nome 
order by
	QuantidadeLivros desc
limit 5