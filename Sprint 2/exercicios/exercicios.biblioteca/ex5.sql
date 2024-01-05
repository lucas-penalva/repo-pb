--Apresente a query para listar o nome dos autores que publicaram livros através de editoras NÃO situadas na região sul do Brasil. 
--Ordene o resultado pela coluna nome, em ordem crescente. Não podem haver nomes repetidos em seu retorno.

select 
	distinct a.nome	
from 
	autor a	
left join 
	livro l on a.codautor = l.autor 
left join 
	editora edt on l.editora = edt.codeditora
left join
	endereco e on edt.endereco = e.codendereco 	
where
	e.estado not in ('PARANÁ', 'RIO GRANDE DO SUL')
order by 
	a.nome asc;