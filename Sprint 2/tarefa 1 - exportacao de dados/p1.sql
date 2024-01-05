--Exportar o resultado da query que obtém os 10 livros mais caros para um arquivo CSV. Utilizar o caractere; (ponto e vírgula) como separador.
--Lembre-se que o conteúdo do seu arquivo deverá respeitar a sequência de colunas e seus respectivos nomes de cabeçalho que listamos abaixo:
--CodLivro Titulo CodAutor NomeAutor Valor CodEditora NomeEditora

select
	l.cod as CodLivro,
	l.titulo as Titulo,
	a.codautor as CodAutor,
	l.autor as NomeAutor,
	l.valor as Valor,
	edt.codeditora as CodEditora,
	edt.nome as NomeEditora
from 
	autor a	
left join 
	livro l on a.codautor = l.autor 
left join 
	editora edt on l.editora = edt.codeditora
left join
	endereco e on edt.endereco = e.codendereco 
order by 
	l.valor desc
limit 10;