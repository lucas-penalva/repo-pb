--Apresente a query para listar as 5 editoras com mais livros na biblioteca. O resultado deve conter apenas as colunas quantidade, nome, estado e cidade.
-- Ordenar as linhas pela coluna que representa a quantidade de livros em ordem decrescente.

SELECT 
    count(l.cod) as quantidade,
    e.nome,
    adr.estado,
    adr.cidade
from 
    editora e
left join 
    livro l on e.codeditora = l.editora
left join 
    endereco adr on e.endereco = adr.codendereco
group by 
    e.codeditora, e.nome, adr.estado, adr.cidade
order by 
    quantidade desc
limit 2;