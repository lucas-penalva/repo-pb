-- Crie uma consulta que lista os 3 nomes mais usados em cada década desde o 1950 até hoje.

/* 'contagem_nomes', no geral, representa quantas vezes cada nome é usado dentro de uma década. Ela agrupa os resultados por década, realizando uma contagem dos nomes.
A função 'row_number', e 'over partition, classifica os nomes conforme quantas vezes eles aparecem dentro de uma década ordenando do mais usado para o menos usado.*/ 

-- a função floor(), serve para arredondar determinado ano conforme o calculo efetuado.
-- Exemplo: (1985 / 10) * 10 = 198,5 * 10 = 1985 
-- a função cast(), converte os anos para valores inteiros, pois a função floor retornam cálculos que apresentam partes decimais, como demonstrado no exemplo acima.

-- A função rank seleciona os três nomes mais usados em cada década. É possível alterar o valor do rank para mostrar mais nomes usados dentro de cada década.

with contagem_nomes as (
    select 
        nome,
        cast(floor(ano / 10) * 10 as integer) as ano_decada,
        sum(total) as quantidade,
        row_number() over(partition by cast(floor(ano / 10) * 10 as integer) order by sum(total) desc) as rank
    from 
        users
    where 
        (ano >= 1950 and ano < 1960) or
        (ano >= 1960 and ano < 1970) or
        (ano >= 1970 and ano < 1980) or
        (ano >= 1980 and ano < 1990) or
        (ano >= 1990 and ano < 2000) or
        (ano >= 2000 and ano < 2010) or
        (ano >= 2010 and ano < 2015)
    group by 
        cast(floor(ano / 10) * 10 as integer), nome
)
select 
    nome,
    ano_decada as ano,
    quantidade
from 
    contagem_nomes
where 
    rank <= 3
order by
    ano_decada,
    quantidade desc;