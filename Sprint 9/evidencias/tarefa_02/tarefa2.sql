---------------------------------
create view dim_carro as
select
	idCarro,
	classiCarro,
	marcaCarro,
	modeloCarro,
	anoCarro,
	tc.idCombustivel,	
	tipoCombustivel
from tb_carro tc inner join tb_combustivel tcb on (tc.idCombustivel = tcb.idCombustivel);

CREATE TABLE dim_carro (
  idCarro INTEGER PRIMARY KEY,
  classiCarro VARCHAR(50),
  marcaCarro VARCHAR(80),
  modeloCarro VARCHAR(80),
  anoCarro INT,
  idCombustivel INT,
  tipoCombustivel VARCHAR(20)
);

---------------------------------

create view dim_cliente as
select
	idCliente,
	nomeCliente,
	cidadeCliente,
	estadoCliente,
	paisCliente
from tb_cliente tc 

CREATE TABLE dim_cliente (
  idCliente INT PRIMARY KEY,
  nomeCliente VARCHAR(100),
  cidadeCliente VARCHAR(40),
  estadoCliente VARCHAR(40),
  paisCliente VARCHAR(40)  
);

---------------------------------

create view dim_vendedor as
select
	idVendedor,
	nomeVendedor,
	sexoVendedor,
	estadoVendedor
from tb_vendedor tv 

CREATE TABLE dim_vendedor (
  idVendedor INT PRIMARY KEY,
  nomeVendedor VARCHAR(15),
  sexoVendedor SMALLINT,
  estadoVendedor VARCHAR(40)
);

---------------------------------

create view fato_locacao as
select
	idLocacao,
	idCliente,
	idCarro,
	dataLocacao,
	horaLocacao,
	kmCarro,
	qtdDiaria,
	vlrDiaria,
	dataEntrega,
	horaEntrega,
	idVendedor
from tbl_locacao tl;

CREATE TABLE fato_locacao (
  idLocacao INT PRIMARY KEY,
  idCliente INT,
  idCarro INT,  
  kmCarro INT,
  qtdDiaria INT,
  vlrDiaria DECIMAL(18,2),  
  idVendedor INT,
  FOREIGN KEY (idCliente) REFERENCES dim_cliente(idCliente),
  FOREIGN KEY (idCarro) REFERENCES dim_carro(idCarro),
  FOREIGN KEY (idVendedor) REFERENCES dim_vendedor(idVendedor),
  FOREIGN KEY (idLocacao) REFERENCES dim_tempo(idLocacao)
);

---------------------------------

CREATE TABLE dim_tempo (
  idLocacao INT PRIMARY KEY, 
  dataLocacao DATETIME,
  horaLocacao TIME,
  dataEntrega DATE,
  horaEntrega TIME  
);