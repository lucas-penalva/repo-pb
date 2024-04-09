---------------------------------------------------------------------------------------------
-- visualizando informações da entidade
select * from tb_locacao tl

---------------------------------------------------------------------------------------------

-- criando tabela combustivel
CREATE TABLE tb_combustivel (
  idCombustivel int primary key,
  tipoCombustivel varchar(20)
);

-- inserindo os dados com base na tabela fornecida
INSERT INTO tb_combustivel (idCombustivel, tipoCombustivel)
SELECT DISTINCT idcombustivel, tipoCombustivel
FROM tb_locacao;

select * from tb_combustivel tc 

---------------------------------------------------------------------------------------------

CREATE TABLE tb_cliente (
  idCliente INT PRIMARY KEY,
  nomeCliente VARCHAR(100),
  cidadeCliente VARCHAR(40),
  estadoCliente VARCHAR(40),
  paisCliente VARCHAR(40)  
);

INSERT INTO tb_cliente (idCliente, nomeCliente, cidadeCliente, estadoCliente, paisCliente)
SELECT DISTINCT idCliente, nomeCliente, cidadeCliente, estadoCliente, paisCliente
FROM tb_locacao;

select * from tb_cliente tc 

---------------------------------------------------------------------------------------------

CREATE TABLE tb_carro (
  idCarro INTEGER PRIMARY KEY,
  classiCarro VARCHAR(50),
  marcaCarro VARCHAR(80),
  modeloCarro VARCHAR(80),
  anoCarro INT,
  idCombustivel INT,
  FOREIGN KEY (idCombustivel) REFERENCES tb_combustivel(idCombustivel)
);

-- inserindo dados
INSERT INTO tb_carro (idCarro, classiCarro, marcaCarro, modeloCarro, anoCarro, idCombustivel)
SELECT DISTINCT idCarro, classiCarro, marcaCarro, modeloCarro, anoCarro, idCombustivel
FROM tb_locacao;

select * from tb_carro tc

---------------------------------------------------------------------------------------------

CREATE TABLE tb_vendedor (
  idVendedor INT PRIMARY KEY,
  nomeVendedor VARCHAR(15),
  sexoVendedor SMALLINT,
  estadoVendedor VARCHAR(40)
);

INSERT INTO tb_vendedor (idVendedor, nomeVendedor, sexoVendedor, estadoVendedor)
SELECT DISTINCT idVendedor, nomeVendedor, sexoVendedor, estadoVendedor
FROM tb_locacao;

select * from tb_vendedor tv 

---------------------------------------------------------------------------------------------

CREATE TABLE tbl_locacao (
  idLocacao INT PRIMARY KEY,
  idCliente INT,
  idCarro INT,
  dataLocacao DATETIME,
  horaLocacao TIME,
  kmCarro INT,
  qtdDiaria INT,
  vlrDiaria DECIMAL(18,2),
  dataEntrega DATE,
  horaEntrega TIME, 
  idVendedor INT,
  FOREIGN KEY (idCliente) REFERENCES tb_cliente(idCliente),
  FOREIGN KEY (idCarro) REFERENCES tb_carro(idCarro),
  FOREIGN KEY (idVendedor) REFERENCES tb_vendedor(idVendedor)    
);

INSERT INTO tbl_locacao (idLocacao, idCliente, idCarro, dataLocacao, horaLocacao, kmCarro, qtdDiaria, vlrDiaria, dataEntrega, horaEntrega, idVendedor)
SELECT idLocacao, idCliente, idCarro, dataLocacao, horaLocacao, kmCarro, qtdDiaria, vlrDiaria, dataEntrega, horaEntrega, idVendedor 
FROM tb_locacao;

select * from tbl_locacao tl

---------------------------------------------------------------------------------------------

-- Remoção das virgulas das datas de locação.
UPDATE tbl_locacao  SET dataLocacao = REPLACE(dataLocacao, ',', '');
-- Remoção das virgulas das datas de entrega.
UPDATE tbl_locacao  SET dataEntrega = REPLACE(dataEntrega, ',', '');

-- Conversão do formato de data para 'YYYY-MM-DD' nas datas de locação.
UPDATE tbl_locacao  SET dataLocacao = strftime('%Y-%m-%d', substr(dataLocacao,1,4)||'-'||substr(dataLocacao,5,2)||'-'||substr(dataLocacao,7,2));
-- Conversão do formato de data para 'YYYY-MM-DD' nas datas de entrega.
UPDATE tbl_locacao  SET dataEntrega = strftime('%Y-%m-%d', substr(dataEntrega,1,4)||'-'||substr(dataEntrega,5,2)||'-'||substr(dataEntrega,7,2));




















 