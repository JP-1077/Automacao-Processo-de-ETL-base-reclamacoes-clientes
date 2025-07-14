

/*
DESENVOLVEDOR: Jo�o Pedro Mendes Fonseca
Objetivo: Processo ETL automatizado para importar, tratar e carregar dados da base Anatel Consumidor Reclama��es
Funcionalidades:
	- Limpeza na tabela temporaria 
	- Importa dados brutos de um arquivo CSV com codifica��o UTF-8 localizado em rede
	- Realizo uma tratativa na base de dados que vai ser direcionada para a tabela de STAGE.
	- Limpa a tabela final para deixa-l� pronta para receber os novos dados.
	- Insere dados da STAGE na tabela final tb_anatel_reclamacoes
	- Registra o log de execu��o em TB_PROCS_LOG para controle de execu��o.
	- Remove tabelas tempor�rias ao final do processo e limpa a tabela final tb_anatel_reclamacoes.

Execu��o: A procedure que armazena todo esse processo � denominada como PR_ANATEL_CONSUMIDOR_RECLAMACOES e � executada diariamente por meio de um SQL Server Agent Job agendado.

Origem dos Dados: Arquivo CSV.

Tabelas Envolvidas:
	- Tempor�rias: #TEMP e #STAGE
	- Tabela final: tb_anatel_reclamacoes
*/

/*=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-*/
												/* ETAPA 1: LIMPEZA DE #TEMP */
/*=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-*/

	-- Remove a tabela tempor�ria #TEMP se ela existir para garantir que a execu��o seja limpa
	IF OBJECT_ID('tempdb..#TEMP', 'U') IS NOT NULL    DROP TABLE #TEMP;


/*=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-*/
												/* ETAPA 2: CONFIGURA��ES DE LOG */
/*=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-*/
	-- Armazena o hor�rio de in�cio do processo.
	DECLARE @START DATETIME = CAST(GETDATE() AS DATETIME)

	-- Define o nome do processo para rastreamento de log.
	DECLARE @PROCESS_NAME VARCHAR(MAX) = 'RCCM_IMPORT_ANATEL_CONSUMIDOR_RECLAMACOES'


/*=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-*/
												/* ETAPA 3: BLOCO DE CARGA */
/*=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-*/

-- Cria��o da tabela tempor�ria #TEMP que receber� os dados brutos do CSV.
CREATE TABLE #TEMP
(
TIPO_ATENDIMENTO VARCHAR(500),
SERVICO VARCHAR(500),
ASSUNTO VARCHAR(500),
UF VARCHAR(500),
PRESTADORA VARCHAR(500),
DATA_ABERTURA VARCHAR(500),
DATA_RESPOSTA VARCHAR(500),
RESPONDIDA VARCHAR(500),
REABERTA VARCHAR(500),
PrazoResposta VARCHAR(500),
ID_SITUACAO VARCHAR(500),
SITUACAO VARCHAR(500),
RESOLVIDA VARCHAR(500),
NOTA_CONSUMIDOR VARCHAR(500),
DATA_FINALIZACAO VARCHAR(500),
DETALHAMENTO VARCHAR(500),
ENTIDADEANDAMENTO VARCHAR(500)
);
	

/*=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-*/
											 /* ETAPA 4: VARIAVEIS DE CONTROLE DE ARQUIVOS */
/*=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-*/
	
	-- Armazena o caminho onde o arquivo CSV est� localizado.
	DECLARE @PATH NVARCHAR(MAX) = '\\SNEPDB56C01\Repositorio\BDS\0044 - IMPORTACAO_ANATEL_RECLAMACOES\0001 - ENTRADAS\'

	-- Nome do arquivo a ser importado.
	DECLARE @FILE NVARCHAR(MAX) = 'AnatelConsumidorReclamacoesCSV.CSV'

	-- Concatena as duas variaveis acima para termos o caminho completo do arquivo.
	DECLARE @FULLPATH NVARCHAR(MAX) = @PATH + @FILE
	
	-- Comando de importa��o usando BULK INSERT com modifica��o UTF - 8 e delimitador ";".
	DECLARE @SQL NVARCHAR(MAX) = ''
	SET @SQL = N'

	BULK INSERT #TEMP
	FROM ''' + @FullPath + '''
	WITH (
	FIELDTERMINATOR = '';'',
	ROWTERMINATOR = ''0x0a'',
	FIRSTROW = 2,
	CODEPAGE = ''65001''
�����		)';
	
	-- Executa o comando de importa��o 
	EXEC SP_EXECUTESQL @SQL;

/*=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-*/
															/* ETAPA 5: TRATAMENTO */
/*=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-*/

	-- Cria a tabela de STAGE com base nos dados da tabela #TEMP. Desta forma, ela aplica dois filtros:
		-- 1. Apenas registros com valor diferente de NULL na coluna TIPO_ATENDIMENTO.
		-- 2. Apenas registros com DATA_FINALIZACAO nos �ltimos 18 meses, a partir da data atual.

	SELECT *
	INTO #STAGE
	FROM #TEMP
	WHERE TIPO_ATENDIMENTO IS NOT NULL 
	AND CAST(DATA_FINALIZACAO AS DATE) >= CAST(DATEADD(MONTH, -18, GETDATE())AS DATE);

	
/*=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-*/
															/* ETAPA 6: REGRA DE N�GOCIO */
/*=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-*/
	
	-- Limpa a tabela final para que ela esteja devidamente preparada para receber os dados da STAGE.

	TRUNCATE TABLE tb_anatel_reclamacoes


/*=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-*/
															  /* ETAPA 7: LOAD */
/*=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-*/

-- Insere os dados tratados da tabela tempor�ria #STAGE na tabela final tb_anatel_reclamacoes.
-- Durante a inser��o, foi aplicado convers�es expl�citas nos tipos de dados para garantir consist�ncia com a estrutura da tabela de destino.

INSERT INTO tb_anatel_reclamacoes
SELECT
    CAST(TIPO_ATENDIMENTO AS NVARCHAR (24)) AS TIPO_ATENDIMENTO,
    CAST(SERVICO AS NVARCHAR (34)) AS SERVICO,
    CAST(ASSUNTO AS NVARCHAR (70)) AS ASSUNTO,
    CAST(UF AS NVARCHAR (4)) AS UF,
	CAST(PRESTADORA AS NVARCHAR (10)) AS PRESTADORA,
    CAST(DATA_ABERTURA AS DATETIME) AS DATA_ABERTURA,
    CAST(DATA_RESPOSTA AS DATETIME) AS DATA_RESPOSTA,
    CAST(RESPONDIDA AS NVARCHAR (2)) AS RESPONDIDA,
    CAST(REABERTA AS NVARCHAR (2)) AS REABERTA,
    CAST(PrazoResposta AS SMALLINT) AS PrazoResposta,
    CAST(ID_SITUACAO AS SMALLINT) AS ID_SITUACAO,
    CAST(SITUACAO AS nvarchar (52)) AS SITUACAO,
    CAST(RESOLVIDA AS NVARCHAR (8)) AS RESOLVIDA,
    CAST(NOTA_CONSUMIDOR AS NVARCHAR (8)) AS NOTA_CONSUMIDOR,
    CAST(DATA_FINALIZACAO AS DATETIME) AS DATA_FINALIZACAO,
    CAST(DETALHAMENTO AS NVARCHAR (94)) AS DETALHAMENTO,
    CAST(ENTIDADEANDAMENTO AS NVARCHAR (24)) AS ENTIDADEANDAMENTO
FROM #STAGE



/*=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-*/
																/* ETAPA 8: LOG POSITIVO */
/*=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-*/

-- Inserir um log na tabela de monitoramento de processos (TB_PROCS_LOG). Para termos um registro quando o processo for executado com sucesso.
	insert into TB_PROCS_LOG
	values (
	@PROCESS_NAME, --processo
	@START, --horario start
	cast(getdate() as datetime), -- horario end
	'OK', --status
	NULL
)


/*=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-*/
														/* ETAPA 9: LIMPEZA DE STAGE, #TEMP E DELETE DO ARQUIVO */
/*=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-*/

	-- Remove a tabela tempor�ria utilizadas.
	IF OBJECT_ID('tempdb..#TEMP', 'U') IS NOT NULL    DROP TABLE #TEMP;

	-- Realiza uma limpeza na tabela de STAGE.
	IF OBJECT_ID('tempdb..#STAGE', 'U') IS NOT NULL    DROP TABLE #STAGE;
	


