# Automacao Processo de ETL base de reclamacoes

## Objetivo 🎯

Automatizat a ingestão de dados da base de dados referente a reclamaçãoes de clientes, a partir de um arquivo CSV, realizando extração, tratamento, carga e controle do processo via ETL e SQL.

## Tecnologias e Ferramentas 🛠

* **Banco de Dados:** SQL Server
* **Agendamento:** SQL Server Agente Jobs
* **Importação:** Comando BULK INSERT


## System Design ✍🏼

![Pipeline](Pipeline%20(2).png)

1. **Importação:** Leitura do CSV via BULK INSERT para #TEMP.
2. **Filtragem:** Criação da #STAGE com registros válidos (últimos 18 meses e TIPO_ATENDIMENTO não nulo).
3. **Preparação da base final:** TRUNCATE da tabela tb_anatel_reclamacoes.
4. **Carga final:** Inserção dos dados da #STAGE com conversões de tipo.
5. **Log de execução:** Registro em TB_PROCS_LOG.
6. **Limpeza final:** Remoção das tabelas temporárias.


## Detalhes Técnicos ⚙

### Fonte de Dados

* Local: \\SNEPDB56C01\Repositorio\BDS\0044 - IMPORTACAO_ANATEL_RECLAMACOES\0001 - ENTRADAS\
* Arquivo: AnatelConsumidorReclamacoesCSV.CSV
* Codificação: UTF-8
* Delimitador: ;

### Transformações

* Filtro na #STAGE:
  * TIPO_ATENDIMENTO IS NOT NULL
  * DATA_FINALIZACAO >= GETDATE() - 18 meses
* Conversões explícitas de tipos (ex: VARCHAR → NVARCHAR, VARCHAR → DATETIME, VARCHAR → SMALLINT).


### Base Final

* Tabela destino: tb_anatel_reclamacoes
* Carga via: INSERT INTO ... SELECT FROM #STAGE


## Monitoramento ✅

* Tabela de Log: TB_PROCS_LOG
* Nome do Processo: RCCM_IMPORT_ANATEL_CONSUMIDOR_RECLAMACOES
* Nome da Procedure: PR_ANATEL_CONSUMIDOR_RECLAMACOES
* Horário de Execução: 11:00


