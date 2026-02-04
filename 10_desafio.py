
# Databricks notebook source
# MAGIC %md
# MAGIC # Desafio: Pipeline de Dados de Vendas
# MAGIC 
# MAGIC ## Objetivo
# MAGIC Construir um pipeline de ingestão e processamento de dados de vendas, utilizando boas práticas de arquitetura em camadas (Bronze, Silver e Gold) e métodos eficientes de carga incremental.
# MAGIC 
# MAGIC ## Instruções
# MAGIC 1. **Ingestão dos Dados (Bronze):**
# MAGIC    - Ler os 100 arquivos de vendas disponíveis.
# MAGIC    - Utilize um dos métodos de carga disponíveis (ex: leitura direta, AutoLoader, etc.).
# MAGIC    - Adicione colunas extras: data de carga e nome do arquivo.
# MAGIC    - Garanta que a carga seja incremental, processando apenas arquivos novos (evite full load).
# MAGIC 
# MAGIC 2. **Transformação (Silver):**
# MAGIC    - Utilize o método de carga MERGE para atualizar/inserir dados na camada Silver.
# MAGIC    - Escolha livremente o tipo de tabela (particionada ou com liquid clustering).
# MAGIC    - Mantenha a lógica incremental, processando apenas dados novos.
# MAGIC 
# MAGIC 3. **Modelagem Analítica (Gold):**
# MAGIC    - Crie duas tabelas:
# MAGIC      - **Fato de Vendas:** tabela detalhada com todos os registros de vendas.
# MAGIC      - **Tabela Agregada:** tabela com o somatório da(s) medida(s) de interesse (ex: valor total de vendas por período, produto, etc.).
# MAGIC    - Assegure que ambas as tabelas sejam alimentadas de forma incremental.
# MAGIC 
# MAGIC ## Requisitos Gerais
# MAGIC - Todos os processos devem evitar cargas completas (full load), processando apenas dados novos a cada execução.
# MAGIC - Documente as decisões tomadas e os métodos utilizados.
# MAGIC - Utilize boas práticas de organização de código e versionamento.
# MAGIC - Fazer a orquestração das cargas das camadas com Databricks Workflows.
# MAGIC - Salvar os notebooks em um repositório Git conectado ao Databricks.
# MAGIC - Utilizar variáveis de ambiente para gerenciar caminhos e configurações.
# MAGIC - Implementar monitoramento básico para acompanhar o status das cargas (ex: logs simples).
# MAGIC - Garantir que o pipeline seja idempotente, ou seja, múltiplas execuções com os mesmos dados não devem causar duplicações ou inconsistências.
# MAGIC - Incluir tratamento de erros para capturar e registrar falhas durante o processo de ingestão e transformação.
# MAGIC - Validar os dados em cada etapa para assegurar a qualidade (ex: checar por valores nulos, formatos corretos, etc.).
# MAGIC - Utilizar notebooks separados para cada camada (Bronze, Silver, Gold) para melhor organização e manutenção do código.
# MAGIC - Documentar o código com comentários explicativos e utilizar markdown para descrever cada etapa do processo.
# MAGIC - Usar Spark com Formato Delta e sem uso de pandas.