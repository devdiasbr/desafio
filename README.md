# Desafio de Engenharia de Dados - Pipeline de Vendas

Este projeto implementa um pipeline de dados de vendas completo seguindo a arquitetura Medalhão (Bronze, Silver, Gold), utilizando PySpark e Delta Lake. O projeto foi estruturado de forma modular e suporta **execução híbrida** (Local em Windows/Linux e Databricks).

## 📋 Descrição do Projeto

O objetivo é processar dados de vendas de forma incremental, garantindo qualidade, deduplicação e agregações para análise.

### Arquitetura Medalhão

1.  **Bronze (Ingestão):**
    *   Lê arquivos CSV brutos da pasta `dados_vendas`.
    *   Processamento incremental: lê apenas arquivos novos que ainda não foram processados.
    *   Adiciona metadados: `data_carga` (timestamp) e `nome_arquivo` (origem).
    *   Armazena os dados brutos em formato Delta.

2.  **Silver (Qualidade e Deduplicação):**
    *   Lê dados da camada Bronze.
    *   Remove duplicatas utilizando `Window functions` baseadas no `codigo_venda`, priorizando o registro mais recente.
    *   Utiliza operação `MERGE` (SCD Tipo 1) para atualizar registros existentes ou inserir novos na tabela Delta.

3.  **Gold (Modelagem e Agregação):**
    *   **Fato Vendas:** Tabela detalhada e limpa pronta para análise. Utiliza `MERGE` para manter consistência.
    *   **Vendas Agregadas:** Tabela sumarizada com o valor total de vendas agrupado por `Produto`, `Ano` e `Mês`. Também atualizada via `MERGE`.

## 🛠️ Tecnologias Utilizadas

*   **Python 3.x**
*   **PySpark**: Processamento distribuído de dados.
*   **Delta Lake**: Camada de armazenamento que traz confiabilidade (ACID) para Data Lakes.
*   **Databricks**: Plataforma unificada de análise de dados (compatível).
*   **Hadoop (Winutils)**: Binários necessários para rodar Spark no Windows (apenas local).
*   **Unittest**: Framework de testes automatizados.

## 🚀 Como Executar

### Opção 1: Execução Local (Windows/Linux)

#### Pré-requisitos
1.  Python instalado (versão 3.8 ou superior recomendada).
2.  Java instalado (JRE/JDK 8 ou 11) e configurado no `JAVA_HOME`.

#### Instalação
1.  Clone este repositório.
2.  Instale as dependências:
    ```bash
    pip install -r requirements.txt
    ```

#### Executando o Pipeline
Para rodar o pipeline completo:
```bash
python main.py
```

Para rodar limpando dados anteriores (reset):
```bash
python main.py --clean
```

### Opção 2: Execução no Databricks

1.  **Databricks Repos:**
    *   No workspace do Databricks, vá em "Repos" e clone este repositório.
    *   Certifique-se de que o cluster tenha suporte a PySpark e Delta Lake (padrão no Databricks Runtime).

2.  **Execução:**
    *   Você pode abrir o arquivo `main.py` e clicar em "Run".
    *   Ou criar um **Job** apontando para o arquivo `main.py` no Repo.
    *   Ou importar os módulos em um Notebook:
        ```python
        from main import main
        main()
        ```
    *   *Nota:* O código detectará automaticamente o ambiente e usará a `SparkSession` do cluster.

### Executando os Testes

Para validar a lógica (funciona em ambos os ambientes):

```bash
python -m unittest tests/test_pipeline.py
```

## 📂 Estrutura do Projeto

```text
.
├── dados_vendas/          # Arquivos CSV de entrada (Raw Data)
├── data/                  # Diretório de saída dos dados processados (Delta Tables)
│   ├── bronze/
│   ├── silver/
│   └── gold/
├── hadoop/                # Binários do Hadoop para Windows (winutils.exe)
├── src/                   # Código fonte do pipeline
│   ├── __init__.py
│   ├── bronze.py          # Lógica da camada Bronze
│   ├── silver.py          # Lógica da camada Silver
│   ├── gold.py            # Lógica da camada Gold
│   └── utils.py           # Configurações do Spark (Híbrido) e utilitários
├── tests/                 # Testes automatizados
│   └── test_pipeline.py   # Testes de integração do pipeline
├── main.py                # Orquestrador principal (Entry Point)
├── requirements.txt       # Dependências do projeto
└── README.md              # Documentação
```

## ⚙️ Detalhes de Implementação

*   **Suporte Híbrido:** O módulo `src/utils.py` detecta se o código está rodando localmente ou no Databricks (`DATABRICKS_RUNTIME_VERSION`) e ajusta as configurações automaticamente.
*   **Modularidade:** O código foi refatorado para o diretório `src/`, separando responsabilidades.
*   **Controle Incremental:** Na camada Bronze, o código verifica diretamente os arquivos já existentes na tabela Delta para evitar reprocessamento (idempotência sem arquivos de controle externos).
*   **Armazenamento Híbrido:** 
    *   **Local (Windows/Linux):** Utiliza o diretório `data/` para armazenar as tabelas Delta.
    *   **Databricks:** Utiliza **Tabelas Gerenciadas** dentro de um Database específico (`desafio_beca`), garantindo isolamento e organização no Hive Metastore / Unity Catalog.
*   **Idempotência:** As camadas Silver e Gold utilizam `MERGE` para garantir consistência.

## 📊 Schema dos Dados

**Bronze & Silver:**
*   `codigo_venda` (String): Identificador único.
*   `numero_fiscal` (Integer)
*   `id_produto` (Integer)
*   `nome_produto` (String)
*   `valor` (Double)
*   `timestamp_venda` (Timestamp)
*   `data_carga` (Timestamp)
*   `nome_arquivo` (String)

**Gold (Agregada):**
*   `nome_produto` (String)
*   `ano` (Integer)
*   `mes` (Integer)
*   `valor_total` (Double)
