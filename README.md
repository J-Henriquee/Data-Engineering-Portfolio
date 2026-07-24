# 🛠️ Data Engineering Portfolio | Nean

Bem-vindo ao meu portfólio central de Engenharia de Dados! 🚀

Este repositório documenta a minha jornada e evolução na construção de infraestruturas de dados. O objetivo principal aqui é consolidar projetos práticos envolvendo todo o ciclo de vida do dado: desde a extração, processamento (ETL/ELT), orquestração, até a modelagem e disponibilização em ambientes Cloud.

## 🧰 Stack Tecnológico

* **Linguagens:** Python, SQL, Bash
* **Bancos de Dados & Modelagem:** PostgreSQL, Modelagem Dimensional (Star Schema)
* **Processamento & Transformação:** Pandas, PySpark, SQLAlchemy, psycopg2
* **Infraestrutura & Orquestração:** Linux (Cron)
* **Cloud & Big Data (Em andamento):** AWS (S3, Redshift Serverless)

## 📂 Índice de Projetos

Abaixo estão os projetos desenvolvidos, organizados do mais recente para o mais antigo. Clique no nome do projeto para ver o código e a documentação detalhada.

| Status | Projeto | Tecnologias Principais | Descrição Curta |
| :---: | :--- | :--- | :--- |
| 🚧 | [🛒 E-commerce Olist AWS Pipeline (Em Construção)](./03_pipeline_olist_aws) | Python, PySpark, AWS S3, Redshift | Construção de um Data Lakehouse na AWS. Ingestão de dados reais de e-commerce no S3, processamento distribuído com Spark e modelagem relacional no Amazon Redshift Serverless. |
| ✅ | [⚽ Pipeline Copa do Mundo: Modelagem e ETL](./02_pipeline_copa) | Python, Pandas, PostgreSQL, SQL | Pipeline ETL estruturado com constraints relacionais. Implementação de Star Schema, tratamento de violações de chaves (Unique/NotNull) e carga automatizada no banco de dados. |
| ✅ | [⛩️ Pipeline Jikan API: Top 1000 Anime](./01_pipeline_jikan) | Python, Pandas, Cron | Pipeline ETL end-to-end com extração via API, tratamento de paginação, limpeza de dados nulos/duplicados e orquestração autônoma no Ubuntu. |

---
Fique à vontade para explorar os códigos. Feedback e conexões são sempre bem-vindos!
