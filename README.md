# 🛡️ Data Anonymization & ETL Pipeline - Hackathon LIAO 2026

## 📌 Sobre o Projeto
Este projeto consiste em um pipeline completo de Extração, Transformação e Carga (ETL) desenvolvido para a infraestrutura de dados do Hackathon LIAO 2026. O objetivo principal é processar dados meteorológicos brutos (INMET) e criar conjuntos de dados seguros para uma maratona de Machine Learning, garantindo a prevenção de vazamento de dados (*Data Leakage*) e ataques de re-identificação (*Fingerprinting*).

## ⚙️ Arquitetura e Transformações (Segurança de Dados)
Para garantir a integridade da competição, o script realiza as seguintes operações:
1. **Data Splitting Temporal:** Separação lógica entre a base de treino histórica (2000 a 2020) e o *Ground Truth* de teste (2021).
2. **Data Masking (Anonimização):** Remoção de *timestamps* e da variável-alvo (target) do conjunto de teste, substituindo-os por IDs sequenciais artificiais.
3. **Statistical Noise Injection (Jitter):** Aplicação de ruído estatístico (distribuição normal randômica) em variáveis contínuas (Temperatura, Umidade) para impossibilitar o cruzamento exato com bases públicas na internet (Engenharia Reversa).

## 🚀 Tecnologias Utilizadas
* **Python 3**
* **Pandas** (Manipulação e estruturação do Dataframe)
* **NumPy** (Cálculo e injeção de ruído matemático)

## 🔒 Nota sobre Versionamento
*Os arquivos `.csv` (base original e arquivos transformados) estão protegidos via `.gitignore` e não são versionados neste repositório por questões de segurança e confidencialidade do Hackathon.*