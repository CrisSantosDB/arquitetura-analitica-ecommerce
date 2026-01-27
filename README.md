# 🚀 Projeto de Engenharia de Dados para E-commerce  
### Transformando dados brutos em decisões de negócio

E-commerces operam em um ambiente altamente competitivo, onde **atrasos logísticos, cancelamentos, baixa conversão, dependência de poucos vendedores** e decisões baseadas em dados inconsistentes podem impactar diretamente o faturamento e a experiência do cliente.

Na prática, o problema **não é a falta de dados**, mas sim a **falta de dados confiáveis, organizados e prontos para análise**.

Este projeto foi desenvolvido com o objetivo de resolver esse problema, construindo uma **arquitetura de dados ponta a ponta**, capaz de transformar dados brutos em **insights acionáveis para as áreas de negócio**.

---

## 🎯 Objetivo do Projeto

Construir um **pipeline de dados escalável, automatizado e governado**, capaz de responder perguntas estratégicas como:

- Onde estão concentradas as vendas e os riscos do negócio?
- Quais categorias, vendedores e regiões geram mais valor — e mais problemas?
- Onde estamos perdendo dinheiro silenciosamente (frete, atrasos, cancelamentos)?
- O negócio está crescendo de forma sustentável ao longo do tempo?

---

## 🧠 Abordagem de Solução

A solução foi estruturada seguindo **boas práticas de Engenharia de Dados**, com separação clara de responsabilidades por camada.

### 🔹 Arquitetura em Camadas (Medallion Architecture)

- **Bronze**: ingestão de dados brutos diretamente do GCS, sem transformação.  
- **Silver**: tratamento, aplicação de regras de negócio, validações de qualidade e histórico (SCD Type 2).  
- **Gold**: dados modelados para consumo analítico, focados em métricas e decisões de negócio.

### 🔹 Automação e Orquestração

- **Apache Airflow (Cloud Composer)** para orquestrar todo o pipeline  
- **Dataproc + PySpark** para processamento distribuído  
- **CI/CD com GitHub + Cloud Build**, garantindo versionamento, rastreabilidade e deploy automático  

---

## 📊 Camada Gold – Dados prontos para decisão

Na camada Gold, foram construídas **tabelas analíticas** que permitem análises como:

- Ranking de vendas por categoria e vendedor  
- Faturamento, custos logísticos e eficiência operacional  
- Satisfação do cliente baseada em avaliações  
- Atrasos de entrega por região  
- Taxa de cancelamento por meio de pagamento  
- Evolução temporal do desempenho (mês a mês e ano a ano)  

Essas tabelas permitem que **analistas e áreas de negócio foquem em insights**, e não em tratamento de dados.

---
## 📁 Estrutura do Projeto
A organização do repositório segue boas práticas de Engenharia de Dados, separando responsabilidades e facilitando manutenção, escalabilidade e colaboração:

├── data/               # Arquivos de configuração, scripts de ingestão e definições das camadas no BigQuery (Bronze, Silver, Gold)
├── utils/              # Utilitários e funções auxiliares reutilizáveis (ex: scripts de apoio ao deploy e automação no GCP)
├── workflows/          # Orquestração dos pipelines (DAGs do Apache Airflow / Cloud Composer)
├── cloudbuild.yaml     # Configuração de CI/CD utilizando Google Cloud Build
├── pyproject.toml      # Gerenciamento de dependências e configurações do projeto Python
└── README.md           # Documentação geral do projeto

---
## 🛠️ Tecnologias Utilizadas

- Google Cloud Platform (GCP)  
- BigQuery  
- Cloud Storage  
- Dataproc  
- Cloud Composer (Airflow)  
- Cloud Build  
- Apache Spark / PySpark  
- SQL (BigQuery)  
- GitHub (CI/CD e versionamento)  

---

## 📄 Documentação Completa

📌 A documentação detalhada da arquitetura, modelagem e decisões técnicas está disponível aqui:  
👉 **[Documentação do Projeto no OneDrive](https://1drv.ms/o/c/1b3e289deb0309a3/IgCtRr4rHTD7RoLFmZ26gjrfAXyLSgcdYiG1ammasQks6vI?e=RQSRDM)**

---

## 💡 Diferencial do Projeto

Este projeto não foi desenvolvido apenas para **processar dados**, mas para **resolver problemas reais de negócio**, garantindo:

- Confiabilidade dos dados  
- Escalabilidade  
- Governança  
- Clareza entre dado técnico e decisão estratégica  
