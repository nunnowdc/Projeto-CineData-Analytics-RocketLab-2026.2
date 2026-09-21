# 🎬 CineData Analytics — Pipeline de Dados End-to-End

Pipeline de dados completo construído no **Databricks**, seguindo a **Arquitetura Medalhão** (Bronze → Silver → Gold), que transforma um catálogo bruto de filmes (base combinada TMDB/IMDb) em um **Data Mart analítico** modelado em Star Schema para consumo por times de BI, e em uma **base de contexto textual** para alimentar um assistente de IA (RAG).

> Projeto desenvolvido para a capacitação **Engenharia de Dados — RocketLab 2026.2 (Visagio)**.

---

## 📌 Sobre o projeto

A CineData Analytics é uma empresa fictícia de inteligência de mercado do setor audiovisual. Os dados chegam **intencionalmente sujos e fragmentados** em 5 arquivos CSV, e o desafio consiste em estruturá-los na infraestrutura Data Lakehouse do Databricks.

O pipeline resolve, de ponta a ponta:

- **Ingestão** dos 5 CSVs + consumo da **API do Banco Central (PTAX)** para cotação do dólar
- **Limpeza e padronização** de dados com problemas reais: *column shift*, datas em múltiplos formatos, duplicatas com IDs distintos, valores numéricos contaminados por texto, separadores inconsistentes e campos ausentes
- **Modelagem dimensional** (Star Schema) com Surrogate Keys e Bridge Tables
- **Geração de contexto textual** para vetorização em um banco vetorial (RAG)
- **Orquestração** via Databricks Workflows, com dependências explícitas e agendamento

**Volume processado:** ~107 mil registros brutos por arquivo → **97.468 filmes únicos** na camada Gold.

---

## 🛠️ Tecnologias utilizadas

| Tecnologia | Uso no projeto |
|---|---|
| **Databricks** | Plataforma Lakehouse (Free Edition, compute Serverless) |
| **PySpark** | Processamento distribuído e transformações |
| **Delta Lake** | Formato de armazenamento de todas as tabelas (ACID, time travel, schema evolution) |
| **Unity Catalog** | Governança e organização em catalog/schemas |
| **Databricks Workflows (Jobs)** | Orquestração das tarefas com dependências e schedule |
| **Databricks Volumes** | Landing zone dos arquivos CSV |
| **API REST (BCB/PTAX)** | Ingestão da cotação do dólar via `requests` |
| **SQL** | Consultas analíticas e validações |

---

## 🏗️ Arquitetura Medalhão

A arquitetura organiza os dados em três camadas, cada uma com uma responsabilidade clara. O dado só avança de camada depois de cumprir o contrato da anterior.

```
   CSVs + API PTAX          BRONZE                 SILVER                  GOLD
   ───────────────    ─────────────────    ──────────────────    ────────────────────
   5 arquivos CSV  →  Cópia fiel do dado →  Dado limpo, tipado →  Modelagem dimensional
   API do BCB         (append-only)         e padronizado         (Star Schema + RAG)
                      + ingestion_datetime  (overwrite)           (overwrite)
```

### 🥉 Bronze — Ingestão bruta

Camada de **preservação do dado original**. Nenhuma transformação de negócio é aplicada: os CSVs são lidos como estão e gravados em Delta, com a adição da coluna de auditoria `ingestion_datetime`.

Usa modo **append**, então cada execução preserva o histórico de ingestões anteriores — o que torna a camada auditável e permite reprocessar o passado.

| Arquivo de origem | Tabela Bronze |
|---|---|
| `movies_info_TMDB_IMDB.csv` | `bronze.tb_movies_info` |
| `movies_financials_IMDB_TMDB.csv` | `bronze.tb_movies_financials` |
| `movies_metrics_IMDB_TMDB.csv` | `bronze.tb_movies_metrics` |
| `credits_and_tags_IMDB_TMDB.csv` | `bronze.tb_credits_and_tags` |
| `movies_reviews.csv` | `bronze.tb_movies_reviews` |
| API PTAX (Banco Central) | `bronze.tb_cotacao_dolar` |

> **Decisão:** na ingestão da API, optei por gravar **todas as chaves do JSON** em vez de selecionar apenas as colunas necessárias. A Bronze deve refletir a origem; a filtragem acontece na Silver.

### 🥈 Silver — Limpeza e padronização

Camada onde o dado vira **confiável**: nomes de colunas traduzidos para português, tipagem correta, deduplicação e tratamento de todas as inconsistências da origem. Usa modo **overwrite** (reconstrução completa a cada execução, garantindo idempotência).

| Tabela Silver | Principais tratamentos |
|---|---|
| `silver.tb_info_filmes` | Normalização e tradução de status; **deduplicação em duas camadas** (por ID e por chave de negócio); conversão de data multi-formato (ISO/US/BR); coluna derivada `ano_lancamento`; limpeza de resíduos de escape do CSV em título, sinopse e tagline; tipagem e saneamento da duração |
| `silver.tb_cotacao_dolar` | **Forward fill** para garantir série temporal contínua (a API não retorna cotação em fins de semana e feriados) |
| `silver.tb_financeiro_filmes` | Higienização de símbolos monetários e separadores; zeros e negativos tratados como ausentes; conversão para BRL; derivação de lucro e margem percentual com proteção contra divisão por zero |
| `silver.tb_metricas_engajamento` | Conversão numérica segura; notas fora do intervalo 0–10 invalidadas; **detecção de column shift na popularidade** |
| `silver.tb_avaliacoes_usuarios` | Deduplicação por combinação completa; validação da escala 0–10; comentários vazios padronizados como *"Sem comentário"* |
| `silver.tb_generos` | Normalização de separadores (vírgula vs. ponto e vírgula), `split` + `explode`, filtragem de ruído |
| `silver.tb_pessoas_empresas` | Explode das 4 colunas de crédito em modelo unificado (Ator, Diretor, Roteirista, Produtora); padronização de capitalização; remoção de resíduos de column shift |

Todas as tabelas passam por **checagens de qualidade (DQ checks)** automatizadas: unicidade de chave, ausência de nulos em campos obrigatórios e conformidade com regras de negócio.

### 🥇 Gold — Modelagem para consumo

Camada de **entrega**: o dado é modelado para responder perguntas de negócio com performance e clareza. Contém o Star Schema completo e a tabela de contexto para IA.

---

## ⭐ Star Schema

```
                              dim_genres
                                   │
                          bridge_movie_genre
                                   │
 dim_people ─── bridge_movie_person ─── dim_movies ─── fact_movies_performance
                                   │            │
                        bridge_movie_company   dim_reviews
                                   │
                            dim_companies
```

### Tabelas da camada Gold

| Tabela | Tipo | Registros | Descrição |
|---|---|---:|---|
| `gold.dim_movies` | Dimensão | 97.468 | Metadados descritivos de cada filme (PK: `sk_movie_id`) |
| `gold.dim_genres` | Dimensão | 19 | Catálogo deduplicado de gêneros |
| `gold.dim_people` | Dimensão | 418.972 | Pessoas envolvidas (Ator, Diretor, Roteirista) |
| `gold.dim_companies` | Dimensão | 44.910 | Catálogo de produtoras/estúdios |
| `gold.dim_reviews` | Dimensão | 97.468 | Avaliações de usuários consolidadas por filme |
| `gold.fact_movies_performance` | **Fato** | 97.468 | Métricas financeiras e de engajamento (grão: 1 filme) |
| `gold.bridge_movie_genre` | Bridge | 139.774 | Relação N:N filme ↔ gênero |
| `gold.bridge_movie_person` | Bridge | 753.819 | Relação N:N filme ↔ pessoa |
| `gold.bridge_movie_company` | Bridge | 117.602 | Relação N:N filme ↔ produtora |
| `gold.gold_genai_movies_context` | Contexto RAG | 97.468 | Documento textual consolidado para vetorização |

### Por que Surrogate Keys?

Todas as dimensões usam **chaves substitutas** geradas via `row_number()` sobre uma `Window` ordenada, produzindo sequências limpas (1, 2, 3...). A SK desacopla o modelo dimensional das chaves naturais da origem — se o TMDB mudar o formato dos seus IDs amanhã, o Star Schema não quebra.

### Por que Bridge Tables?

Um filme tem vários gêneros, vários atores e várias produtoras (e vice-versa). Colocar essas informações direto na `dim_movies` exigiria ou concatenar tudo numa string (perdendo a capacidade de filtrar e agregar) ou duplicar a linha do filme (quebrando o grão "1 linha = 1 filme"). As bridge tables resolvem isso: carregam **apenas o par de chaves estrangeiras**, preservando o grão de todas as tabelas conectadas.

### Tabela de contexto para IA

A `gold_genai_movies_context` gera, para cada filme, um documento em linguagem natural pronto para vetorização:

> *"O filme [TÍTULO], lançado no ano de [ANO], faturou [RECEITA] e teve um custo de [ORÇAMENTO]. Estrelado por [ATORES] e dirigido por [DIRETOR], o filme possui a seguinte sinopse: [SINOPSE]."*

O ponto crítico aqui é o tratamento de nulos: funções de concatenação retornam `NULL` para a **string inteira** se qualquer campo envolvido for nulo — um único diretor ausente faria o filme desaparecer silenciosamente da base de contexto. A solução foi aplicar `coalesce()` com **textos de fallback específicos por campo**, de forma que a ausência do dado seja informada de maneira honesta ao modelo (*"elenco não divulgado"*, *"valor de receita indisponível"*) em vez de omitida.

---

## ⚙️ Orquestração

O pipeline é orquestrado por um **Databricks Workflow** com três tarefas encadeadas por dependências explícitas:

```
to_Bronze ──→ to_Silver ──→ to_Gold
```

- **Agendamento:** diário às 06:00 (America/Sao_Paulo), simulando uma rotina real de atualização em produção
- **Compute:** Serverless
- **Configuração exportada:** [`job.yaml`](./job.yaml)
- **Evidência de execução:** [`print_execucao_job.png`](./print_execucao_job.png)

> **Nota sobre o modo append da Bronze:** como cada execução do Job reingere os CSVs, o volume da Bronze cresce a cada run. Isso é intencional (a camada é append-only por definição) e não afeta o resultado final — a deduplicação da Silver mantém apenas a versão mais recente de cada registro, com base em `ingestion_datetime`.

---

## 📊 Respostas das perguntas de negócio

### 1. Receita total (em R$) de todos os filmes da base

**R$ 834.730.511.068,54**

### 2. Top 5 filmes com maior popularidade

| Título | Popularidade |
|---|---:|
| blue beetle | 2.994,357 |
| Gran Turismo | 2.680,593 |
| The Nun II | 1.692,778 |
| Meg 2: The Trench | 1.567,273 |
| retribution | 1.547,220 |

### 3. Quantidade de filmes por gênero

| Gênero | Filmes | | Gênero | Filmes |
|---|---:|---|---|---:|
| Drama | 32.235 | | Science Fiction | 3.725 |
| Documentary | 18.959 | | Family | 3.720 |
| Comedy | 18.523 | | Mystery | 3.288 |
| Thriller | 10.226 | | Fantasy | 3.254 |
| Horror | 9.635 | | Adventure | 2.849 |
| Romance | 7.634 | | Music | 2.781 |
| Action | 5.975 | | History | 2.405 |
| Crime | 4.720 | | War | 955 |
| Animation | 4.406 | | Western | 409 |
| TV Movie | 4.075 | | | |

### 4. Top 10 filmes por receita (com `RANK()`)

| # | Título | Receita (US$) | Receita (R$) |
|---:|---|---:|---:|
| 1 | Avengers: Endgame | 2.800.000.000 | 14.439.320.000,00 |
| 2 | Avatar: The Way of Water | 2.320.250.281 | 11.965.298.674,09 |
| 3 | AVENGERS: INFINITY WAR | 2.052.415.039 | 10.584.099.114,62 |
| 4 | spider-man: no way home | 1.921.847.111 | 9.910.773.366,72 |
| 5 | The Lion King | 1.663.075.401 | 8.576.313.535,42 |
| 6 | Top Gun: Maverick | 1.488.732.821 | 7.677.246.284,61 |
| 7 | Barbie | 1.428.545.028 | 7.366.863.854,89 |
| 8 | The Super Mario Bros. Movie | 1.355.725.263 | 6.991.339.608,76 |
| 9 | Black Panther | 1.349.926.083 | 6.961.433.817,42 |
| 10 | Star Wars: The Last Jedi | 1.332.698.830 | 6.872.594.596,43 |

### 5. Ator com mais participações nos últimos 2 anos

**Suhas — 4 participações**

### 6. Produtora com maior lucro nos últimos 5 anos

**Universal Pictures — US$ 5.772.329.679**

> **Sobre o recorte temporal das perguntas 5 e 6:** conforme o enunciado, a data-limite superior é a **data de lançamento mais recente presente na base** (`2026-02-19`), e não a data de execução. Ao investigar a distribuição de filmes por ano, identifiquei que a base concentra ~10 mil filmes/ano até 2023, cai para 1.800 em 2024 e despenca para menos de 10 registros somados em 2025–2029 — indicando que ela foi construída por volta de meados de 2024, e que os registros posteriores são residuais.
>
> Como consequência, a janela de 2 anos definida por essa data máxima cai na região mais esparsa dos dados (874 filmes dos 97.468), o que explica o valor baixo do topo do ranking. Mantive a regra exatamente como especificada e registro aqui o artefato identificado.

---

## 🔍 Tratamento de dados e decisões técnicas

Esta seção documenta os problemas reais encontrados na base e o raciocínio por trás de cada decisão. Vários deles não estavam explícitos no enunciado — foram descobertos **validando os resultados** da camada Gold.

### Deduplicação em duas camadas

A deduplicação por `id` removia apenas duplicatas exatas. Ao validar a pergunta 5, percebi que um ator aparecia com 12 "participações" que eram, na verdade, **o mesmo filme repetido 12 vezes** com IDs diferentes. A base tinha 222 grupos de filmes duplicados (um deles com 25 cópias).

Adicionei uma segunda camada de deduplicação por **chave de negócio** (título normalizado + data de lançamento), removendo 411 registros fantasma.

**Detalhe importante:** essa dedup precisa rodar **depois** da conversão de datas. A origem tem datas em três formatos (ISO/US/BR), então o mesmo filme com a data escrita em formatos diferentes escapava da deduplicação quando a comparação era feita sobre a string bruta.

> **Por que não deduplicar apenas por título?** Uma abordagem mais agressiva (normalizar o título removendo tudo após os dois-pontos) colapsaria franquias inteiras: *Avengers: Endgame* e *Avengers: Infinity War* virariam a mesma chave, e um dos dois seria apagado silenciosamente — justamente dois filmes do top 3 de bilheteria. Limpeza agressiva demais destrói dado legítimo.

### Column shift — contaminação entre colunas

A base apresenta deslocamento de colunas, espalhando texto fora de contexto por campos numéricos. Casos identificados e tratados:

- **`popularidade` com valores de ano:** filmes apareciam no topo do ranking de popularidade com valores exatamente iguais ao seu ano de lançamento (2018, 2020). Popularidade real do TMDB nunca é um inteiro "redondo" — sempre tem casas decimais. Regra aplicada: valor inteiro exato dentro de faixa plausível de ano (1880–2030) → `NULL`.
- **`duracao_minutos` com nomes de pessoas:** a conversão numérica quebrava com valores como `" Conor McGregor"`. Aplicada conversão segura (o que não for inteiro válido vira `NULL`), além da tipagem correta para `INT`.
- **Nomes de pessoas/empresas com padrões de década e século:** valores como `1990s`, `12th Century` contaminavam o catálogo de pessoas. Filtro com regex **ancorado** (`^...$`), deliberadamente restritivo para não afetar nomes legítimos com números (`50 Cent`, `21 Savage`, `9m88`) nem empresas reais como `20th Century Studios`.

### Nulos: quando preencher e quando preservar

O tratamento de nulos foi decidido **caso a caso**, conforme o significado do dado ausente:

| Situação | Decisão | Justificativa |
|---|---|---|
| Filme sem avaliações | `qtd_avaliacoes = 0` | A contagem real é zero |
| Filme sem avaliações | `nota_media = NULL` | Não existe "nota zero" para quem não foi avaliado — preencher com 0 seria uma mentira estatística |
| Orçamento/receita ausente | `NULL` | Nulo aqui significa "não sabemos", não "custou zero" |
| Lucro com orçamento ou receita ausente | `NULL` | Um filme com receita conhecida e orçamento desconhecido apareceria com lucro integral, o que seria falso |
| Campos do documento de contexto (RAG) | Texto de fallback | Sem isso, a concatenação retornaria `NULL` e o filme sumiria da base de contexto |

### Títulos irrecuperáveis

54 filmes ficaram com `titulo = NULL` após a remoção de sujeira estrutural. Cheguei a implementar um fallback usando `titulo_original`, que recuperaria 10 deles — mas, ao **validar o resultado**, constatei que nesses registros o `titulo_original` também estava contaminado por column shift: o que entrava como "título" eram frases de sinopse (*"photos and archival footage"*, *"involving quotes from the French composer Erik Satie"*).

O fallback trocava um `NULL` honesto por um dado **ativamente errado**, que se propagaria para a `dim_movies` e para a base de contexto da IA. Decisão: manter os nulos e documentar.

### Capitalização de nomes próprios

Ao padronizar a capitalização das entidades, identifiquei que `initcap()` trata apenas espaços em branco como delimitador de palavra — nomes como `'weird Al' Yankovic` ficavam incorretos por causa do apóstrofo. Apliquei correção específica para esse caso.

Já para nomes iniciados por `#`, `&` e `(`, optei conscientemente por **não corrigir**: a inspeção manual mostrou que vários são estilizações intencionais (`#1nfluence`, `((o))eco`, `(pre)forma-se`), e uma correção automática descaracterizaria marcas reais.

### Escopo: o que não foi implementado (e por quê)

- **`dim_date`:** uma dimensão de data faz sentido quando há muitos eventos por dia (vendas, pedidos). Aqui o grão é "um registro por filme", e as datas relevantes são atributos descritivos do filme, não eixos de análise temporal de alta cardinalidade.
- **Views / Materialized Views:** o enunciado especifica o uso de `display()` para as perguntas de negócio, sem necessidade de persistir os resultados analíticos.

---

## 📁 Estrutura do repositório

```
.
├── Landing_to_Bronze.ipynb      # Ingestão dos CSVs + API PTAX → camada Bronze
├── Bronze_to_Silver.ipynb       # Limpeza, tipagem, deduplicação → camada Silver
├── Silver_to_Gold.ipynb         # Star Schema, contexto RAG e análises → camada Gold
├── job.yaml                     # Configuração exportada do Databricks Workflow
├── print_execucao_job.png       # Evidência da execução bem-sucedida do Job
└── README.md
```

---

## 📥 Dados de entrada

Os arquivos CSV de origem **não estão versionados neste repositório** (são material da capacitação e ultrapassam os limites de tamanho do GitHub). Para reproduzir o pipeline, é necessário disponibilizar os seguintes arquivos em um Volume do Databricks:

| Arquivo | Conteúdo | Volume aprox. |
|---|---|---:|
| `movies_info_TMDB_IMDB.csv` | Metadados dos filmes (título, data, duração, idioma, status, sinopse, tagline) | ~107 mil linhas |
| `movies_financials_IMDB_TMDB.csv` | Orçamento e receita | ~106 mil linhas |
| `movies_metrics_IMDB_TMDB.csv` | Popularidade, notas e contagem de votos (TMDB e IMDb) | ~107 mil linhas |
| `credits_and_tags_IMDB_TMDB.csv` | Elenco, direção, roteiro, produtoras e gêneros | ~106 mil linhas |
| `movies_reviews.csv` | Avaliações de usuários (nota e comentário) | ~32 mil linhas |

A cotação do dólar é obtida em tempo de execução via API do Banco Central (PTAX), não requerendo arquivo.

## ▶️ Como reproduzir

1. Criar um **Volume** no Databricks e fazer upload dos 5 arquivos CSV
2. Ajustar a variável `landing_path` no notebook `Landing_to_Bronze` para o caminho do seu Volume
3. Executar os notebooks na ordem: `Landing_to_Bronze` → `Bronze_to_Silver` → `Silver_to_Gold`
4. Alternativamente, importar o `job.yaml` para criar o Workflow completo com as dependências já configuradas

> Os notebooks são **idempotentes** nas camadas Silver e Gold (modo `overwrite`) e podem ser reexecutados livremente. A camada Bronze é append-only por definição da arquitetura.
