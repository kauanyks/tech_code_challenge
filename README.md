# Introdução 

Projeto desenvolvido a partir de um desafio de Engenharia de Dados, no qual havia a necessidade de criação de uma pipeline completa.

# O Desafio

Foram disponibilizados duas fontes de dados: um banco de dados Postgres e um arquivo CSV. O arquivo CSV representa detalhes das ordens de um sistema de ecommerce. O banco de dados é disponibilizado pela Microsoft para fins educacionais, chamado de Northwind, a única diferença é que a tabela order_detail não existe no banco, já que está sendo disponibilizada em CSV.

Schema do banco de dados original: 

![image](https://user-images.githubusercontent.com/49417424/105997621-9666b980-608a-11eb-86fd-db6b44ece02a.png)

O desafio é construir uma pipeline que extrai os dados todos os dias de ambas as fontes e escreve primeiramente no disco local e posteriormente em um banco de dados de sua escolha. 

É importante que todos os passos sejam isolados, ou seja, deve ser possível executar um step sem executar os demais.

## Passo 1

Para esse primeiro passo, no qual você deve escrecer os dados para o disco local, deve haver um arquivo para cada tabela do banco de dados e um para o arquivo CSV. Essa pipeline deve ter uma separação no caminho dos arquivos para cada fonte de dados (Postgres ou CSV), nome da tabela e data de execução, por exemplo:

```
/data/postgres/{table}/2021-01-01/file.format
/data/postgres/{table}/2021-01-02/file.format
/data/csv/2021-01-02/file.format
```

Você tem a liberdade de escolher o nome e formato de arquivos que serão utilizados.

## Passo 2

No segundo step, você deve carregar os dados do disco local e escrever no banco de dados escolhido. Lembre sempre de justificar as escolhas tomadas durante o processo.

## Passo 3

O objetivo final é realizar uma query que mostre as ordens e seus detalhes. As informações de ordens estão em uma tabela chamada "orders", que vem do banco de dados Northwind e os detalhes no arquivo CSV disponibilizado. O relacionamento das tabelas é através do campo "order_id".

A pipeline deve seguir esse formato:

![image](https://user-images.githubusercontent.com/49417424/105993225-e2aefb00-6084-11eb-96af-3ec3716b151a.png)


## Requisitos

- Todas as tasks devem ser idempotentes, a pipeline deve poder rodar mais de uma vez no dia e o resultado ser exatamente o mesmo
- O step 2 depende do 1, então não deve ser possível executar o segundo se o primeiro não ter sucesso
- Você deve extrair todas as tabelas das fontes disponibilizadas, independente se serão utilizadas posteriormente
- Deve ser possível identificar onde a pipeline falhar, caso der erro em algum step
- Deve ser disponibilizadas instruções para execução da pipeline; quando mais fácil, melhor
- Deve ser gerado um CSV ou JSON com o resultado da query final
- Não precisa agendar a execução da pipeline, mas deve ser possível executar em dias diferentes, ou seja, deve ser passado um argumento com a data desejada e a pipeline reprocessar os dados daquele dia específico.

## Setup Necessário

O banco de dados pode ser configurado utilizando docker compose. Para mais instruções: https://docs.docker.com/compose/install/

As credenciais estão disponíveis no arquivo docker-compose.yml

# Pipeline

## Instruções para execução da pipeline:

- o arquivo .ipynb deve estar salvo na pasta code-challenge-main e pode ser executado via Visual Studio Code, desde que a extensão Jupyter esteja instalada.
- deve ser executado o pip install das seguintes ibliotecas, caso não estejam instaladas ainda:
    - pip install psycopg2
    - pip install pandas
    - pip install pymongo
- o parâmetro de data deve ser passado na variável processing_date, seguindo as instruções de formato comentadas no código ('yyyy-MM-dd' ou str(date.today())).

## Banco de dados e formato de arquivo utilizado:

Foi utilizado como banco de dados o sistema MongoDB, tendo em vista a base de armazenamento desse sistema ser em documentos. Foi realizada a configuração de network access para permitir todos os endereços de IP e o acesso ao banco através do seguinte usuário e senha: northwind_user e thewindisblowing, respectivamente.

A escolha de utilização do MongoDB foi visando o crescimento exponencial dos dados (escalabilidade) e a capacidade em que esse banco de dados tem de garantir alta disponibilidade por meio de um processo de replicação. Esse banco também fornece
suporte oficial de driver para praticamente todas as linguagens populares, garantindo uma maior facilidade de trabalho.

A estrutura de documentos foi organizada a partir de collections, na qual cada tabela gerou uma collection nomeada {tabela}_{processing_date}_collection, para uma melhor organização dentro do banco de dados. Os documentos foram modelados através da formatação JSON e, em seguida, inseridos no MongoDB onde são convertidos em um formato binário para armazenamento. O formato JSON agrega flexibilidade aos documentos, trazendo um arquivo semi-estruturado que permite um schema evolution, conforme a necessidade da empresa.

## Step 1

Foram utilizadas as bibliotecas psycopg2 (conexão banco PostgreSQL), os (interação com o Sistema Operacional e disco local), datetime (geração da data atual) e pandas (escrita CSV).

Inicialmente foi criada uma variável chamada processing_date para definição de qual data seria executada. 

Posteriormente foi criada uma função save_postgres_to_csv, com os parâmetros de nome da tabela e data de processamento. A função define o caminho a ser salvo os arquivos a partir do diretório em que o arquivo .ipynb está salvo e tenta criar a pasta de cada etapa (fonte - postgres/csv, nome tabela, data processamento) caso não exista. 
```
current_path = os.getcwd()
source = 'postgres'
source_path = os.path.join(current_path, source)

#create source folder if not exists
try:
    os.mkdir(source_path)
    print(f'path created: ', source_path)
except:
    pass

#create specific table folder if not exists
table_path = os.path.join(source_path, table)

try:
    os.mkdir(table_path)
    print(f'path created: ', table_path)
except:
    pass

date_path = os.path.join(table_path, processing_date)

#create processing date folder if not exists
try:
    os.mkdir(date_path)
    print(f'path created: ', date_path)
except:
    pass

filename = f"{table}.csv"      
final_path = os.path.join(date_path, filename)
```
Com o caminho para salvar os arquivos criado, a função conecta no banco de dados e realiza a query que exporta para CSV.
```
conn = psycopg2.connect(host = "localhost", database="northwind", user="northwind_user", password=<password>)
```
Por fim, a função escreve o CSV gerado no caminho definido, com encoding UTF-8
```
cur = conn.cursor()
sql = f"COPY (select * from {table}) TO STDOUT WITH CSV HEADER"  

with open(final_path, "w", encoding = 'utf-8') as file:
    cur.copy_expert(sql, file)
    file.close 
```

A segunda função é específica para o arquivo CSV, chamada de save_csv_to_csv e recebe os mesmos parâmetros que a anterior. A lógica de criação do caminho para salvar os arquivos é a mesma e para escrita utiliza-se a função to_csv, com a opção de index = False.

Por fim, se a conexão com o banco Postgres ocorrer com sucesso, lê-se uma lista com o nome de cada uma das tabelas e executa-se as funções descritas acima.

## Step 2

Foram utilizadas as bibliotecas pymongo (conexão banco MongoSB), os (interação com o Sistema Operacional e disco local), datetime (geração da data atual) e pandas (escrita CSV).

Novamente chama-se a variável processing_date, pensando na execução dos steps separadamente.

Foi criada a função csv_to_mongo, com os parâmetros de nome da tabela, fonte e data de processamento. Dentro dessa função, é feita a validação de execução do step anterior, garantindo que haja o csv escrito localmente antes de seguir com a execução.

Posteriormente é feita a conexão com o banco:
```
client = pymongo.MongoClient("mongodb+srv://northwind_user:<password>@cluster0.ti9zwzb.mongodb.net/?retryWrites=true&w=majority")
```
Após a conexão, a função verifica se a tabela possui dados e segue para a criação do banco de dados e collection no mongo, caso não existam. Por fim, os dados são escritos na collection específica de cada tabela.
```
if data.empty:
    print(f'{table} is an empty table, not added to the mongo collection')
else:
    #create mongodb database if not exists
    if "northwind_db" not in client.list_database_names():
        client["northwind_db"]
        print("northwind_db dabatase created")

    #create or overwrite mongodb collection 
    db = client["northwind_db"]
    collection = db[f"{table}_{processing_date}_collection"]
    
    if f"{table}_{processing_date}_collection" not in client["northwind_db"].list_collection_names():
        collection
        print(f"{table}_{processing_date}_collection created")
    else:
        collection.drop()
        collection
        print(f"{table}_{processing_date}_collection overwritten")

    collection.insert_many(data.to_dict('records'))
```

Para chamar a execução da função, novamente utiliza-se uma lista com os nomes das tabelas.

## Step 3

Foram utilizadas as bibliotecas pymongo (conexão banco MongoSB) e pandas (escrita CSV).

Novamente é realizada a conexão com o MongoDB e, dessa vez, é feita a leitura das tabelas "orders" e "order_details", passando o schema de cada uma delas.

Por fim, é realizado um left join entre as tabelas utilizando o campo "order_id" como chave e salvo um csv do arquivo final.
```
final_df = orders.join(order_details.set_index('order_id'), on = 'order_id', how = 'left')

current_path = os.getcwd()
filename = f"full_order_table.csv"
file_path = os.path.join(current_path, filename)

final_df.to_csv(file_path, sep = ',', index = False, encoding = 'utf-8')
```