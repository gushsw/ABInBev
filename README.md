# abinbev
Case datalake para a abinbev

 DataLake abinbev
Esta documentação tem como objetivo explicar a arquitetura de dados implementada para o projeto abinbev, focada na construção de um Data Lake utilizando serviços da AWS. A escolha dessa arquitetura visa garantir alta eficiência no processamento de dados em larga escala, suportar atualizações diárias em batches e assegurar alta disponibilidade de dados para análise. 

# Arquitetura
A arquitetura do Data Lake foi projetada com o objetivo de ser simples, eficiente e econômica. Considerando que estamos operando em um ambiente de nuvem, onde custos podem se acumular rapidamente, a otimização dos recursos foi uma prioridade. Sendo assim, esta arquitetura se baseia nos seguintes serviços da AWS: 

# Desenho da arquitetura: 
![image](https://github.com/user-attachments/assets/f33d0055-e600-4ad9-9adc-4ec539710201)

  1) S3 Buckets: Comumente conhecido como S3 uma sigla para "Simple Storage Service", é um serviço de armazenamento de dados da AWS que se assemelha a um file server, mas com a peculiaridade de ser um serviço serverless. Sendo assim, os buckets S3 são utilizados nesta arquitetura para constituir as três camadas do DataLake da AbInbev, seguindo a estrutura de medalhão: abinbev_bucket_bronze, abinbev_bucket_silver, abinbev_bucket_gold. Além disso, haverá mais um bucket, que auxiliará como repositório do time e servirá para a configuração do MWAA (Managed Workflows for Apache Airflow)¹.
  ¹O serviço MWAA será explicado mais à frente.

  2) Glue: O Glue é um serviço da AWS onde podemos criar jobs ETL, visualizar dados em uma camada, monitorar todo o processo ETL e catalogar dados. Nesta arquitetura, o Glue será utilizado para executar pipelines ETL, que serão orquestrados pelo Apache Airflow, sendo todo o poder de processamento necessário para suportar o ETL de batches diários de dados fornecido por máquinas virtuais do tipo G1X, G2X, G4X, G8X, que variam de 4 CPUs e 16 GB de RAM até 32 CPUs e 128 GB de RAM. Isso é significativamente mais econômico e simples do que se tivéssemos escolhido utilizar máquinas virtuais no serviço EC2 (Elastic Cloud Computing) da AWS.

# Configurando o ambiente
Para construir o DataLake dentro da AWS, você deverá instalar os seguintes pré-requisitos:

1) Instalar o Python em uma versão igual ou superior à 3.11: https://www.python.org/downloads/
2) Instalar AWS CLI, seguindo o passo a passo de acordo com o seu sistema operacional: https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html
   
ATENÇÃO!
 - Caso o seu ambiente possua validação de usuários federados via AD, você deverá configurar os usuários seguindo as instruções neste link: https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-sso.html.
Após a instalação do AWS CLI, não esqueça de configurar os seus parâmetros de usuário através do comando aws configure, fornecendo suas chaves de acesso. Esse procedimento evita a necessidade de passar suas chaves de acesso como constantes dentro dos códigos Python de implementação do ambiente. Instruções neste link: https://docs.aws.amazon.com/cli/latest/userguide/getting-started-quickstart.html.


# Após a instalação do Python, você deverá instalar as seguintes bibliotecas Python:

 ATENÇÃO!
 - Considere criar um ambiente virtual para evitar conflito de dependências!

•  boto3: pip install boto3 ou python -m pip install boto3.
Função: Biblioteca Python que atua como o SDK da AWS. Ela permite interagir com os serviços AWS usando Python.
Documentação: https://pypi.org/project/boto3/

•  logging: pip install logging ou python -m pip install logging.
Função: Biblioteca Python utilizada para registrar eventos, sendo especialmente útil para alertar falhas de login e outros eventos no ambiente.
Documentação: https://pypi.org/project/logging2/

•  json: pip install json ou python -m pip install json.
Função: Biblioteca Python utilizada para passar os parâmetros de acesso dos usuários e definição dos grupos dentro da AWS utilizando JSON.
Documentação: https://pypi.org/project/JSON4JSON/

•  pendulum==2.1.2: pip install pendulum==2.1.2 ou python -m pip install pendulum==2.1.2.
Função: Biblioteca Python utilizada para construir constantes temporais como dias, semanas, fusos horários, etc. É um grande facilitador para criar triggers de execução dentro da AWS pelo Airflow.
Documentação: https://pypi.org/project/pendulum/

•  apache-airflow: pip install apache-airflow ou python -m pip install apache-airflow.
Função: Plataforma para autorar, programar e monitorar fluxos de trabalho, como pipelines de dados.
Documentação: https://pypi.org/project/apache-airflow/

•  apache-airflow-providers-amazon: pip install apache-airflow-providers-amazon ou python -m pip install apache-airflow-providers-amazon.
Função: Pacote que permite ao Airflow interagir com serviços da AWS, como S3, Glue, EMR, etc.
Documentação: https://pypi.org/project/apache-airflow-providers-amazon/

•  pandas: pip install pandas ou python -m pip install pandas.
Função: O Pandas é uma biblioteca essencial para manipulação e análise de dados em estruturas de dados como DataFrames.
Documentação: https://pypi.org/project/pandas/

•  pyarrow: pip install pyarrow ou python -m pip install pyarrow.
Função: O PyArrow fornece suporte para operações eficientes de leitura e escrita em formatos de dados como Apache Parquet e Arrow.
Documentação: https://pypi.org/project/pyarrow/

Estas bibliotecas devem ser instaladas para garantir a funcionalidade de todos os scripts python que implementam a arquitetura do DataLake da AbInbev, assim como possibilita o gerenciamento das dags do airflow assim como a interoperatividade entre o apache airflow e o ambiente em nuvem do DataLake

# Conclusão
A implementação da arquitetura de Data Lake para o projeto AbInbev utilizando serviços da AWS é uma solução robusta e eficiente, capaz de atender às demandas de processamento e armazenamento de dados em larga escala. A escolha cuidadosa dos serviços, como Amazon S3, AWS Glue, MWAA e Power BI, é fundamental para garantir uma estrutura flexível, escalável e econômica, capaz de lidar com a variabilidade das cargas de trabalho e a complexidade das operações de dados da startup.
Ao adotarmos o modelo de camadas Bronze, Silver e Gold no Amazon S3, a arquitetura assegura que os dados sejam gerenciados de forma organizada e eficiente, permitindo uma transição suave desde a ingestão dos dados brutos até sua transformação em insights valiosos para o negócio. A integração com o MWAA proporciona uma orquestração simplificada e segura dos pipelines de dados, enquanto o AWS Glue oferece uma solução escalável para a execução de ETL, equilibrando custos e desempenho.
Por fim, a escolha do Power BI como ferramenta de visualização complementa essa arquitetura, possibilitando que as equipes de dados e negócios acessem e analisem os dados refinados de maneira intuitiva e colaborativa. Com essa combinação de tecnologias, o Data Lake da AbInbev está bem posicionado para suportar as necessidades atuais e futuras da empresa, proporcionando uma base sólida para a tomada de decisões estratégicas e a inovação contínua no gerenciamento de dados.
