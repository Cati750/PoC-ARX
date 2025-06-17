Conteúdo e guia das pastas

1º Passo:
 - Presidio: Contém o script em Python responsável por correr a ferramenta Presidio para a classificação automática de PII. Executar este ficheiro primeiro para iniciar o serviço local Presidio.

2º Passo:
 - DATA: Contém o ficheiro com a base de dados MySQL.É necessário ter um ambiente de desenvolvimento local (ex: Laragon) para conectar ao servidor MySQL. Iniciar o servidor antes de executar os scripts Java.

3ª Passo:
 - LIBRARIES: Contém as bibliotecas necessárias para correr a API Java do ARX. Importar estas bibliotecas no ambiente de desenvolvimento Java - se ainda não estiverem presentes (ex: IntelliJ IDEA). Certificar que as dependências estão configuradas no classpath do projeto.

4º Passo:
 - POC_ARX: Contém os scripts Java que utilizam a biblioteca ARX e o serviço Presidio para classificar e anonimizar dados. Abrir o projeto no IDE. Executar a classe Main para iniciar o processo de classificação e anonimização.

