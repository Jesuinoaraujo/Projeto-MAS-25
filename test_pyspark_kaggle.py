from pyspark.sql import SparkSession
import os
import zipfile
from kaggle.api.kaggle_api_extended import KaggleApi

# Configurações do Spark (não será usado agora, mas está configurado para análises futuras)
spark = SparkSession.builder \
    .appName("Pipeline MinIO com Kaggle") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minio") \
    .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

# Configurações para a API do Kaggle
api = KaggleApi()
api.authenticate()

# Caminho do diretório e arquivo ZIP
kaggle_dataset = "utkarshx27/non-alcohol-fatty-liver-disease"
destination_path = "/home/jesuino/Projeto-MAS-25/data"
zip_file_path = os.path.join(destination_path, "non-alcohol-fatty-liver-disease.zip")

# Baixar o conjunto de dados do Kaggle
print("Baixando o conjunto de dados do Kaggle...")
api.dataset_download_files(kaggle_dataset, path=destination_path, unzip=False)

# Verificar se o arquivo ZIP foi baixado
if os.path.exists(zip_file_path):
    print(f"Arquivo baixado com sucesso: {zip_file_path}")
else:
    print("Erro ao baixar o arquivo. Certifique-se de que o conjunto de dados existe no Kaggle.")
    exit()

# Descompactar o arquivo ZIP
print("Descompactando os dados...")
with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
    zip_ref.extractall(destination_path)

# Identificar os arquivos CSV
csv_files = [f for f in os.listdir(destination_path) if f.endswith('.csv')]
print("Arquivos CSV encontrados após descompactação:")
for csv_file in csv_files:
    print(f" - {csv_file}")

print("\nOs arquivos foram descompactados e estão prontos para serem enviados ao bucket `raw` no MinIO.")
print("Para enviar os arquivos ao MinIO, siga estas etapas:")
print("1. Acesse a interface do MinIO em http://localhost:9001")
print("2. Faça login com as credenciais: usuário 'minio' e senha 'minio123'")
print("3. No bucket `raw`, clique em 'Upload Files' e envie os arquivos:")
for csv_file in csv_files:
    print(f"   - {os.path.join(destination_path, csv_file)}")

print("\nDepois que os arquivos estiverem no MinIO, você pode iniciar as análises com o arquivo `nafld1.csv`.")

