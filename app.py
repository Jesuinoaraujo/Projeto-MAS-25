import streamlit as st
from joblib import load
import os

# Configuração do caminho para carregar o modelo
MODEL_PATH = '/home/jesuino/Projeto-MAS-25/notebooks/SVC_model.joblib'

try:
    # Tentando carregar o modelo a partir do caminho especificado
    svcmodel = load(MODEL_PATH)
except FileNotFoundError:
    st.error(f"Arquivo do modelo não encontrado no caminho: {MODEL_PATH}")
    st.stop()

# Título da aplicação
st.title("Previsão da Doença Hepática Gordurosa Não Alcoólica (DHGNA)")

# Entrada de dados na barra lateral
st.sidebar.header("Dados do Paciente")
idade = st.sidebar.number_input("Idade", min_value=1, max_value=120, value=30)
peso = st.sidebar.number_input("Peso (kg)", min_value=1.0, max_value=300.0, value=70.0)
altura = st.sidebar.number_input("Altura (cm)", min_value=50.0, max_value=250.0, value=170.0)
genero = st.sidebar.selectbox("Gênero", options=["Feminino", "Masculino"])

# Cálculo do IMC
altura_m = altura / 100  # Converte altura para metros
imc = peso / (altura_m ** 2)

# Exibe o IMC calculado
st.sidebar.write(f"IMC Calculado: {imc:.2f}")

# Botão para realizar a previsão
if st.sidebar.button("Realizar Previsão"):
    # Ajustando os dados para o formato do modelo
    genero_bin = 0 if genero == "Feminino" else 1
    dados_paciente = [[idade, peso, altura, imc, genero_bin]]
    
    try:
        # Fazendo a previsão
        previsao = svcmodel.predict(dados_paciente)
        resultado = "Alto risco de DHGNA" if previsao[0] == 1 else "Baixo risco de DHGNA"
        
        # Exibe o resultado da previsão
        st.success(f"Resultado da Previsão: {resultado}")
    except Exception as e:
        st.error(f"Erro ao realizar a previsão: {str(e)}")