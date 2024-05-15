import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta

class DataDownloader:
    def __init__(self, start_date='2010-01-01'):
        self.start_date = start_date
        self.end_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        self.ibovespa = None
        self.cambio = None

    def download_data(self):
        try:
            print(f"Baixando dados de {self.start_date} até {self.end_date}...")

            self.ibovespa = yf.download('^BVSP', start=self.start_date, end=self.end_date)['Adj Close']
            self.cambio = yf.download('USDBRL=X', start=self.start_date, end=self.end_date)['Adj Close']

            if self.ibovespa.empty or self.cambio.empty:
                raise ValueError("Os dados baixados estão vazios. Verifique sua conexão com a Internet e tente novamente.")
            
            print("Dados baixados com sucesso!")
        except Exception as e:
            print(f"Erro ao baixar os dados: {e}")
    
    def get_ibovespa(self):
        return self.ibovespa
    
    def get_cambio(self):
        return self.cambio
    
    def get_end_date(self):
        return self.end_date

class DataTransformer:
    def __init__(self, ibovespa, cambio):
        self.ibovespa = ibovespa
        self.cambio = cambio
        self.transformed_data = None
    
    def transform_data(self):
        try:
            print("Tratando os dados...")
            
            # Transformar de Series para DataFrame
            ibovespa_df = pd.DataFrame(self.ibovespa)
            ibovespa_df.columns = ['IBOV']
            cambio_df = pd.DataFrame(self.cambio)
            cambio_df.columns = ['Dolar']
            
            # Juntar os dois DataFrames com merge
            self.transformed_data = pd.merge(ibovespa_df, cambio_df, how='inner', left_index=True, right_index=True)
            
            # Criar nova coluna com o índice IBOVESPA em dólar
            self.transformed_data['IBOV_USD'] = self.transformed_data['IBOV'] / self.transformed_data['Dolar']
            
            print("Dados tratados com sucesso!")
        except Exception as e:
            print(f"Erro ao tratar os dados: {e}")

    def get_transformed_data(self):
        return self.transformed_data

class DataPipeline:
    def __init__(self):
        self.downloader = DataDownloader()
        self.transformer = None

    def run(self):
        self.downloader.download_data()
        
        ibovespa = self.downloader.get_ibovespa()
        cambio = self.downloader.get_cambio()
        end_date = self.downloader.get_end_date()
        
        if ibovespa is not None and cambio is not None:
            self.transformer = DataTransformer(ibovespa, cambio)
            self.transformer.transform_data()
            
            transformed_data = self.transformer.get_transformed_data()
            if transformed_data is not None:
                try:
                    file_name = f'IBOVESPA_USD_{end_date}.csv'
                    print(f"Salvando dados no disco como '{file_name}'...")
                    transformed_data.to_csv(file_name)
                    print(f"Dados salvos com sucesso em '{file_name}'.")
                except Exception as e:
                    print(f"Erro ao salvar os dados: {e}")
            else:
                print("Erro: Houve uma falha no tratamento dos dados. Contacte o suporte!")
        else:
            print("Erro: Dados não foram baixados corretamente. Contacte o suporte!")

if __name__ == "__main__":
    pipeline = DataPipeline()
    pipeline.run()
