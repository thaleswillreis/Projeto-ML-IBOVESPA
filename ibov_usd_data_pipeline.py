import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta

class DataDownloader:
    """
    Classe responsável por baixar os dados financeiros do Yahoo Finance.

    Atributos:
    ----------
    start_date : str
        Data inicial para o download dos dados.
    end_date : str
        Data final para o download dos dados (o dia anterior à data atual).
    ibovespa : pandas.Series
        Dados do índice Ibovespa.
    cambio : pandas.Series
        Dados do câmbio USD/BRL.

    Métodos:
    -------
    download_data():
        Baixa os dados do Yahoo Finance.
    get_ibovespa():
        Retorna os dados do Ibovespa.
    get_cambio():
        Retorna os dados do câmbio USD/BRL.
    get_end_date():
        Retorna a data final dos dados.
    """
    def __init__(self, start_date='2010-01-01'):
        """
        Inicializa a classe com a data inicial e final.

        Parâmetros:
        ----------
        start_date : str
            Data inicial para o download dos dados (padrão é '2010-01-01').
        """
        self.start_date = start_date
        self.end_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        self.ibovespa = None
        self.cambio = None

    def download_data(self):
        """
        Baixa os dados do Ibovespa e do câmbio USD/BRL do Yahoo Finance.

        Levanta:
        -------
        ValueError:
            Se os dados baixados estiverem vazios.
        Exception:
            Para qualquer outro erro ocorrido durante o download dos dados.
        """
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
        """
        Retorna os dados do Ibovespa.

        Retorna:
        -------
        pandas.Series:
            Dados do Ibovespa.
        """
        return self.ibovespa
    
    def get_cambio(self):
        """
        Retorna os dados do câmbio USD/BRL.

        Retorna:
        -------
        pandas.Series:
            Dados do câmbio USD/BRL.
        """
        return self.cambio
    
    def get_end_date(self):
        """
        Retorna a data final dos dados.

        Retorna:
        -------
        str:
            Data final dos dados no formato 'AAAA-MM-DD'.
        """
        return self.end_date

class DataTransformer:
    """
    Classe responsável por transformar os dados financeiros baixados.

    Atributos:
    ----------
    ibovespa : pandas.Series
        Dados do índice Ibovespa.
    cambio : pandas.Series
        Dados do câmbio USD/BRL.
    transformed_data : pandas.DataFrame
        Dados transformados com o índice IBOVESPA em dólar.

    Métodos:
    -------
    transform_data():
        Transforma os dados baixados.
    get_transformed_data():
        Retorna os dados transformados.
    """

    def __init__(self, ibovespa, cambio):
        """
        Inicializa a classe com os dados do Ibovespa e do câmbio USD/BRL.

        Parâmetros:
        ----------
        ibovespa : pandas.Series
            Dados do índice Ibovespa.
        cambio : pandas.Series
            Dados do câmbio USD/BRL.
        """
        self.ibovespa = ibovespa
        self.cambio = cambio
        self.transformed_data = None
    
    def transform_data(self):
        """
        Transforma os dados baixados, criando um novo DataFrame com o índice IBOVESPA em dólar.

        Levanta:
        -------
        Exception:
            Para qualquer erro ocorrido durante a transformação dos dados.
        """
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
        """
        Retorna os dados transformados.

        Retorna:
        -------
        pandas.DataFrame:
            Dados transformados com o índice IBOVESPA em dólar.
        """
        return self.transformed_data

class DataPipeline:
    """
    Classe responsável por coordenar o fluxo de trabalho de download e transformação dos dados.

    Atributos:
    ----------
    downloader : DataDownloader
        Instância da classe DataDownloader.
    transformer : DataTransformer
        Instância da classe DataTransformer.

    Métodos:
    -------
    run():
        Coordena o download e transformação dos dados, e salva os dados transformados em um arquivo CSV.
    """
    def __init__(self):
        """
        Inicializa a classe com uma instância de DataDownloader.
        """
        self.downloader = DataDownloader()
        self.transformer = None

    def run(self):
        """
        Coordena o download e transformação dos dados, verificando a integridade em cada etapa e tratando exceções.
        Salva os dados transformados em um arquivo CSV com a data final dos dados no nome do arquivo.

        Levanta:
        -------
        Exception:
            Para qualquer erro ocorrido durante o salvamento dos dados.
        """
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
