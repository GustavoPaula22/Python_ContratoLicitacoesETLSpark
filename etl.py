from controle import dataextract as de
from controle import dataload as dl
import re
from pyspark.sql import SparkSession


def start():
    print('Buscando dados Planilha...')
    df = de.planilha()

    print('Tratando dados Planilha/DataFrame...')
    if df.count() == 0:
        print('Data Frame Vazio. Sem dados na Planilha!')
    else:
        df = df.filter(df['Apelido'].isNotNull())

        new_columns = [re.sub(r'\s+', ' ', col.strip().replace('\n', ' '))
                       for col in df.columns]
        df = df.toDF(*new_columns)

        selecionarColunas = [
            "APELIDO (PALAVRA CHAVE)",
            "DEMANDANTE",
            "DATA SOLICITAÇÃO",
            "MODALIDADE",
            "CRITÉRIO DE JULGAMENTO",
            "ESCOPO",
            "OBJETO",
            "PROGRAMA",
            "DATA ESTIMADA CONTRATAÇÃO",
            "PROCESSO SISLOG Nº",
            "PROCESSO ADMINISTRATIVO SEI Nº",
            "DATA DE ABERTURA",
            "VALOR ESTIMADO",
            "VALOR CONTRATADO",
            "EMPRESA VENCEDORA",
            "STATUS BI",
            "% Desconto",
            "PRIORIDADE",
            "UNIDADE STATUS BI",
            "DATA ESTIMADA AÇÕES GELIC",
            "AÇÕES GELIC"
        ]

        df = df.select(*selecionarColunas)

        columnsName = [
            "Apelido",
            "Demandante",
            "DataSolicitacao",
            "Modalidade",
            "CriterioJulgamento",
            "Escopo",
            "Objeto",
            "Programa",
            "DataEstimadaContratacao",
            "ProcessoSislog",
            "ProcessoAdministrativoSei",
            "DataAbertura",
            "ValorEstimado",
            "ValorContratado",
            "EmpresaVencedora",
            "StatusBI",
            "PorcentagemDesconto",
            "Prioridade",
            "UnidadeStatusBI",
            "DataEstimadaAcoesGelic",
            "AcoesGelic"
        ]

        df = df.toDF(*columnsName)

        date_columns = [
            "DataSolicitacao",
            "DataEstimadaContratacao",
            "DataAbertura",
            "DataEstimadaAcoesGelic"
        ]

        for col in date_columns:
            df = df.withColumn(col, df[col].cast('date'))

        df = df.fillna('')

        def clean_currency(col):
            return (
                df[col]
                .replace('R$', '', regex=True)
                .replace('.', '', regex=True)
                .replace(',', '', regex=True)
                .replace(' ', '', regex=True)
                .replace('\n', '', regex=True)
                .cast('float')
                .alias(col)
            )

        df = df.withColumn('ValorEstimado', clean_currency('ValorEstimado'))
        df = df.withColumn('ValorContratado',
                           clean_currency('ValorContratado'))

        df = df.withColumn(
            'PorcentagemDesconto',
            df['PorcentagemDesconto']
            .replace('%', '', regex=True)
            .replace(' ', '', regex=True)
            .replace('\n', '', regex=True)
            .cast('int')
            .alias('PorcentagemDesconto')
        )

        print('Inserindo dados SQL Server...')
        dl.sqlServer(df)


if __name__ == '__main__':
    print('Iniciando processo...')
    start()
