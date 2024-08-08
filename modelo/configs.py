# ====== informações acesso banco de dados ====== #
server = r'10.6.0.16'
database = r'SIGELI'
user = r'suporte'
senha = r'asi'

queryInsert = """
        INSERT INTO [dbo].[eSGLContratoLicitacoes] 
        ([Apelido]
        ,[Demandante]
        ,[DataSolicitacao]
        ,[Modalidade]
        ,[CriterioJulgamento]
        ,[Escopo]
        ,[Objeto]
        ,[Programa]
        ,[DataEstimadaContratacao]
        ,[ProcessoSislog]
        ,[ProcessoAdministrativoSei]
        ,[DataAbertura]
        ,[ValorEstimado]
        ,[ValorContratado]
        ,[EmpresaVencedora]
        ,[StatusBI]
        ,[PorcentagemDesconto]
        ,[Prioridade]
        ,[UnidadeStatusBI]
        ,[DataEstimadaAcoesGelic]
        ,[AcoesGelic])
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

queryBusca = """
    SELECT 
       [Apelido]
      ,[Demandante]
      ,[DataSolicitacao]
      ,[Modalidade]
      ,[CriterioJulgamento]
      ,[Escopo]
      ,[Objeto]
      ,[Programa]
      ,[DataEstimadaContratacao]
      ,[ProcessoSislog]
      ,[ProcessoAdministrativoSei]
      ,[DataAbertura]
      ,[ValorEstimado]
      ,[ValorContratado]
      ,[EmpresaVencedora]
      ,[StatusBI]
      ,[PorcentagemDesconto]
      ,[Prioridade]
      ,[UnidadeStatusBI]
      ,[DataEstimadaAcoesGelic]
      ,[AcoesGelic]
    FROM [dbo].[eSGLContratoLicitacoes]
    """
