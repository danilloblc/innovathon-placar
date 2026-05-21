import urllib.request
import urllib.parse

URL = "https://placar.contagilpb.com.br/quiz/importar"
SENHA = "innovathon2026"

# categoria | enunciado | resposta
perguntas = [
    ("Tecnologia", "Qual recurso do Power Query possui função similar ao PROCV do Excel, sendo usado para cruzar duas tabelas/fontes de dados?", "Mesclar consultas (Merge)"),
    ("Tecnologia", "Qual recurso do Power Query permite dividir um texto usando delimitadores?", "Dividir coluna"),
    ("Tecnologia", "Qual linguagem é utilizada no Power Query?", "Linguagem M"),
    ("Tecnologia", "Qual etapa remove linhas duplicadas no Power Query?", "Remover duplicatas"),
    ("Tecnologia", "Qual IA é desenvolvida pelo Google?", "Gemini"),
    ("Tecnologia", "Qual IA é desenvolvida pela OpenAI?", "ChatGPT"),
    ("Tecnologia", "Qual IA da Microsoft está integrada ao Excel e Office?", "Copilot"),
    ("Tecnologia", "Qual ferramenta é mais usada para dashboards gerenciais?", "Power BI"),
    ("Tecnologia", "Qual recurso do Power BI permite criar medidas personalizadas?", "DAX"),
    ("Tecnologia", "O que são KPI?", "Indicadores-chave de desempenho"),

    ("Tributário", "Em qual formato de arquivo os dados da NF-e vêm estruturados?", "XML"),
    ("Tributário", "Quais os dois novos tributos criados pela reforma tributária?", "IBS e CBS"),
    ("Tributário", "Qual sistema do governo centraliza escriturações fiscais e contábeis digitais?", "SPED"),
    ("Tributário", "O IBS será gerido por qual entidade?", "Comitê Gestor"),
    ("Tributário", "Quando o crédito do ICMS é maior que o débito, qual o resultado da apuração?", "Saldo credor"),
    ("Tributário", "Qual órgão da administração pública é responsável pelo ICMS?", "Estadual"),
    ("Tributário", "No cálculo do Simples Nacional, o que significa a sigla RBT12?", "Receita Bruta dos Últimos 12 Meses"),

    ("Contabilidade", "No método das partidas dobradas, o total dos débitos deve ser igual a quê?", "Ao total dos créditos"),
    ("Contabilidade", "Qual grupo do balanço contém bancos e caixa?", "Ativo Circulante"),
    ("Contabilidade", "Em qual demonstração aparecem os bens e direitos da empresa?", "Balanço Patrimonial"),
    ("Contabilidade", "Qual grupo do balanço representa obrigações da empresa?", "Passivo"),
    ("Contabilidade", "Qual adicional pode existir no cálculo do IRPJ?", "Adicional de 10%"),
    ("Contabilidade", "Como o aumento de uma conta do ativo normalmente é registrado no lançamento contábil?", "Débito"),
    ("Contabilidade", "Em um lançamento contábil, o aumento de uma receita normalmente é registrado em qual lado?", "Crédito"),

    ("Previdenciária", "Qual obrigação acessória reúne informações trabalhistas e previdenciárias?", "eSocial"),
    ("Previdenciária", "Qual documento registra a jornada de trabalho do colaborador?", "Controle de ponto"),
    ("Previdenciária", "Qual é o prazo legal para pagamento do salário mensal?", "Até o 5º dia útil"),
    ("Previdenciária", "O que trata a NR-17?", "Ergonomia"),

    ("Legalização", "Qual órgão é responsável pelo registro de constituição de empresas no estado?", "Junta Comercial"),
    ("Legalização", "Como se chama o documento que autoriza o funcionamento de uma empresa no município?", "Alvará de Funcionamento"),
]

texto = "\n".join(f"{c} | {e} | {r}" for c, e, r in perguntas)
data = urllib.parse.urlencode({"password": SENHA, "texto": texto}).encode("utf-8")
req = urllib.request.Request(URL, data=data, method="POST")
with urllib.request.urlopen(req) as resp:
    print("Resposta:", resp.read().decode("utf-8"))
