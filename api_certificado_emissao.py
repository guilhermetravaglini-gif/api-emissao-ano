from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
import requests
from bs4 import BeautifulSoup
import re
import base64
import tempfile
import os
from cryptography.hazmat.primitives.serialization import pkcs12
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

app = FastAPI(
    title="API Extrator NFS-e por Data de Emissão",
    description="API para extração de faturamento por data de emissão (não por competência)",
    version="1.0.0"
)

class FaturamentoRequestEmissao(BaseModel):
    certificado_base64: str = Field(..., description="Certificado A1 em base64")
    senha_certificado: str = Field(..., description="Senha do certificado")
    ano: str = Field(..., description="Ano de emissão (ex: 2025)", pattern=r"^\d{4}$")

class FaturamentoResponse(BaseModel):
    CNPJ: str
    Faturamento: float
    Notas_Encontradas: int
    Ano_Emissao: str

def fazer_login_certificado(certificado_base64, senha_certificado):
    """Realiza login com certificado A1 e retorna sessão autenticada"""
    
    try:
        cert_data = base64.b64decode(certificado_base64)
        private_key, certificate, ca_certs = pkcs12.load_key_and_certificates(
            cert_data,
            senha_certificado.encode(),
            backend=default_backend()
        )
    except Exception as e:
        raise Exception("Autenticação não realizada. Favor inserir os dados corretamente de acesso")
    
    temp_dir = tempfile.mkdtemp()
    cert_path = os.path.join(temp_dir, 'cert.pem')
    key_path = os.path.join(temp_dir, 'key.pem')
    
    with open(cert_path, 'wb') as f:
        f.write(certificate.public_bytes(serialization.Encoding.PEM))
    
    with open(key_path, 'wb') as f:
        f.write(private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        ))
    
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
    })
    
    session.cert = (cert_path, key_path)
    
    try:
        url = "https://www.nfse.gov.br/EmissorNacional/Certificado"
        response = session.get(url, timeout=30)
        
        if 'Emissor' not in session.cookies:
            raise Exception("Autenticação não realizada. Favor inserir os dados corretamente de acesso")
        
        cnpj = None
        soup = BeautifulSoup(response.text, 'html.parser')
        dropdown_perfil = soup.find('li', class_='dropdown perfil')
        if dropdown_perfil:
            texto = dropdown_perfil.get_text()
            cnpj_match = re.search(r'CNPJ:\s*(\d+)', texto)
            if cnpj_match:
                cnpj_limpo = cnpj_match.group(1)
                if len(cnpj_limpo) == 14:
                    cnpj = f"{cnpj_limpo[:2]}.{cnpj_limpo[2:5]}.{cnpj_limpo[5:8]}/{cnpj_limpo[8:12]}-{cnpj_limpo[12:]}"
        
        session.temp_cert_path = cert_path
        session.temp_key_path = key_path
        session.temp_dir = temp_dir
        
        return session, cnpj
        
    except requests.exceptions.SSLError:
        try:
            os.remove(cert_path)
            os.remove(key_path)
            os.rmdir(temp_dir)
        except:
            pass
        raise Exception("Autenticação não realizada. Favor inserir os dados corretamente de acesso")
    except Exception as e:
        try:
            os.remove(cert_path)
            os.remove(key_path)
            os.rmdir(temp_dir)
        except:
            pass
        if "Autenticação não realizada" in str(e):
            raise
        raise Exception("Autenticação não realizada. Favor inserir os dados corretamente de acesso")

def limpar_arquivos_temporarios(session):
    """Limpa arquivos temporários do certificado"""
    try:
        if hasattr(session, 'temp_cert_path'):
            os.remove(session.temp_cert_path)
        if hasattr(session, 'temp_key_path'):
            os.remove(session.temp_key_path)
        if hasattr(session, 'temp_dir'):
            os.rmdir(session.temp_dir)
    except:
        pass

def processar_pagina_por_emissao(soup, ano_filtro):
    """Processa uma página de notas filtrando por DATA DE EMISSÃO (não competência)"""
    faturamento_pagina = 0.0
    notas_na_pagina = 0
    continuar = True
    
    tbody = soup.find('tbody')
    if not tbody:
        return 0.0, 0, False
    
    linhas = tbody.find_all('tr')
    if not linhas:
        return 0.0, 0, False
    
    for linha in linhas:
        try:
            # Verifica se está emitida (status)
            img_gerada = linha.find('img', src='/EmissorNacional/img/tb-gerada.svg')
            if not img_gerada:
                continue
            
            # MUDANÇA PRINCIPAL: Agora usa a coluna de DATA DE EMISSÃO ao invés de COMPETÊNCIA
            td_data_emissao = linha.find('td', class_='td-data')
            if not td_data_emissao:
                continue
            
            data_emissao_texto = td_data_emissao.get_text(strip=True)  # Ex: "01/12/2025"
            
            # Extrai dia, mês e ano da data de emissão
            match = re.search(r'(\d{2})/(\d{2})/(\d{4})', data_emissao_texto)
            if not match:
                continue
            
            dia = match.group(1)
            mes = match.group(2)
            ano_emissao = match.group(3)
            
            # Se o ano de emissão é menor que o solicitado, para de buscar
            if int(ano_emissao) < int(ano_filtro):
                continuar = False
                break
            
            # Se o ano de emissão é maior que o solicitado, pula
            if int(ano_emissao) > int(ano_filtro):
                continue
            
            # Se chegou aqui, o ano de emissão é igual ao solicitado
            # Extrai o valor
            td_valor = linha.find('td', class_='td-valor')
            if not td_valor:
                continue
            
            valor_texto = td_valor.get_text(strip=True)
            valor_limpo = valor_texto.replace('.', '').replace(',', '.')
            valor = float(valor_limpo)
            
            faturamento_pagina += valor
            notas_na_pagina += 1
        except:
            continue
    
    return faturamento_pagina, notas_na_pagina, continuar

def buscar_notas_por_emissao(session, ano):
    """Busca e processa todas as notas fiscais por DATA DE EMISSÃO"""
    faturamento_total = 0.0
    notas_processadas = 0
    pagina = 1
    continuar = True
    url_base = "https://www.nfse.gov.br/EmissorNacional/Notas/Emitidas"
    
    while continuar:
        url = url_base if pagina == 1 else f"{url_base}?pg={pagina}"
        response = session.get(url, timeout=30)
        if response.status_code != 200:
            break
        
        soup = BeautifulSoup(response.text, 'html.parser')
        faturamento_pagina, notas_pagina, continuar = processar_pagina_por_emissao(soup, ano)
        
        faturamento_total += faturamento_pagina
        notas_processadas += notas_pagina
        
        if not continuar:
            break
        
        paginacao = soup.find('div', class_='paginacao')
        if not paginacao:
            break
        
        link_proxima = paginacao.find('a', title='Próxima')
        if not link_proxima or 'javascript:' in link_proxima.get('href', ''):
            break
        
        pagina += 1
    
    return faturamento_total, notas_processadas

@app.get("/")
def read_root():
    return {
        "status": "ok", 
        "message": "API Extrator NFS-e por Data de Emissão online",
        "docs": "/docs"
    }

@app.post("/api/faturamento-emissao", response_model=FaturamentoResponse)
def obter_faturamento_por_emissao(request: FaturamentoRequestEmissao):
    """
    Extrai o faturamento de NFS-e filtrando por DATA DE EMISSÃO (não por competência)
    
    - **certificado_base64**: Arquivo .pfx ou .p12 convertido em base64
    - **senha_certificado**: Senha do certificado digital
    - **ano**: Ano de EMISSÃO da nota (formato YYYY)
    
    IMPORTANTE: Este endpoint considera a DATA DE EMISSÃO da nota, não a competência!
    Exemplo: Nota emitida em 02/01/2025 com competência 12/2024 SERÁ CONSIDERADA no ano 2025.
    """
    session = None
    
    try:
        # Faz login com certificado
        session, cnpj = fazer_login_certificado(
            request.certificado_base64,
            request.senha_certificado
        )
        
        if not cnpj:
            cnpj = "Não identificado"
        
        # Busca as notas por data de emissão
        faturamento, quantidade = buscar_notas_por_emissao(session, request.ano)
        
        # Limpa arquivos temporários
        limpar_arquivos_temporarios(session)
        
        return FaturamentoResponse(
            CNPJ=cnpj,
            Faturamento=round(faturamento, 2),
            Notas_Encontradas=quantidade,
            Ano_Emissao=request.ano
        )
        
    except Exception as e:
        if session:
            limpar_arquivos_temporarios(session)
        
        if "Autenticação não realizada" in str(e):
            raise HTTPException(status_code=401, detail=str(e))
        raise HTTPException(status_code=500, detail=f"Erro: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)
