from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
import requests
from bs4 import BeautifulSoup
import re
import base64
import tempfile
import os
from typing import Optional
from cryptography.hazmat.primitives.serialization import pkcs12
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

app = FastAPI(
    title="API Extrator NFS-e com Paginação (Data de Emissão)",
    description="API para extração de faturamento página por página filtrando por DATA DE EMISSÃO",
    version="2.0.0"
)

class FaturamentoRequestPaginadoEmissao(BaseModel):
    certificado_base64: str = Field(..., description="Certificado A1 em base64")
    senha_certificado: str = Field(..., description="Senha do certificado")
    ano: str = Field(..., description="Ano de emissão (ex: 2025)", pattern=r"^\d{4}$")
    pagina: int = Field(..., description="Número da página a processar (começa em 1)", ge=1)

class FaturamentoResponsePaginadoEmissao(BaseModel):
    CNPJ: str
    Pagina: int
    Faturamento_Pagina: float
    Notas_Pagina: int
    Tem_Proxima_Pagina: bool
    Motivo_Parada: Optional[str] = None
    Ano_Emissao_Filtro: str

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

def processar_pagina_unica_emissao(soup, ano_filtro):
    """Processa UMA ÚNICA página de notas filtrando por DATA DE EMISSÃO"""
    faturamento_pagina = 0.0
    notas_na_pagina = 0
    tem_proxima = True
    motivo_parada = None
    
    tbody = soup.find('tbody')
    if not tbody:
        return 0.0, 0, False, "Nenhuma nota encontrada na página"
    
    linhas = tbody.find_all('tr')
    if not linhas:
        return 0.0, 0, False, "Nenhuma nota encontrada na página"
    
    for linha in linhas:
        try:
            # Verifica se está emitida
            img_gerada = linha.find('img', src='/EmissorNacional/img/tb-gerada.svg')
            if not img_gerada:
                continue
            
            # Extrai DATA DE EMISSÃO (não competência)
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
            
            # Se encontrou emissão de ano MENOR que o solicitado, PARA
            if int(ano_emissao) < int(ano_filtro):
                tem_proxima = False
                motivo_parada = f"Encontrou emissão de ano anterior ({ano_emissao})"
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
            
        except Exception:
            continue
    
    # Se não encontrou nenhuma nota na página, provavelmente acabou
    if notas_na_pagina == 0 and tem_proxima:
        tem_proxima = False
        motivo_parada = "Página vazia ou sem notas válidas"
    
    return faturamento_pagina, notas_na_pagina, tem_proxima, motivo_parada

def buscar_pagina_especifica_emissao(session, pagina, ano):
    """Busca UMA página específica de notas por data de emissão"""
    url_base = "https://www.nfse.gov.br/EmissorNacional/Notas/Emitidas"
    
    # Monta URL da página
    url = url_base if pagina == 1 else f"{url_base}?pg={pagina}"
    
    response = session.get(url, timeout=30)
    
    if response.status_code != 200:
        return 0.0, 0, False, f"Erro ao acessar página {pagina}"
    
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Processa a página
    faturamento, quantidade, tem_proxima, motivo = processar_pagina_unica_emissao(soup, ano)
    
    return faturamento, quantidade, tem_proxima, motivo

@app.get("/")
def read_root():
    return {
        "status": "ok", 
        "message": "API Extrator NFS-e com Paginação (Data de Emissão) online",
        "docs": "/docs",
        "versao": "2.0.0"
    }

@app.post("/api/faturamento-paginado-emissao", response_model=FaturamentoResponsePaginadoEmissao)
def obter_faturamento_paginado_emissao(request: FaturamentoRequestPaginadoEmissao):
    """
    Extrai o faturamento de UMA PÁGINA ESPECÍFICA de NFS-e filtrando por DATA DE EMISSÃO
    
    - **certificado_base64**: Arquivo .pfx ou .p12 convertido em base64
    - **senha_certificado**: Senha do certificado digital
    - **ano**: Ano de EMISSÃO da nota (formato YYYY)
    - **pagina**: Número da página a processar (começa em 1)
    
    IMPORTANTE: 
    - Este endpoint processa APENAS UMA PÁGINA por vez
    - Filtra por DATA DE EMISSÃO (não por competência)
    - Para automaticamente ao encontrar emissão de ano anterior
    - Use em loop no N8N para processar todas as páginas
    - REGRA DE EXCEÇÃO: Considera emissão, não competência
    
    Exemplo: Nota emitida em 02/01/2025 com competência 12/2024 SERÁ CONSIDERADA no ano 2025
    
    Exemplo de uso no N8N:
    let total = 0;
    let pagina = 1;
    let continuar = true;
    
    while (continuar) {
      response = await chamarAPI(pagina);
      total += response.Faturamento_Pagina;
      continuar = response.Tem_Proxima_Pagina;
      pagina++;
    }
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
        
        # Busca a página específica por data de emissão
        faturamento, quantidade, tem_proxima, motivo = buscar_pagina_especifica_emissao(
            session, 
            request.pagina, 
            request.ano
        )
        
        # Limpa arquivos temporários
        limpar_arquivos_temporarios(session)
        
        return FaturamentoResponsePaginadoEmissao(
            CNPJ=cnpj,
            Pagina=request.pagina,
            Faturamento_Pagina=round(faturamento, 2),
            Notas_Pagina=quantidade,
            Tem_Proxima_Pagina=tem_proxima,
            Motivo_Parada=motivo,
            Ano_Emissao_Filtro=request.ano
        )
        
    except Exception as e:
        if session:
            limpar_arquivos_temporarios(session)
        
        if "Autenticação não realizada" in str(e):
            raise HTTPException(status_code=401, detail=str(e))
        raise HTTPException(status_code=500, detail=f"Erro: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8003)
