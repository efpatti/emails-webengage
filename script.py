import os
import re
import json
import time
from pathlib import Path
from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo
from typing import Dict, List, Tuple, Optional

import pandas as pd
from IPython.display import HTML, display
from google.cloud import bigquery
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging

# === CONFIGURAÇÕES ===
AUTH_URL = "https://sso.mottu.cloud/realms/Internal/protocol/openid-connect/token"
EVENTS_URL = "https://event-integration.mottu.cloud/v2/events"
CLIENT_ID = "mottu-admin"
PROJETO_BIGQUERY = "dm-mottu-aluguel"
USERNAME = "enzo.ferracini@mottu.com.br"
PASSWORD = "4VRE43AK@"
BONI_REFERENCIA_DIAS=2  # Buscar dados de 2 dias atrás
BONI_DATA_REFERENCIA=2025-08-20  # Data específica
BONI_FALTA_USA_BONUS_FINAL=1  # Usar BonusFinal para faltas

# Configuração de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Datas (D-1 por padrão no fuso America/Sao_Paulo)
REFERENCIA_DIAS = int(BONI_REFERENCIA_DIAS)
_tz = ZoneInfo('America/Sao_Paulo')
_now_br = datetime.now(_tz)
DATA_REFERENCIA = (_now_br - timedelta(days=REFERENCIA_DIAS)).date()

# Override manual opcional
_override = str(BONI_DATA_REFERENCIA) if BONI_DATA_REFERENCIA else None
if _override:
    try:
        DATA_REFERENCIA = date.fromisoformat(_override)
        logger.info(f"➡️ Override de DATA_REFERENCIA aplicado: {DATA_REFERENCIA}")
    except ValueError:
        logger.warning(f"⚠️ Valor inválido em BONI_DATA_REFERENCIA: {_override}")

if not USERNAME or not PASSWORD:
    logger.warning("⚠️ Defina as variáveis de ambiente MOTTU_USERNAME e MOTTU_PASSWORD para autenticar.")

logger.info(f"Data de referência (fuso BR): {DATA_REFERENCIA}")

class MottuAuthenticator:
    """Gerenciador de autenticação com cache de token"""
    
    def __init__(self, username: str = None, password: str = None):
        self.username = username or USERNAME
        self.password = password or PASSWORD
        self._token = None
        self._token_expiry = None
    
    def get_access_token(self) -> str:
        """Obtém token de acesso, usando cache se válido"""
        if self._token and self._token_expiry and datetime.now() < self._token_expiry:
            return self._token
        
        if not self.username or not self.password:
            raise ValueError("Credenciais ausentes. Configure MOTTU_USERNAME e MOTTU_PASSWORD.")

        payload = {
            "grant_type": "password",
            "client_id": CLIENT_ID,
            "username": self.username,
            "password": self.password
        }
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        
        try:
            response = requests.post(AUTH_URL, data=payload, headers=headers, timeout=30)
            if response.status_code == 200:
                token_data = response.json()
                self._token = token_data.get("access_token")
                # Cache por 50min (tokens geralmente duram 1h)
                self._token_expiry = datetime.now() + timedelta(minutes=50)
                logger.info(f"🔓 Autenticado com sucesso como {self.username}")
                return self._token
            else:
                raise RuntimeError(f"Erro ao autenticar: {response.status_code} - {response.text}")
        except requests.RequestException as e:
            raise RuntimeError(f"Erro de conexão ao autenticar: {str(e)}")

class BigQueryDataManager:
    """Gerenciador de queries do BigQuery"""
    
    def __init__(self, project: str = PROJETO_BIGQUERY):
        self.project = project
        self.client = bigquery.Client(project=project, location="US")
    
    def buscar_prestadores_elegíveis(self) -> pd.DataFrame:
        """Busca prestadores elegíveis baseado nos cargos e status"""
        query = """
        SELECT 
            id_funcionario,
            id_funcionario_v2,
            nome_funcionario,
            email,
            cargo,
            filial,
            filial_code
        FROM `exp_colaboradores.funcionarios_filiais` f 
        WHERE f.cargo IN ("Auxiliar de rua", "Agente de Suporte de Rua") 
          AND foi_desligado = false
        """
        return self.client.query(query).to_dataframe()
    
    def buscar_dados_bonificacao(self, data_referencia: date) -> pd.DataFrame:
        """Busca dados de bonificação para a data especificada"""
        query = f"""
        WITH prestadores_elegíveis AS (
            SELECT 
                id_funcionario,
                id_funcionario_v2,
                nome_funcionario,
                email,
                cargo,
                filial,
                filial_code
            FROM `exp_colaboradores.funcionarios_filiais` f 
            WHERE f.cargo IN ("Auxiliar de rua", "Agente de Suporte de Rua") 
              AND foi_desligado = false
        ),
        bonificacao_data AS (
            SELECT
                p.*,
                -- Dados de penalidades vindos da jornada
                COALESCE(dp.ChegouAtrasado, 0) as ChegouAtrasado,
                COALESCE(dp.LogoutAntecipado, 0) as LogoutAntecipado
            FROM `flt_servicos_rua.pre_bonificacao_motoristas` p
            LEFT JOIN (
                -- Subquery para pegar penalidades de jornada
                SELECT
                    usuario_id,
                    data_jornada,
                    MAX(CASE WHEN TIME_DIFF(horario_login, inicio_jornada, MINUTE) > 15 THEN 1 ELSE 0 END) AS ChegouAtrasado,
                    MAX(CASE WHEN TIME_DIFF(fim_jornada, horario_logout, MINUTE) > 15 THEN 1 ELSE 0 END) AS LogoutAntecipado
                FROM `flt_servicos_rua.jornada_suporte`
                WHERE data_jornada = DATE('{data_referencia}')
                GROUP BY usuario_id, data_jornada
            ) dp ON CAST(p.usuarioId AS STRING) = CAST(dp.usuario_id AS STRING) 
                   AND DATE(p.data_abertura) = dp.data_jornada
            WHERE DATE(p.data_abertura) = DATE('{data_referencia}')
        )
        SELECT 
            b.*,
            pe.id_funcionario,
            pe.email,
            pe.nome_funcionario,
            pe.cargo,
            -- Formatação de dados para o email
            FORMAT_DATE('%d/%m/%Y', DATE(b.data_abertura)) as data_abertura_fmt,
            CASE WHEN b.Passou90km = 1 THEN 'Sim' ELSE 'Não' END AS Passou90kmFmt,
            CASE WHEN b.ChegouAtrasado = 1 THEN 'Sim' ELSE 'Não' END AS ChegouAtrasadoFmt,
            CASE WHEN b.LogoutAntecipado = 1 THEN 'Sim' ELSE 'Não' END AS LogoutAntecipadoFmt,
            -- Primeiro nome para personalização
            SPLIT(pe.nome_funcionario, ' ')[OFFSET(0)] as primeiro_nome
        FROM bonificacao_data b
        INNER JOIN prestadores_elegíveis pe ON CAST(b.usuarioId AS STRING) = CAST(pe.id_funcionario_v2 AS STRING)
        WHERE b.QtdServicos > 0  -- Só quem teve serviços
        ORDER BY b.data_abertura, pe.nome_funcionario
        """
        
        try:
            df = self.client.query(query).to_dataframe()
            logger.info(f"📊 Dados de bonificação carregados: {len(df)} registros")
            return df
        except Exception as e:
            logger.error(f"Erro ao buscar dados de bonificação: {str(e)}")
            return pd.DataFrame()
    
    def buscar_metricas_filial(self, data_referencia: date) -> pd.DataFrame:
        """Busca métricas de filiais para validação de elegibilidade"""
        query = f"""
        SELECT 
            filial,
            providers_count,
            pct_under_90,
            approx_inadimplencia_rate
        FROM `flt_servicos_rua.pre_bonificacao_motoristas`
        WHERE DATE(data_abertura) = DATE('{data_referencia}')
          AND providers_count >= 4
        GROUP BY filial, providers_count, pct_under_90, approx_inadimplencia_rate
        """
        return self.client.query(query).to_dataframe()

def _inicio_quinzena(d: date) -> date:
    """Calcula início da quinzena"""
    return d.replace(day=1) if d.day <= 15 else d.replace(day=16)

def _id_quinzena(d: date) -> str:
    """Gera ID da quinzena"""
    q = 1 if d.day <= 15 else 2
    return f"{d.year}-{d.month:02d}-Q{q}"

class PenaltyCalculator:
    """Calculador de penalidades quinzenais"""
    
    def __init__(self, bq_manager: BigQueryDataManager):
        self.bq_manager = bq_manager
    
    def carregar_base_quinzena(self, data_ref: date) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Carrega dados de bônus e jornada para cálculo de penalidades"""
        inicio = _inicio_quinzena(data_ref)
        
        bonus_query = f"""
        SELECT 
            DATE(p.data_abertura) AS data_abertura,
            p.usuarioId,
            p.filial,
            p.modal,
            p.motorista,
            p.QtdServicos,
            p.PercentTroca,
            p.BonusTotal,
            p.Passou90km,
            p.Passou120km
        FROM `flt_servicos_rua.pre_bonificacao_motoristas` p
        LEFT JOIN `flt_servicos_rua.dimensionamento_filiais_rua` df ON p.filial = df.filial
        WHERE DATE(p.data_abertura) BETWEEN DATE('{inicio}') AND DATE('{data_ref}')
          AND df.prestadores_previsto >= 4
          AND COALESCE(df.gerente_regional, '') <> 'Oscar'
        """
        
        jornada_query = f"""
        SELECT 
            usuario_id, 
            data_jornada, 
            status_login, 
            status_logout
        FROM `flt_servicos_rua.jornada_suporte`
        WHERE data_jornada BETWEEN DATE('{inicio}') AND DATE('{data_ref}')
          AND dia_tipo = 'Trabalho'
          AND (status_login IN ('Falta','Atrasado') OR status_logout = 'Antecipado')
        """
        
        try:
            bonus_df = self.bq_manager.client.query(bonus_query).to_dataframe()
            jornada_df = self.bq_manager.client.query(jornada_query).to_dataframe()
            
            # Processamento de datas
            if not bonus_df.empty:
                bonus_df['data_abertura'] = pd.to_datetime(bonus_df['data_abertura'], errors='coerce')
                bonus_df = bonus_df.dropna(subset=['data_abertura'])
                bonus_df['quinzena'] = bonus_df['data_abertura'].apply(lambda x: _id_quinzena(x.date()))
            
            if not jornada_df.empty:
                jornada_df['data_jornada'] = pd.to_datetime(jornada_df['data_jornada'], errors='coerce')
                jornada_df = jornada_df.dropna(subset=['data_jornada'])
                jornada_df['quinzena'] = jornada_df['data_jornada'].apply(lambda x: _id_quinzena(x.date()))
            
            return bonus_df, jornada_df
        
        except Exception as e:
            logger.error(f"Erro ao carregar dados da quinzena: {str(e)}")
            return pd.DataFrame(), pd.DataFrame()
    
    def calcular_penalidades_quinzena(self, data_ref: date) -> pd.DataFrame:
        """Calcula penalidades da quinzena"""
        bonus_df, jornada_df = self.carregar_base_quinzena(data_ref)
        
        if bonus_df.empty or jornada_df.empty:
            return pd.DataFrame(columns=['usuarioId','quinzena','Penalidade','ValorPerdido','Dias','Dia'])

        # Trata penalidade de velocidade (>90 km/h perde bônus do dia)
        pass90_series = pd.to_numeric(bonus_df['Passou90km'], errors='coerce').fillna(0).astype(int)
        bonus_df['BonusFinal'] = bonus_df['BonusTotal'].where(pass90_series != 1, 0)

        faltas_df = jornada_df[jornada_df['status_login']=='Falta'].copy()
        atrasos_df = jornada_df[
            (jornada_df['status_login']=='Atrasado') | 
            (jornada_df['status_logout']=='Antecipado')
        ].copy()

        penalidades = []
        usuarios_com_bonus = bonus_df[bonus_df['BonusTotal']>0]['usuarioId'].unique()
        
        # Filtra apenas usuários com bônus
        faltas_df = faltas_df[faltas_df['usuario_id'].isin(usuarios_com_bonus)]
        atrasos_df = atrasos_df[
            atrasos_df['usuario_id'].isin(usuarios_com_bonus) & 
            (~atrasos_df['usuario_id'].isin(faltas_df['usuario_id']))
        ]

        # Faltas: perde toda a quinzena
        usar_bonus_final_para_falta = bool(BONI_FALTA_USA_BONUS_FINAL)
        base_col = 'BonusFinal' if usar_bonus_final_para_falta else 'BonusTotal'

        for (uid, quinzena), group in faltas_df.groupby(['usuario_id','quinzena']):
            valor_perdido = bonus_df[
                (bonus_df['usuarioId']==uid) & 
                (bonus_df['quinzena']==quinzena)
            ][base_col].sum()

            dias = group['data_jornada'].dt.strftime('%Y-%m-%d').unique().tolist()
            penalidades.append({
                'usuarioId': uid,
                'quinzena': quinzena,
                'Penalidade': 'Falta',
                'ValorPerdido': valor_perdido,
                'Dias': dias,
                'Dia': None
            })

        # Atrasos: perde maiores bônus da quinzena
        for (uid, quinzena), group in atrasos_df.groupby(['usuario_id','quinzena']):
            bonus_quinzena = bonus_df[
                (bonus_df['usuarioId']==uid) & 
                (bonus_df['quinzena']==quinzena)
            ]

            if bonus_quinzena.empty:
                continue

            bonus_sorted = bonus_quinzena.sort_values(base_col, ascending=False)
            dias_atraso = group['data_jornada'].dt.strftime('%Y-%m-%d').unique()

            for i, (_, rowb) in enumerate(bonus_sorted.iterrows()):
                if i >= len(dias_atraso):
                    break
                penalidades.append({
                    'usuarioId': uid,
                    'quinzena': quinzena,
                    'Penalidade': 'Atraso',
                    'ValorPerdido': rowb[base_col],
                    'Dias': None,
                    'Dia': dias_atraso[i]
                })

        return pd.DataFrame(penalidades)
    
    def resumo_penalidades(self, data_ref: date) -> Dict:
        """Gera resumo de penalidades por usuário"""
        pen_df = self.calcular_penalidades_quinzena(data_ref)
        
        if pen_df.empty:
            return {'df': pen_df, 'por_usuario': {}}
        
        agregado = pen_df.groupby(['usuarioId','Penalidade'])['ValorPerdido'].sum().unstack(fill_value=0)
        resultado = {}
        
        for usuarioId, row in agregado.iterrows():
            falta_perdida = 'Falta' in row.index and row['Falta'] > 0
            valor_falta = float(row['Falta']) if 'Falta' in row.index else 0.0
            valor_atraso = float(row['Atraso']) if 'Atraso' in row.index else 0.0
            
            resultado[usuarioId] = {
                'PerdeuQuinzenaFalta': falta_perdida,
                'ValorPerdidoFalta': valor_falta,
                'ValorPerdidoAtrasos': valor_atraso,
            }
        
        return {'df': pen_df, 'por_usuario': resultado}

class WebEngageEventManager:
    """Gerenciador de eventos para WebEngage"""
    
    def __init__(self, authenticator: MottuAuthenticator):
        self.authenticator = authenticator
    
    def format_eventos_bonificacao(self, resultado_df: pd.DataFrame, penalidades_map: Dict) -> List[Dict]:
        """Formata eventos de bonificação para envio ao WebEngage"""
        eventos = []
        
        for _, row in resultado_df.iterrows():
            # Busca penalidades do usuário
            pen = penalidades_map.get(int(row['usuarioId']), {})
            
            # Dados do evento conforme estrutura do email HTML
            event_data = {
                'data_abertura': row['data_abertura_fmt'],
                'filial': row['filial'],
                'modal': row['modal'],
                'motorista': row['motorista'],
                'first_name': row['primeiro_nome'],
                'QtdServicos': int(float(row['QtdServicos'])),
                'BonusTotal': f"{float(row['BonusTotal']):.2f}",
                'Passou90km': row['Passou90kmFmt'],
                'ChegouAtrasado': row['ChegouAtrasadoFmt'],
                'LogoutAntecipado': row['LogoutAntecipadoFmt'],
                # Informações de penalidades
                'PerdeuQuinzenaFalta': pen.get('PerdeuQuinzenaFalta', False),
                'ValorPerdidoFalta': f"{pen.get('ValorPerdidoFalta', 0.0):.2f}",
                'ValorPerdidoAtrasos': f"{pen.get('ValorPerdidoAtrasos', 0.0):.2f}",
                # Deeplink para o sistema
                'deeplink': f"https://bonificacao-ui.mottu.io/?type={row['modal'].lower()}&secao=1&data={row['data_abertura']}&usuario={row['usuarioId']}"
            }
            
            evento = {
                'userIds': [row['id_funcionario']],
                'eventName': 'email_bonificacao_diaria_motorista',
                'applicationSource': 'user-management',
                'eventTime': datetime.utcnow().isoformat() + 'Z',
                'eventData': {
                    'email_bonificacao_diaria_motorista': {
                        'custom': event_data
                    }
                },
                'providers': ['web-engage-serviceprovider']
            }
            
            eventos.append(evento)
        
        return eventos
    
    def send_event(self, event: Dict, max_retries: int = 3) -> bool:
        """Envia evento individual para a API"""
        token = self.authenticator.get_access_token()
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        
        for attempt in range(1, max_retries + 1):
            try:
                response = requests.post(
                    EVENTS_URL, 
                    headers=headers, 
                    data=json.dumps(event), 
                    timeout=30
                )
                
                if response.status_code in (200, 201):
                    motorista = event['eventData']['email_bonificacao_diaria_motorista']['custom']['motorista']
                    logger.info(f"✅ Evento enviado ({attempt}ª tentativa) -> {motorista}")
                    return True
                else:
                    logger.warning(f"⚠️ Tentativa {attempt} falhou ({response.status_code}): {response.text[:200]}")
                    
            except requests.RequestException as e:
                logger.warning(f"⚠️ Erro de rede na tentativa {attempt}: {str(e)}")
            
            if attempt < max_retries:
                time.sleep(2 * attempt)  # Backoff exponencial
        
        motorista = event['eventData']['email_bonificacao_diaria_motorista']['custom']['motorista']
        logger.error(f"❌ Falha final ao enviar evento -> {motorista}")
        return False
    
    def send_events_batch(self, eventos: List[Dict], max_workers: int = 5) -> Dict[str, int]:
        """Envia eventos em lote com threading"""
        resultados = {"sucessos": 0, "falhas": 0}
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_event = {
                executor.submit(self.send_event, evento): evento 
                for evento in eventos
            }
            
            for future in as_completed(future_to_event):
                if future.result():
                    resultados["sucessos"] += 1
                else:
                    resultados["falhas"] += 1
                
                # Pequena pausa para evitar rate limiting
                time.sleep(0.5)
        
        return resultados

def preview_eventos(eventos: List[Dict], limite: int = 3):
    """Mostra preview dos eventos formatados"""
    logger.info(f"🔍 Pré-visualização de {min(limite, len(eventos))} eventos:")
    
    for i, ev in enumerate(eventos[:limite]):
        print(f"\n=== EVENTO {i+1} ===")
        event_data = ev['eventData']['email_bonificacao_diaria_motorista']['custom']
        print(f"Usuário: {event_data['motorista']} ({event_data['first_name']})")
        print(f"Filial: {event_data['filial']}")
        print(f"Data: {event_data['data_abertura']}")
        print(f"Serviços: {event_data['QtdServicos']}")
        print(f"Bônus: R$ {event_data['BonusTotal']}")
        print(f"Passou 90km/h: {event_data['Passou90km']}")
        print(f"Chegou Atrasado: {event_data['ChegouAtrasado']}")
        print(f"Logout Antecipado: {event_data['LogoutAntecipado']}")
        
        if event_data['PerdeuQuinzenaFalta']:
            print(f"⚠️ Perdeu quinzena por falta: R$ {event_data['ValorPerdidoFalta']}")
        if float(event_data['ValorPerdidoAtrasos']) > 0:
            print(f"⚠️ Valor perdido por atrasos: R$ {event_data['ValorPerdidoAtrasos']}")

class BonificationSystem:
    """Sistema principal de bonificação"""
    
    def __init__(self):
        self.authenticator = MottuAuthenticator()
        self.bq_manager = BigQueryDataManager()
        self.penalty_calculator = PenaltyCalculator(self.bq_manager)
        self.event_manager = WebEngageEventManager(self.authenticator)
    
    def executar_envio(self, data_referencia: date = None, preview: int = 3, enviar: bool = False, max_workers: int = 5):
        """Execução principal do sistema"""
        data_ref = data_referencia or DATA_REFERENCIA
        
        try:
            logger.info("🚀 Iniciando sistema de bonificação diária...")
            
            # 1. Autenticação
            logger.info("🔐 Validando autenticação...")
            token = self.authenticator.get_access_token()
            
            # 2. Buscar dados
            logger.info("📊 Carregando dados de bonificação...")
            df_bonificacao = self.bq_manager.buscar_dados_bonificacao(data_ref)
            
            if df_bonificacao.empty:
                logger.warning("⚠️ Nenhum dado de bonificação encontrado para a data.")
                return
            
            logger.info(f"✅ Dados carregados: {len(df_bonificacao)} prestadores com bonificação")
            
            # 3. Calcular penalidades
            logger.info("⚖️ Calculando penalidades da quinzena...")
            penalidades_summary = self.penalty_calculator.resumo_penalidades(data_ref)
            penalidades_map = penalidades_summary['por_usuario']
            
            if penalidades_summary['df'].empty:
                logger.info("✅ Nenhuma penalidade encontrada na quinzena")
            else:
                usuarios_penalizados = len(penalidades_summary['df']['usuarioId'].unique())
                logger.info(f"⚠️ Penalidades encontradas para {usuarios_penalizados} usuários")
            
            # 4. Preparar eventos
            logger.info("🛠️ Preparando eventos para WebEngage...")
            eventos = self.event_manager.format_eventos_bonificacao(df_bonificacao, penalidades_map)
            logger.info(f"📧 {len(eventos)} eventos preparados")
            
            # 5. Preview
            if preview > 0:
                preview_eventos(eventos, limite=preview)
            
            # 6. Envio
            if not enviar:
                logger.info("🔍 Modo preview ativado - defina enviar=True para disparar eventos")
                return
            
            logger.info("📤 Iniciando envio de eventos...")
            resultados = self.event_manager.send_events_batch(eventos, max_workers=max_workers)
            
            logger.info(f"✅ Envio concluído:")
            logger.info(f"   • Sucessos: {resultados['sucessos']}")
            logger.info(f"   • Falhas: {resultados['falhas']}")
            logger.info(f"   • Taxa de sucesso: {(resultados['sucessos']/len(eventos)*100):.1f}%")
            
        except Exception as e:
            logger.error(f"💥 Erro na execução: {str(e)}")
            raise

# === PONTO DE ENTRADA ===
if __name__ == '__main__':
    sistema = BonificationSystem()
    
    # Execução em modo preview por padrão
    sistema.executar_envio(
        data_referencia=DATA_REFERENCIA,
        preview=2, 
        enviar=False,  # Mude para True para enviar eventos
        max_workers=3  # Ajuste conforme necessário
    )