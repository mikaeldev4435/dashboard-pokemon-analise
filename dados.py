import os
import time
import random
import json
import requests
import pandas as pd
from typing import Optional, Dict, Any, List, Iterable

# Config
BASE_URL = "http://ec2-52-67-119-247.sa-east-1.compute.amazonaws.com:8000"
CREDENCIAIS = {
    "username": os.getenv("API_USER", "kaizen-poke"),
    "password": os.getenv("API_PASS", "4w9f@D39fkkO"),
}

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "kaizen-poke-client/1.1 (+requests)"})

# Infra HTTP: retry/backoff
def request_with_retry(
    method: str,
    url: str,
    *,
    headers: Optional[Dict[str, str]] = None,
    params: Optional[Dict[str, Any]] = None,
    json_body: Optional[Dict[str, Any]] = None,
    timeout: int = 20,
    max_retries: int = 6,
    base_sleep: float = 0.6,
    backoff_factor: float = 1.6,
) -> requests.Response:
    for attempt in range(1, max_retries + 1):
        try:
            resp = SESSION.request(
                method=method.upper(),
                url=url,
                headers=headers,
                params=params,
                json=json_body,
                timeout=timeout,
            )

            if resp.status_code == 429:
                retry_after = resp.headers.get("Retry-After")
                if retry_after:
                    try:
                        sleep_s = float(retry_after)
                    except ValueError:
                        sleep_s = base_sleep * (backoff_factor ** (attempt - 1))
                else:
                    sleep_s = base_sleep * (backoff_factor ** (attempt - 1))
                sleep_s += random.uniform(0, 0.4)
                print(f"429 recebido. Aguardando {sleep_s:.1f}s (tentativa {attempt}/{max_retries})...")
                time.sleep(sleep_s)
                continue

            if 500 <= resp.status_code < 600:
                sleep_s = base_sleep * (backoff_factor ** (attempt - 1)) + random.uniform(0, 0.4)
                print(f"{resp.status_code} do servidor. Retry em {sleep_s:.1f}s (tentativa {attempt}/{max_retries})...")
                time.sleep(sleep_s)
                continue

            resp.raise_for_status()
            return resp

        except requests.exceptions.RequestException as e:
            if attempt == max_retries:
                print(f"Falha definitiva ao chamar {url}: {e}")
                raise
            sleep_s = base_sleep * (backoff_factor ** (attempt - 1)) + random.uniform(0, 0.4)
            print(f"Erro de rede '{e}'. Retry em {sleep_s:.1f}s (tentativa {attempt}/{max_retries})...")
            time.sleep(sleep_s)

    raise RuntimeError("Falha após todas as tentativas.")

# Endpoints
def fazer_login(url: str, creds: Dict[str, str]) -> Optional[str]:
    print("--- 1) Login ---")
    resp = request_with_retry("post", f"{url}/login", json_body=creds)
    dados = resp.json()
    token = dados.get("access_token") or dados.get("token")
    print("Login OK" if token else " Token não encontrado")
    return token

def buscar_health(url: str, token: Optional[str]) -> None:
    headers = {"Authorization": f"Bearer {token}"} if token else None
    resp = request_with_retry("get", f"{url}/health", headers=headers)
    print("Saúde:", resp.json())

def listar_pokemons(
    url: str,
    token: str,
    start_page: int = 1,
    per_page: int = 50,
    throttle_s: float = 0.12,
) -> pd.DataFrame:
    print("\n--- 2) Listando pokémons (para mapear id->name) ---")
    headers = {"Authorization": f"Bearer {token}"}
    page = start_page
    rows: List[Dict[str, Any]] = []
    while True:
        params = {"page": page, "per_page": per_page}
        resp = request_with_retry("get", f"{url}/pokemon", headers=headers, params=params)
        payload = resp.json()
        lista = payload.get("pokemons") or payload.get("results") or []
        if not lista:
            break
        rows.extend(lista)
        print(f"• Página {page} (+{len(lista)})")
        page += 1
        time.sleep(throttle_s)
    df = pd.DataFrame(rows)
    # Garante colunas mínimas
    keep = [c for c in ["id", "name"] if c in df.columns]
    if not {"id", "name"}.issubset(df.columns):
        print(" A listagem não trouxe 'id' e 'name'. Os nomes faltantes serão preenchidos via /pokemon/{id}.")
    return df[keep] if keep else df

def listar_combates(
    url: str,
    token: str,
    start_page: int = 1,
    per_page: int = 50,
    throttle_s: float = 0.18,
) -> pd.DataFrame:
    print("\n--- 3) Listando combates ---")
    headers = {"Authorization": f"Bearer {token}"}
    page = start_page
    rows: List[Dict[str, Any]] = []
    while True:
        params = {"page": page, "per_page": per_page}
        resp = request_with_retry("get", f"{url}/combats", headers=headers, params=params)
        payload = resp.json()
        combates = payload.get("combats") or payload.get("results") or []
        if not combates:
            break
        rows.extend(combates)
        print(f"• Página {page} (+{len(combates)})")
        page += 1
        time.sleep(throttle_s)
    return pd.DataFrame(rows)

def atributos_pokemon(url: str, token: str, pokemon_id: int) -> Dict[str, Any]:
    headers = {"Authorization": f"Bearer {token}"}
    resp = request_with_retry("get", f"{url}/pokemon/{pokemon_id}", headers=headers)
    return resp.json()

# Enriquecimento
def enriquecer_combates_com_nomes(combates: pd.DataFrame, pokemons_df: pd.DataFrame) -> pd.DataFrame:
    """
    Adiciona colunas de nome aos combates usando a listagem de pokemons.
    Se faltar nome para algum id, fica NaN (e pode ser preenchido depois via atributos).
    """
    pok = pokemons_df.copy()
    if "nome" in pok.columns and "name" not in pok.columns:
        pok = pok.rename(columns={"nome": "name"})
    if "id" not in pok.columns or "name" not in pok.columns:
        raise ValueError("pokemons_df precisa ter colunas 'id' e 'name' (ou 'nome').")

    # 2) Garante que o índice do mapa seja inteiro
    pok["id"] = pok["id"].astype(int)
    mapa_pokemons = pok.set_index("id")["name"].to_dict()

    # 3) Cópia dos combates e normalização de tipos
    df = combates.copy()
    # nomes das colunas que o endpoint retorna
    id_cols = {
        "first_pokemon": "first_pokemon_name",
        "second_pokemon": "second_pokemon_name",
        "winner": "winner_name",
    }

    # Verifica se as colunas existem
    for c in id_cols:
        if c not in df.columns:
            raise ValueError(f"Coluna esperada nos combates não encontrada: '{c}'")

    # Converte para int (trata NaN/strings)
    for c in id_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")  # permite NA

    # 4) Faz o map com cuidado — usar .map funciona melhor com ints normais
    # transformar o mapa para usar chaves inteiras
    mapa = {int(k): v for k, v in mapa_pokemons.items()}

    df[id_cols["first_pokemon"]] = df["first_pokemon"].map(mapa)
    df[id_cols["second_pokemon"]] = df["second_pokemon"].map(mapa)
    df[id_cols["winner"]] = df["winner"].map(mapa)

    # 5) Logs úteis para debug
    for newcol in id_cols.values():
        n_missing = df[newcol].isna().sum()
        total = len(df)
        print(f"Coluna '{newcol}': {total - n_missing}/{total} nomes preenchidos, {n_missing} faltando.")

    # mostra alguns IDs que não foram encontrados
    for idcol, namecol in id_cols.items():
        missing_ids = df.loc[df[namecol].isna(), idcol].dropna().unique()
        if len(missing_ids):
            print(f"IDs faltantes em '{idcol}' (exemplos): {missing_ids[:10]}")

    return df

def baixar_atributos_para_ids(
    url: str,
    token: str,
    ids: Iterable[int],
    throttle_s: float = 0.20,
) -> pd.DataFrame:
    """
    Baixa atributos detalhados para um conjunto de IDs (únicos), com throttle e retry.
    """
    registros: List[Dict[str, Any]] = []
    for i, pid in enumerate(sorted(set(int(x) for x in ids))):
        try:
            dados = atributos_pokemon(url, token, pid)
            registros.append(dados)
            if "name" in dados:
                print(f"✓ {pid} - {dados['name']}")
            else:
                print(f"✓ {pid}")
        except Exception as e:
            print(f"✗ Falha ao buscar atributos do {pid}: {e}")
        time.sleep(throttle_s)
    return pd.DataFrame(registros)

def preencher_nomes_faltantes_via_atributos(combates_enriquecidos: pd.DataFrame, atributos_df: pd.DataFrame) -> pd.DataFrame:
    """
    Caso a listagem não tenha trazido 'name', preenche nomes a partir do dataframe de atributos.
    """
    if not {"id", "name"}.issubset(atributos_df.columns):
        return combates_enriquecidos
    mapa = atributos_df.drop_duplicates("id").set_index("id")["name"]
    out = combates_enriquecidos.copy()
    for col_id, col_name in [
        ("first_pokemon", "first_pokemon_name"),
        ("second_pokemon", "second_pokemon_name"),
        ("winner", "winner_name"),
    ]:
        if col_name not in out.columns:
            out[col_name] = out[col_id].map(mapa)
        else:
            out[col_name] = out[col_name].fillna(out[col_id].map(mapa))
    return out

def add_col(atributos_df,combates_df):

    atributos_nulos = atributos_df.isnull().sum()
    combates_nulos = combates_df.isnull().sum()
    print(atributos_nulos)
    print(combates_nulos)

    colunas = ['hp', 'attack', 'defense', 'sp_attack', 'sp_defense', 'speed']
    atributos_df['forca_total'] = atributos_df[colunas].sum(axis=1)

    padrao_regex = r'[,/]'
    split_types = atributos_df['types'].str.split(padrao_regex, expand=True)
    novos_nomes = {i: f'type{i + 1}' for i in split_types.columns}
    split_types = split_types.rename(columns=novos_nomes)
    atributos_df = pd.concat([atributos_df, split_types], axis=1)

    return atributos_df

if __name__ == "__main__":
    token = fazer_login(BASE_URL, CREDENCIAIS)
    if not token:
        raise SystemExit("Não foi possível autenticar.")
    buscar_health(BASE_URL, token)

    # 1) Traz lista de pokémons (id, name) — Evita N requisições
    df_pokemons = listar_pokemons(BASE_URL, token, per_page=50, throttle_s=0.12)

    # 2) Traz todos os combates
    df_combates = listar_combates(BASE_URL, token, per_page=50, throttle_s=0.18)

    # 3) Troca IDs por nomes
    df_combates_nomes = enriquecer_combates_com_nomes(df_combates, df_pokemons)

    # 4) Gera conjunto de IDs únicos que aparecem nos combates
    ids_unicos = pd.unique(pd.concat([
        df_combates["first_pokemon"].astype(int),
        df_combates["second_pokemon"].astype(int),
        df_combates["winner"].astype(int),
    ], ignore_index=True))

    # 5) Baixa atributos detalhados de TODOS esses pokémons
    df_atributos = baixar_atributos_para_ids(BASE_URL, token, ids_unicos, throttle_s=0.20)

    # 6) Caso algum nome tenha faltado, preenche via atributos
    df_combates_nomes = preencher_nomes_faltantes_via_atributos(df_combates_nomes, df_atributos)

    #7) Adicionar colunas
    df_atributos = add_col(df_atributos,df_combates_nomes)

    # 8) Salva CSVs
    df_combates_nomes.to_csv("combates_com_nomes.csv", index=False)
    df_atributos.to_csv("atributos_pokemons.csv", index=False)
    print(" Arquivos salvos: combates_com_nomes.csv e atributos_pokemons.csv")