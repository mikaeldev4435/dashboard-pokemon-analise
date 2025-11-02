import pandas as pd
import streamlit as st
import plotly.express as px

df_atributos = pd.read_csv('atributos_pokemons.csv')
df_combates = pd.read_csv('combates_com_nomes.csv')

@st.cache_data
def dados_mais_vitorias():
    cont_vitorias = df_combates['winner_name'].value_counts()
    top5_vencedores = cont_vitorias.head(5)
    df_top5 = top5_vencedores.reset_index()
    df_top5.columns = ['Pokémon', 'Vitórias']

    return df_top5

@st.cache_data
def dados_mais_derrotas():
    # 1. Contar todas as participações (quantas vezes lutaram)
    participacoes_p1 = df_combates['first_pokemon_name'].value_counts()
    participacoes_p2 = df_combates['second_pokemon_name'].value_counts()

    # Somar P1 e P2 para obter o total de lutas por Pokémon
    # .add() alinha os nomes dos Pokémon automaticamente
    total_participacoes = participacoes_p1.add(participacoes_p2, fill_value=0).astype(int)

    # 2. Contar todas as vitórias
    total_vitorias = df_combates['winner_name'].value_counts()

    # 3. Combinar os dados em um DataFrame
    # 'total_participacoes' já tem o nome do Pokémon como índice
    df_stats = pd.DataFrame({'TotalParticipacoes': total_participacoes})

    # Juntar as vitórias (usando o índice, que é o nome do Pokémon)
    df_stats = df_stats.merge(total_vitorias.rename('TotalVitorias'),
                              left_index=True,
                              right_index=True,
                              how='left')  # 'left' para manter Pokémon que nunca venceram

    # Se um Pokémon nunca venceu, seu total de vitórias será NaN.
    # Precisamos converter NaN para 0.
    df_stats['TotalVitorias'] = df_stats['TotalVitorias'].fillna(0).astype(int)

    # 4. Calcular Derrotas
    df_stats['Derrotas'] = df_stats['TotalParticipacoes'] - df_stats['TotalVitorias']

    # 5. Obter o Top 5
    df_top5 = df_stats.sort_values(by='Derrotas', ascending=False).head(5)

    # 6. Formatar para o gráfico
    # O nome do Pokémon está no índice, então resetamos
    df_top5_formatado = df_top5[['Derrotas']].reset_index()
    df_top5_formatado.columns = ['Pokémon', 'Derrotas']

    return df_top5_formatado

@st.cache_data
def dados_correlacao():
    try:
        # Limpeza e preparação dos dados
        stats_cols = ['id', 'hp', 'attack', 'defense', 'sp_attack', 'sp_defense', 'speed', 'legendary']
        df_atributos_stats = df_atributos[stats_cols].copy()

        # Mapeamento robusto para 'legendary'
        legendary_map = {'true': 1, 'false': 0, True: 1, False: 0}
        df_atributos_stats['legendary'] = df_atributos_stats['legendary'].map(legendary_map).fillna(0).astype(int)

        df_combates_base = df_combates[['first_pokemon', 'second_pokemon', 'winner']]

        # Merges
        merge1 = pd.merge(df_combates_base, df_atributos_stats, left_on='first_pokemon', right_on='id')
        full_combat_stats = pd.merge(merge1, df_atributos_stats, left_on='second_pokemon', right_on='id',
                                     suffixes=('_p1', '_p2'))

        # Engenharia de Features
        full_combat_stats['p1_wins'] = (full_combat_stats['winner'] == full_combat_stats['first_pokemon']).astype(int)
        full_combat_stats['hp_diff'] = full_combat_stats['hp_p1'] - full_combat_stats['hp_p2']
        full_combat_stats['attack_diff'] = full_combat_stats['attack_p1'] - full_combat_stats['attack_p2']
        full_combat_stats['defense_diff'] = full_combat_stats['defense_p1'] - full_combat_stats['defense_p2']
        full_combat_stats['sp_attack_diff'] = full_combat_stats['sp_attack_p1'] - full_combat_stats['sp_attack_p2']
        full_combat_stats['sp_defense_diff'] = full_combat_stats['sp_defense_p1'] - full_combat_stats['sp_defense_p2']
        full_combat_stats['speed_diff'] = full_combat_stats['speed_p1'] - full_combat_stats['speed_p2']
        full_combat_stats['legendary_diff'] = full_combat_stats['legendary_p1'] - full_combat_stats['legendary_p2']

        # Análise de Correlação
        features = ['p1_wins', 'hp_diff', 'attack_diff', 'defense_diff', 'sp_attack_diff', 'sp_defense_diff',
                    'speed_diff', 'legendary_diff']
        corr_matrix = full_combat_stats[features].corr()

        return corr_matrix

    except FileNotFoundError:
        st.error(
            "Erro: Verifique se os arquivos 'combates_com_nomes.csv' e 'atributos_pokemons.csv' estão no local correto.")
        return None

@st.cache_data
def calcular_taxa_vitoria_tipo():
    # 1. Preparar dados de atributos
    df_atributos_slim = df_atributos[['id', 'type1', 'type2', 'type3']]

    # 2. Calcular Total de Vitórias por Tipo
    df_winner_types = pd.merge(df_combates[['winner']], df_atributos_slim, left_on='winner', right_on='id')
    wins_type1 = df_winner_types['type1'].value_counts()
    wins_type2 = df_winner_types['type2'].value_counts()
    wins_type3 = df_winner_types['type3'].value_counts()

    # Somar vitórias
    total_wins_per_type = wins_type1.add(wins_type2, fill_value=0).add(wins_type3, fill_value=0).astype(
        int)

    # 3. Calcular Total de Participações (Matches) por Tipo
    df_first = pd.merge(df_combates[['first_pokemon']], df_atributos_slim, left_on='first_pokemon', right_on='id')
    df_second = pd.merge(df_combates[['second_pokemon']], df_atributos_slim, left_on='second_pokemon', right_on='id')

    part_t1_first = df_first['type1'].value_counts()
    part_t2_first = df_first['type2'].value_counts()
    part_t3_first = df_first['type3'].value_counts()

    part_t1_second = df_second['type1'].value_counts()
    part_t2_second = df_second['type2'].value_counts()
    part_t3_second = df_second['type3'].value_counts()

    # Somar participações
    total_matches_per_type = (
        part_t1_first.add(part_t2_first, fill_value=0)
        .add(part_t3_first, fill_value=0)  # <-- CORREÇÃO
        .add(part_t1_second, fill_value=0)
        .add(part_t2_second, fill_value=0)
        .add(part_t3_second, fill_value=0)  # <-- CORREÇÃO
        .astype(int)
    )

    # 4. Calcular Taxa de Vitória e Limpar
    df_type_stats = pd.DataFrame({'TotalWins': total_wins_per_type, 'TotalMatches': total_matches_per_type})
    df_type_stats = df_type_stats.dropna()
    df_type_stats['WinRate'] = df_type_stats['TotalWins'] / df_type_stats['TotalMatches']

    # 5. Limpeza de Nomes
    df_type_stats.index = df_type_stats.index.str.strip()

    # 6. Agrupar os tipos limpos
    df_agrupado = df_type_stats.groupby(df_type_stats.index).agg(
        TotalWins=('TotalWins', 'sum'),
        TotalMatches=('TotalMatches', 'sum')
    ).reset_index()
    df_agrupado['WinRate'] = df_agrupado['TotalWins'] / df_agrupado['TotalMatches']
    df_agrupado = df_agrupado.sort_values(by='WinRate', ascending=False)

    return df_agrupado.rename(columns={'index': 'Tipo'})

@st.cache_data
def analisar_tipos_comuns():
    # Junta as três colunas de tipo em uma só série
    tipos_series = pd.concat([
        df_atributos['type1'],
        df_atributos['type2'],
        df_atributos['type3']
    ])
    # Conta os valores, remove nulos (NaN) e reseta para um DataFrame
    df_tipos_comuns = tipos_series.dropna().value_counts().reset_index()
    df_tipos_comuns.columns = ['Tipo', 'Contagem']
    return df_tipos_comuns

@st.cache_data
def analisar_proporcao_lendarios():
    # Mapa para limpar os dados da coluna 'legendary'
    legendary_map = {'true': 'Lendário', 'false': 'Não Lendário',
                     True: 'Lendário', False: 'Não Lendário'}

    # Mapeia, preenche NaNs (se houver) como 'Não Lendário' e conta
    contagem = df_atributos['legendary'].map(legendary_map).fillna('Não Lendário').value_counts()
    df_proporcao = contagem.reset_index()
    df_proporcao.columns = ['Categoria', 'Contagem']
    return df_proporcao

@st.cache_data
def analisar_vitorias_lendarios():
    # Mapeamento para valores consistentes
    legendary_map = {'true': True, 'false': False, True: True, False: False}

    # Selecionar colunas necessárias
    df_leg = df_atributos[['id', 'legendary']].copy()
    df_leg['legendary'] = df_leg['legendary'].map(legendary_map).fillna(False)

    # Mesclar o dataframe de combates com o de atributos (para o vencedor)
    df_vitorias = pd.merge(df_combates[['winner']], df_leg, left_on='winner', right_on='id')

    # Contar vitórias de lendários vs não lendários
    contagem_vitorias = df_vitorias['legendary'].value_counts()

    # Criar um dataframe com rótulos legíveis
    df_resultado = pd.DataFrame({
        'Categoria': ['Lendário' if x else 'Não Lendário' for x in contagem_vitorias.index],
        'Vitórias': contagem_vitorias.values
    })

    # Calcular proporção
    total_vitorias = df_resultado['Vitórias'].sum()
    df_resultado['Proporção (%)'] = (df_resultado['Vitórias'] / total_vitorias * 100).round(2)

    return df_resultado

st.set_page_config(layout='wide')
# --- SEÇÃO 1: TOP 5 VENCEDORES ---
st.header('Análise 1: Top 5 Pokémons com Mais Vitórias')
df_top_5 = dados_mais_vitorias()

if not df_top_5.empty:
    fig_top5 = px.bar(
        df_top_5,
        x='Pokémon',
        y='Vitórias',
        title='Top 5 Vencedores',
        text='Vitórias',
        color='Pokémon'
    )
    fig_top5.update_traces(textposition='outside')
    st.plotly_chart(fig_top5, use_container_width=True)

# --- SEÇÃO 1B: TOP 5 POKÉMONS COM MAIS DERROTAS ---
st.header('Análise 1B: Top 5 Pokémons com Mais Derrotas')
df_top_5_derrotas = dados_mais_derrotas()

if not df_top_5_derrotas.empty:
    fig_top5_derrotas = px.bar(
        df_top_5_derrotas,
        x='Pokémon',
        y='Derrotas',
        title='Top 5 Pokémons com Mais Derrotas',
        text='Derrotas',
        color='Pokémon'  # Usa cores categóricas, igual ao seu gráfico de vitórias
    )
    fig_top5_derrotas.update_traces(textposition='outside')
    st.plotly_chart(fig_top5_derrotas, use_container_width=True)

# --- SEÇÃO 2: ANÁLISE DE INFLUÊNCIA
st.header('Análise 2: Quais Atributos Mais Influenciam a Vitória?')
st.markdown("""
Esta análise mostra o quão importante é a *vantagem* em um atributo. 
Correlação entre a 'diferença de status' (ex: Velocidade do P1 - Velocidade do P2) 
e o resultado da partida (se P1 venceu).
""")

corr_matrix = dados_correlacao()

if corr_matrix is not None:
    # Preparar dados para o gráfico de barras
    corr_with_winner = corr_matrix['p1_wins'].sort_values(ascending=False)
    df_corr_plot = corr_with_winner.drop('p1_wins').reset_index()
    df_corr_plot.columns = ['Atributo', 'Correlação']

    nomes_atributos = {
        'hp_diff': 'Dif. HP', 'attack_diff': 'Dif. Ataque', 'defense_diff': 'Dif. Defesa',
        'sp_attack_diff': 'Dif. Ataque Especial', 'sp_defense_diff': 'Dif. Defesa Especial',
        'speed_diff': 'Dif. Velocidade', 'legendary_diff': 'Dif. Lendário'
    }
    df_corr_plot['Atributo'] = df_corr_plot['Atributo'].map(nomes_atributos)

    # Gráfico de Barras de Influência
    fig_bar = px.bar(
        df_corr_plot.sort_values('Correlação'),
        x='Correlação',
        y='Atributo',
        orientation='h',
        title='Influência de Cada Atributo na Vitória',
        text='Correlação'
    )
    fig_bar.update_traces(texttemplate='%{text:.3f}', textposition='auto')
    fig_bar.update_layout(xaxis_title="Correlação com a Vitória", yaxis_title="Diferença de Atributo")

    # Exibir gráficos
    st.plotly_chart(fig_bar, use_container_width=True)

else:
    st.warning("Não foi possível carregar os dados para a análise de correlação.")

# --- Seção 3: Exibição da Taxa de Vitória por Tipo ---
df_tipos_final = calcular_taxa_vitoria_tipo()
st.header("Análise 3: Taxa de Vitória por Tipo (Q1 e Q2)")
st.write("Tipos com desempenho consistentemente superior")

fig = px.bar(
    df_tipos_final,
    x='Tipo',
    y='WinRate',
    title="Taxa de Vitória por Tipo de Pokémon",
    labels={'WinRate': 'Taxa de Vitória', 'Tipo': 'Tipo'},
    color='WinRate',
    color_continuous_scale='RdYlGn',
    hover_data=['TotalWins', 'TotalMatches']
)
st.plotly_chart(fig, use_container_width=True)

# --- Seção 4: Tipos Mais Comuns ---
df_tipos = analisar_tipos_comuns()
st.header("Análise 4: Tipos de Pokémon Mais Comuns")

fig_tipos_comuns = px.bar(
    df_tipos.sort_values('Contagem', ascending=False),
    x='Tipo',
    y='Contagem',
    title='Contagem de Pokémon por Tipo',
    color='Tipo',
    text='Contagem'
)
fig_tipos_comuns.update_traces(textposition='outside')
st.plotly_chart(fig_tipos_comuns, use_container_width=True)

#--- Seção 5: Proporção de Lendários ---
st.header("Análise 5: Proporção de Pokémon Lendários")
df_proporcao = analisar_proporcao_lendarios()
fig_pie_lendarios = px.pie(
    df_proporcao,
    names='Categoria',
    values='Contagem',
    title='Proporção de Pokémon Lendários vs. Não Lendários',
    hole=0.3  # Cria um gráfico de "rosca"
)
fig_pie_lendarios.update_traces(textinfo='percent+label', pull=[0, 0.1])  # Destaca Lendários
st.plotly_chart(fig_pie_lendarios, use_container_width=True)

# --- Seção 6: Lendários vencem mais? ---
st.header("Análise 6: Pokémons Lendários Vencem Mais?")

df_vitorias_lendarios = analisar_vitorias_lendarios()

if not df_vitorias_lendarios.empty:
    fig_lend_vitorias = px.pie(
        df_vitorias_lendarios,
        names='Categoria',
        values='Vitórias',
        title='Proporção de Vitórias: Lendários vs Não Lendários',
        hole=0.3
    )
    fig_lend_vitorias.update_traces(textinfo='percent+label', pull=[0, 0.1])
    st.plotly_chart(fig_lend_vitorias, use_container_width=True)

else:
    st.warning("Não foi possível calcular as vitórias de lendários.")


