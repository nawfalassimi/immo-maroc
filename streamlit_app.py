import streamlit as st
import pandas as pd
import plotly.express as px
from src.supabase_utils import read_from_supabase, get_all_villes, get_quartiers_for_ville

st.set_page_config(page_title="Immobilier Maroc", page_icon="🏠", layout="wide")
st.title("🏘️ Tableau de bord immobilier au Maroc")

# --- Sidebar Filtres ---
st.sidebar.header("🔎 Filtres")

# Récupère toutes les villes dynamiquement depuis Supabase
villes = get_all_villes()
selected_ville = st.sidebar.selectbox("Ville", villes)

# Récupère les quartiers de la ville sélectionnée
quartiers = get_quartiers_for_ville(selected_ville)
selected_quartier = st.sidebar.selectbox("Quartier", [""] + quartiers)

# Prix / Prix au m2
prix_min, prix_max = st.sidebar.slider("Prix (MAD)", 0, 10_000_000, (0, 10_000_000), step=50_000)
prix_m2_min, prix_m2_max = st.sidebar.slider("Prix / m²", 0, 100_000, (0, 100_000), step=1_000)

# --- Lecture filtrée ---
df = read_from_supabase(
    ville=selected_ville,
    quartier=selected_quartier if selected_quartier else None,
    prix_min=prix_min,
    prix_max=prix_max,
    prix_m2_min=prix_m2_min,
    prix_m2_max=prix_m2_max,
)

# --- Nettoyage types ---
numeric_cols = ["prix", "superficie", "pieces", "chambres", "sdb", "prix_m2"]
for col in numeric_cols:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

st.write(f"📊 {len(df)} annonces trouvées")

if not df.empty:
    # --- Statistiques ---
    st.subheader("📈 Statistiques")
    col1, col2, col3 = st.columns(3)
    col1.metric("Prix moyen", f"{df['prix'].mean():,.0f} MAD")
    col2.metric("Prix médian", f"{df['prix'].median():,.0f} MAD")
    col3.metric("Prix au m² moyen", f"{df['prix_m2'].mean():,.0f} MAD")

    # --- Graphiques de distribution ---
    col1, col2 = st.columns(2)

    with col1:
        st.plotly_chart(
            px.histogram(df, x="prix", nbins=30, title="Distribution des prix"),
            use_container_width=True
        )

    with col2:
        st.plotly_chart(
            px.histogram(df, x="prix_m2", nbins=30, title="Distribution du prix/m²"),
            use_container_width=True
        )

    # --- Statistiques supplémentaires ---
    st.subheader("📈 Statistiques par quartier")

    stat_quartiers = df.groupby("quartier")[["prix", "prix_m2"]].agg(["mean", "median"]).round(0)
    st.dataframe(stat_quartiers)

    fig_box = px.box(df[df["prix_m2"] < 50000], x="quartier", y="prix_m2",
                     title="Boxplot du prix/m² par quartier")
    fig_box.update_layout(xaxis_title="", yaxis_title="Prix/m²", xaxis_tickangle=45)
    st.plotly_chart(fig_box, use_container_width=True)

    fig_pieces = px.histogram(df, x="pieces", title="Nombre d'annonces par nombre de pièces")
    fig_pieces.update_layout(xaxis_title="Nombre de pièces", yaxis_title="Nombre d’annonces")
    st.plotly_chart(fig_pieces, use_container_width=True)

    # --- Données ---
    st.subheader("📋 Détail des annonces")
    st.dataframe(df[[
        "titre", "ville", "quartier", "prix", "superficie", "prix_m2", "pieces", "chambres", "sdb", "lien"
    ]])
else:
    st.warning("Aucune donnée trouvée pour ces filtres.")
