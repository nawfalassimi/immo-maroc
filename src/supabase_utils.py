import streamlit as st
import pandas as pd
from supabase import create_client, Client


# Config
SUPABASE_URL = st.secrets["supabase"]["url"]
SUPABASE_KEY = st.secrets["supabase"]["key"]
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


def save_to_supabase(df):
    df = df.drop_duplicates(subset=["lien"])
    rows = df.to_dict(orient="records")
    try:
        supabase.table("annonces_immo") \
            .upsert(rows, on_conflict=["lien"]) \
            .execute()
        print(f"✅ {len(rows)} lignes insérées ou mises à jour.")
    except Exception as e:
        print("❌ Erreur Supabase:", e)


def read_from_supabase(ville=None, quartier=None, prix_max=None, prix_min=None,
                       prix_m2_max=None, prix_m2_min=None, limit=10000000):
    query = supabase.table("annonces_immo").select("*")

    if ville:
        query = query.ilike("ville", ville)
    if quartier:
        query = query.ilike("quartier", quartier)
    if prix_min is not None:
        query = query.gte("prix", prix_min)
    if prix_max is not None:
        query = query.lte("prix", prix_max)
    if prix_m2_min is not None:
        query = query.gte("prix_m2", prix_m2_min)
    if prix_m2_max is not None:
        query = query.lte("prix_m2", prix_m2_max)

    query = query.limit(limit)
    response = query.execute()

    return pd.DataFrame(response.data)


def get_all_villes():
    response = supabase.table("annonces_immo").select("ville").execute()
    villes = pd.DataFrame(response.data)["ville"].dropna().unique()
    return sorted(villes)


def get_quartiers_for_ville(ville):
    response = (
        supabase.table("annonces_immo")
        .select("quartier")
        .eq("ville", ville)
        .execute()
    )
    quartiers = pd.DataFrame(response.data)["quartier"].dropna().unique()
    return sorted(quartiers)




if __name__ == "__main__":
    df_filtered = read_from_supabase(
        ville="Casablanca",
        prix_max=10_000_000,
        prix_m2_max=50_000,
        limit=10
    )
    print(df_filtered.head())

    cities_df = get_all_villes()
    print(cities_df)