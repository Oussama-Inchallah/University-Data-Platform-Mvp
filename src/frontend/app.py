# ==============================================================================
# © 2026 University Data Platform
# Ce fichier fait partie du système central de la Plateforme Data Universitaire.
# ==============================================================================

"""
Interface Utilisateur (Streamlit)
---------------------------------
Application web permettant d'interroger la plateforme de données via l'API.
Propose une recherche lexicale (Elasticsearch), sémantique (Qdrant) et un RAG (Gemini).
"""

import streamlit as st
import requests
import json
import os
from datetime import datetime

# URL de l'API (nom du service docker)
API_URL = os.getenv("API_URL", "http://search-api:8000")
ES_URL = "http://elasticsearch:9200"

@st.cache_data(ttl=60)
def get_faculties():
    try:
        response = requests.get(f"{API_URL}/faculties")
        response.raise_for_status()
        data = response.json()
        return ["Toutes"] + data.get("faculties", [])
    except Exception:
        return ["Toutes", "FSAC", "FSJESAC", "FSTM"] # Fallback statique

def get_metadata_filters(faculty=None, doc_type=None, language=None):
    try:
        params = {}
        if faculty and faculty != "Toutes": params["faculty"] = faculty
        if doc_type and doc_type != "Toutes": params["doc_type"] = doc_type
        if language and language != "Toutes": params["language"] = language
        
        response = requests.get(f"{API_URL}/metadata_filters", params=params)
        response.raise_for_status()
        return response.json()
    except Exception:
        return {
            "faculties": ["FSAC", "FSJESAC", "FSTM"],
            "doc_types": ["documents_registry", "course_catalog", "website_page", "research_publications"],
            "languages": ["fr", "en", "ar"]
        }

st.set_page_config(
    page_title="University Data Platform",
    page_icon="🎓",
    layout="wide"
)

st.title("🎓 Plateforme Universitaire Intelligente")
st.markdown("Bienvenue sur l'interface unifiée d'exploration des données de l'université.")

# Création des 3 onglets
tab1, tab2, tab3, tab4 = st.tabs([
    "🤖 Assistant IA (RAG Hybride)", 
    "🔍 Moteur de Recherche (ES)", 
    "🧠 Recherche Sémantique (Qdrant)",
    "📈 Observabilité de la Plateforme"
])

# ---------------------------------------------------------
# ONGLET 1 : ASSISTANT IA (QDRANT / SEMANTIQUE)
# ---------------------------------------------------------
with tab1:
    st.header("Posez une question à l'Assistant IA (Hybride ES + Qdrant)")
    st.markdown("Cet assistant hybride combine la précision des mots-clés d'Elasticsearch avec la compréhension sémantique de Qdrant.")
    
    query_rag = st.text_input("Votre question :", placeholder="Ex: Quelles sont les conditions d'admission en Master ?", key="rag_input")
    
    if st.button("Demander à l'IA", type="primary"):
        if query_rag:
            with st.spinner("L'IA réfléchit..."):
                try:
                    response = requests.get(f"{API_URL}/search/semantic", params={"q": query_rag})
                    response.raise_for_status()
                    data = response.json()
                    
                    results = data.get("results", [])
                    llm_answer = data.get("llm_answer")
                    
                    if llm_answer:
                        st.info("💡 **Réponse de l'IA :**")
                        st.write(llm_answer)
                        st.divider()
                        
                    if results:
                        st.success(f"Voici les {len(results)} sources utilisées par l'IA pour générer cette réponse :")
                        for i, res in enumerate(results):
                            with st.expander(f"📚 Source {i+1} (Score RRF: {res.get('score', 0):.2f}) - {res.get('faculty', 'Inconnu')}"):
                                st.markdown(f"**Texte extrait :**\n\n> {res.get('text', '')}")
                                if res.get('url'):
                                    st.markdown(f"**Lien original :** [{res.get('url')}]({res.get('url')})")
                    else:
                        st.warning("Aucune information pertinente trouvée.")
                except Exception as e:
                    st.error(f"Erreur de connexion à l'API IA : {e}")

# ---------------------------------------------------------
# ONGLET 2 : MOTEUR DE RECHERCHE (ELASTICSEARCH / KEYWORD)
# ---------------------------------------------------------
with tab2:
    st.header("Recherche dans les Catalogues et Actualités")
    st.markdown("Recherche classique par mots-clés (titres, descriptions de cours, news).")
    
    col1, col2 = st.columns([3, 1])
    with col1:
        query_kw = st.text_input("Mots-clés :", placeholder="Ex: informatique", key="kw_input")
    with col2:
        # Liste dynamique des facultés
        faculty_filter = st.selectbox("Faculté :", get_faculties())
    
    if st.button("Rechercher", key="btn_kw"):
        if query_kw:
            with st.spinner("Recherche en cours..."):
                try:
                    params = {"q": query_kw}
                    if faculty_filter != "Toutes":
                        params["faculty"] = faculty_filter
                        
                    response = requests.get(f"{API_URL}/search/keyword", params=params)
                    response.raise_for_status()
                    data = response.json()
                    
                    results = data.get("results", [])
                    total = data.get("total_hits", 0)
                    facets = data.get("facets", [])
                    
                    # Construction du texte pour les facettes
                    facets_text = " | ".join([f"{f['key']} ({f['doc_count']})" for f in facets])
                    if facets_text:
                        st.caption(f"Répartition des résultats : {facets_text}")
                    
                    st.success(f"{total} documents trouvés (Affichage des 10 meilleurs).")
                    
                    for res in results:
                        st.card = st.container()
                        with st.card:
                            st.markdown(f"### 📄 {res.get('type', 'Document').replace('_', ' ').title()}")
                            st.caption(f"🏫 Faculté : **{res.get('faculty', 'Inconnu')}** | 🌍 Langue : {res.get('language', 'Inconnu')} | 🎯 Score : {res.get('score', 0):.2f}")
                            st.markdown(res.get('snippet', ''), unsafe_allow_html=True)
                            st.divider()
                except Exception as e:
                    st.error(f"Erreur de connexion au Moteur de Recherche : {e}")

# ---------------------------------------------------------
# ONGLET 3 : RECHERCHE SEMANTIQUE PURE (QDRANT)
# ---------------------------------------------------------
with tab3:
    st.header("Exploration Documentaire Sémantique (Avancée)")
    st.markdown("Recherche de documents par proximité de sens combinée avec des filtres stricts de métadonnées.")
    
    query_qdrant = st.text_input("Idée ou concept recherché :", placeholder="Ex: Procédure de validation des modules", key="qdrant_input")
    
    # Récupérer l'état actuel des filtres (ou 'Toutes' par défaut)
    current_doc = st.session_state.get("qdrant_doc_type", "Toutes")
    current_fac = st.session_state.get("qdrant_faculty", "Toutes")
    current_lang = st.session_state.get("qdrant_language", "Toutes")
    
    # Appel dynamique à l'API pour récupérer les listes en cascade
    filters = get_metadata_filters(faculty=current_fac, doc_type=current_doc, language=current_lang)
    
    # Si la valeur sélectionnée n'est plus dans la liste renvoyée (ex: on change de fac, et ce type n'existe pas), on force "Toutes"
    doc_options = ["Toutes"] + filters["doc_types"]
    if current_doc not in doc_options: current_doc = "Toutes"
        
    fac_options = ["Toutes"] + filters["faculties"]
    if current_fac not in fac_options: current_fac = "Toutes"
        
    lang_options = ["Toutes"] + filters["languages"]
    if current_lang not in lang_options: current_lang = "Toutes"
    
    col_a, col_b, col_c = st.columns(3)
    with col_a:
        doc_type_filter = st.selectbox("Type :", doc_options, index=doc_options.index(current_doc), key="qdrant_doc_type")
    with col_b:
        faculty_filter_q = st.selectbox("Faculté :", fac_options, index=fac_options.index(current_fac), key="qdrant_faculty")
    with col_c:
        language_filter = st.selectbox("Langue :", lang_options, index=lang_options.index(current_lang), key="qdrant_language")
    
    if st.button("Chercher", key="btn_qdrant"):
        if query_qdrant:
            with st.spinner("Recherche vectorielle et filtrage dans Qdrant..."):
                try:
                    payload = {"query": query_qdrant}
                    
                    if doc_type_filter != "Toutes":
                        payload["type"] = doc_type_filter
                    if faculty_filter_q != "Toutes":
                        payload["faculty"] = faculty_filter_q
                    if language_filter != "Toutes":
                        payload["language"] = language_filter
                        
                    response = requests.post(f"{API_URL}/search/qdrant_advanced", json=payload)
                    response.raise_for_status()
                    data = response.json()
                    
                    results = data.get("results", [])
                    filters_applied = data.get("filters_applied", 0)
                    
                    if results:
                        st.success(f"Voici les {len(results)} segments sémantiquement les plus proches (avec {filters_applied} filtres appliqués) :")
                        for i, res in enumerate(results):
                            title_parts = [
                                f"🧩 Correspondance {i+1} (Score vectoriel: {res.get('score', 0):.2f})",
                                f"Type: {res.get('type', 'Inconnu')}",
                                f"Site: {res.get('faculty', 'Inconnu')}"
                            ]
                            if res.get('language'):
                                title_parts.append(f"Langue: {res.get('language')}")
                                
                            expander_title = " - ".join(title_parts)
                            
                            with st.expander(expander_title):
                                st.markdown(f"**Texte :**\n\n> {res.get('text', '')}")
                                
                                meta_info = f"Faculté : **{res.get('faculty', 'Inconnu')}**"
                                if res.get('language'):
                                    meta_info += f" | Langue: **{res.get('language')}**"
                                    
                                st.caption(meta_info)
                                
                                if res.get('url'):
                                    st.markdown(f"[Voir la source]({res.get('url')})")
                    else:
                        st.warning("Aucun document sémantiquement proche n'a été trouvé avec ces filtres.")
                except Exception as e:
                    st.error(f"Erreur de connexion à Qdrant via l'API : {e}")

# ---------------------------------------------------------
# ONGLET 4 : OBSERVABILITE (DATAOPS)
# ---------------------------------------------------------
def fetch_logs(must_conditions=None):
    query = {"match_all": {}} if not must_conditions else {"bool": {"must": must_conditions}}
    es_query = {
        "query": query,
        "sort": [{"timestamp": {"order": "desc"}}],
        "size": 50
    }
    try:
        res = requests.post(f"{ES_URL}/platform_logs/_search", json=es_query)
        if res.status_code == 404:
            return None
        res.raise_for_status()
        return res.json()["hits"]["hits"]
    except Exception as e:
        st.error(f"Erreur: {e}")
        return []

def display_logs(hits):
    if not hits:
        st.info("Aucun log trouvé.")
        return
        
    for hit in hits:
        src = hit["_source"]
        status = src.get("status", "UNKNOWN")
        color = "green" if status == "SUCCESS" else "red" if status == "FAILED" else "orange"
        
        st.markdown(f"**[{src.get('timestamp', '')}]** <span style='color:{color}'>**{status}**</span> : Job **{src.get('job_name', '')}**", unsafe_allow_html=True)
        if status == "FAILED" and src.get('error_message'):
            st.code(src.get('error_message'))

with tab4:
    st.header("Surveillance des Pipelines de Données")
    
    sub1, sub2, sub3, sub4 = st.tabs([
        "🕷️ Logs Scraper", 
        "⚙️ Logs d'exécution ETL", 
        "⚠️ Erreurs d'ingestion", 
        "📊 Tableaux de bord"
    ])
    
    with sub1:
        st.subheader("Logs des robots d'extraction (Crawlers)")
        if st.button("Rafraîchir Logs Scraper", key="btn_scraper"):
            with st.spinner("Récupération..."):
                hits = fetch_logs([{"wildcard": {"job_name": "*scraper*"}}])
                if hits is None: st.info("Index introuvable.")
                else: display_logs(hits)
                
    with sub2:
        st.subheader("Logs de traitement et d'indexation (Spark/ETL)")
        if st.button("Rafraîchir Logs ETL", key="btn_etl"):
            with st.spinner("Récupération..."):
                # On filtre tout ce qui n'est pas scraper
                hits = fetch_logs([
                    {"bool": {"must_not": [{"wildcard": {"job_name": "*scraper*"}}]}}
                ])
                if hits is None: st.info("Index introuvable.")
                else: display_logs(hits)
                
    with sub3:
        st.subheader("Erreurs et alertes d'ingestion")
        if st.button("Rafraîchir Erreurs", key="btn_err"):
            with st.spinner("Récupération..."):
                hits = fetch_logs([{"term": {"status": "FAILED"}}])
                if hits is None: st.info("Index introuvable.")
                else: display_logs(hits)
                
    with sub4:
        st.subheader("📊 Santé des Pipelines (DataOps)")
        st.markdown("Vue consolidée de la fiabilité de chaque pipeline d'ingestion.")
        
        if st.button("Charger le Dashboard DataOps", key="btn_stats", type="primary"):
            try:
                # --- Ligne 0 : Grands chiffres ---
                res_global = requests.post(f"{ES_URL}/platform_logs/_search", json={
                    "size": 0,
                    "aggs": {"statuses": {"terms": {"field": "status"}}}
                })
                if res_global.status_code == 200:
                    aggs = res_global.json().get("aggregations", {}).get("statuses", {}).get("buckets", [])
                    total = sum(b["doc_count"] for b in aggs)
                    success = next((b["doc_count"] for b in aggs if b["key"] == "SUCCESS"), 0)
                    failed = next((b["doc_count"] for b in aggs if b["key"] == "FAILED"), 0)
                    taux = round(success * 100.0 / total, 1) if total > 0 else 0
                    
                    c1, c2, c3, c4 = st.columns(4)
                    c1.metric("Total Exécutions", total)
                    c2.metric("✅ Succès", success)
                    c3.metric("❌ Échecs", failed)
                    c4.metric("Taux de Fiabilité", f"{taux}%")
                    
                    if taux >= 90:
                        st.success(f"🟢 Plateforme en bonne santé — Taux de fiabilité de {taux}%")
                    elif taux >= 70:
                        st.warning(f"🟡 Attention — Taux de fiabilité de {taux}%")
                    else:
                        st.error(f"🔴 Critique — Taux de fiabilité de {taux}%")
                
                st.divider()
                
                # --- Tableau détaillé par Pipeline ---
                st.markdown("#### Détail par Pipeline")
                res_pipelines = requests.post(f"{ES_URL}/platform_logs/_search", json={
                    "size": 0,
                    "aggs": {
                        "pipelines": {
                            "terms": {"field": "job_name", "size": 50},
                            "aggs": {
                                "statuses": {"terms": {"field": "status"}},
                                "last_run": {"max": {"field": "timestamp"}}
                            }
                        }
                    }
                })
                
                if res_pipelines.status_code == 200:
                    pipelines = res_pipelines.json().get("aggregations", {}).get("pipelines", {}).get("buckets", [])
                    
                    if pipelines:
                        import pandas as pd
                        rows = []
                        for p in pipelines:
                            name = p["key"]
                            p_total = p["doc_count"]
                            p_success = next((s["doc_count"] for s in p["statuses"]["buckets"] if s["key"] == "SUCCESS"), 0)
                            p_failed = next((s["doc_count"] for s in p["statuses"]["buckets"] if s["key"] == "FAILED"), 0)
                            p_taux = round(p_success * 100.0 / p_total, 1) if p_total > 0 else 0
                            last_run = p.get("last_run", {}).get("value_as_string", "N/A")
                            
                            status_emoji = "✅" if p_failed == 0 else "⚠️" if p_taux >= 80 else "❌"
                            rows.append({
                                "Statut": status_emoji,
                                "Pipeline": name,
                                "Succès": p_success,
                                "Échecs": p_failed,
                                "Total": p_total,
                                "Fiabilité (%)": p_taux,
                                "Dernière exécution": last_run
                            })
                        
                        df = pd.DataFrame(rows).sort_values("Fiabilité (%)", ascending=True)
                        st.dataframe(df, use_container_width=True, hide_index=True)
                    else:
                        st.info("Aucun pipeline trouvé dans les logs.")
                        
            except Exception as e:
                st.error(f"Erreur de récupération des métriques : {e}")
