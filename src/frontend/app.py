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

st.set_page_config(
    page_title="University Data Platform",
    page_icon="🎓",
    layout="wide"
)

st.title("🎓 Plateforme Universitaire Intelligente")
st.markdown("Bienvenue sur l'interface unifiée d'exploration des données de l'université.")

# Création des 3 onglets
tab1, tab2, tab3 = st.tabs([
    "🤖 Assistant IA (RAG)", 
    "🔍 Moteur de Recherche", 
    "📈 Platform Monitoring (DataOps)"
])

# ---------------------------------------------------------
# ONGLET 1 : ASSISTANT IA (QDRANT / SEMANTIQUE)
# ---------------------------------------------------------
with tab1:
    st.header("Posez une question à l'Assistant IA")
    st.markdown("Cet assistant comprend le **sens** de votre question et cherche dans les règlements, thèses et syllabus.")
    
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
                            with st.expander(f"📚 Source {i+1} (Pertinence: {res.get('score', 0):.2f}) - {res.get('faculty', 'Inconnu')}"):
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
                            st.write(res.get('snippet', ''))
                            st.divider()
                except Exception as e:
                    st.error(f"Erreur de connexion au Moteur de Recherche : {e}")

# ---------------------------------------------------------
# ONGLET 3 : PLATFORM MONITORING (DATAOPS)
# ---------------------------------------------------------
with tab3:
    st.header("Surveillance des Pipelines de Données")
    st.markdown("Analyse des logs d'exécution Airflow et Spark stockés dans Elasticsearch.")
    
    if st.button("Rafraîchir les Logs Systèmes"):
        with st.spinner("Connexion à Elasticsearch..."):
            try:
                # Requête directe vers ES (platform_logs)
                es_query = {
                    "query": {"match_all": {}},
                    "sort": [{"timestamp": {"order": "desc"}}],
                    "size": 10
                }
                res = requests.post(f"{ES_URL}/platform_logs/_search", json=es_query)
                
                if res.status_code == 404:
                    st.info("L'index `platform_logs` n'existe pas encore. Aucun log n'a été enregistré pour le moment.")
                else:
                    res.raise_for_status()
                    hits = res.json()["hits"]["hits"]
                    
                    if not hits:
                        st.info("Aucun log trouvé dans l'index.")
                    else:
                        st.success("Logs récupérés avec succès.")
                        for hit in hits:
                            src = hit["_source"]
                            status = src.get("status", "UNKNOWN")
                            color = "green" if status == "SUCCESS" else "red" if status == "FAILED" else "orange"
                            
                            st.markdown(f"**[{src.get('timestamp', '')}]** <span style='color:{color}'>**{status}**</span> : Job **{src.get('job_name', '')}**", unsafe_allow_html=True)
                            if status == "FAILED" and src.get('error_message'):
                                st.code(src.get('error_message'))
            except Exception as e:
                st.error(f"Impossible de joindre le système de monitoring : {e}")
