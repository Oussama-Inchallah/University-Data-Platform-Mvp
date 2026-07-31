# ==============================================================================
# © 2026 University Data Platform
# Ce fichier fait partie du système central de la Plateforme Data Universitaire.
# ==============================================================================

"""
Système d'Alerting (DataOps)
----------------------------
Gère l'envoi de notifications (Email, Slack, etc.) en cas d'échec des pipelines.
Définit également les paramètres par défaut (Retry, Timeout) partagés par tous les DAGs.
"""

from airflow.utils.email import send_email
from datetime import timedelta
import logging
import os

# Extraction de la liste des emails destinataires (séparés par des virgules)
dest_emails_str = os.environ.get('SMTP_MAIL_TO', 'admin@example.com')
DEST_EMAILS = [email.strip() for email in dest_emails_str.split(',') if email.strip()]

def failure_alert(context):
    """
    Callback exécuté lors de l'échec d'une tâche.
    Envoie un email détaillé avec l'erreur exacte.
    """
    task_instance = context.get('task_instance')
    dag_id = task_instance.dag_id
    task_id = task_instance.task_id
    exception = context.get('exception')
    log_url = task_instance.log_url

    subject = f"🔴 [Airflow] Échec du DAG: {dag_id} - Tâche: {task_id}"
    
    html_content = f"""
    <h2>Échec d'une tâche Airflow</h2>
    <ul>
        <li><b>DAG :</b> {dag_id}</li>
        <li><b>Tâche :</b> {task_id}</li>
        <li><b>Date d'exécution :</b> {context.get('execution_date')}</li>
        <li><b>Lien des Logs :</b> <a href="{log_url}">Ouvrir dans Airflow</a></li>
    </ul>
    <h3>Erreur reçue :</h3>
    <pre style="background-color: #f8d7da; color: #721c24; padding: 10px; border: 1px solid #f5c6cb; font-family: monospace;">
{str(exception)}
    </pre>
    """
    
    try:
        if DEST_EMAILS:
            send_email(DEST_EMAILS, subject, html_content)
            logging.info(f"Alerte email envoyée avec succès à {DEST_EMAILS} pour {task_id}")
        else:
            logging.warning(f"Aucune adresse email configurée (SMTP_MAIL_TO) pour envoyer l'alerte de {task_id}")
    except Exception as e:
        logging.error(f"Échec de l'envoi de l'alerte email : {e}")

def dag_failure_alert(context):
    """
    Callback exécuté lors de l'échec global d'un DAG (ex: OOM Killer, Timeout total).
    Capté directement par le Scheduler Airflow, même si le Worker est mort.
    """
    dag_run = context.get('dag_run')
    dag_id = dag_run.dag_id
    
    subject = f"🚨 [CRITIQUE] Échec global du DAG: {dag_id}"
    
    html_content = f"""
    <h2>Échec Global d'un Pipeline Airflow</h2>
    <ul>
        <li><b>DAG :</b> {dag_id}</li>
        <li><b>Date d'exécution :</b> {context.get('execution_date')}</li>
        <li><b>Raison probable :</b> Le système a tué le processus brutalement (Out Of Memory - Exit 137), le Worker Airflow a planté, ou une erreur système majeure est survenue.</li>
    </ul>
    <h3>Action requise :</h3>
    <p>Vérifiez les logs de l'infrastructure Docker (<code>docker compose logs airflow-worker</code>) et l'allocation mémoire de votre serveur ou Docker Desktop.</p>
    """
    
    try:
        if DEST_EMAILS:
            send_email(DEST_EMAILS, subject, html_content)
            logging.info(f"Alerte email de DAG envoyée avec succès à {DEST_EMAILS} pour le crash de {dag_id}")
    except Exception as e:
        logging.error(f"Échec de l'envoi de l'alerte email de DAG : {e}")



# Configuration globale pour tous les DAGs
global_default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'email': DEST_EMAILS,
    'email_on_failure': False,  # On désactive le mail par défaut d'Airflow car on utilise notre callback personnalisé plus lisible
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': failure_alert
}
