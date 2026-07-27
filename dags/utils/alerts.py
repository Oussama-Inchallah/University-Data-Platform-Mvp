from airflow.utils.email import send_email
from datetime import timedelta
import logging

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
        # L'adresse destinataire approuvée par l'utilisateur
        send_email("badreddineabba@gmail.com", subject, html_content)
        logging.info(f"Alerte email envoyée avec succès à badreddineabba@gmail.com pour {task_id}")
    except Exception as e:
        logging.error(f"Échec de l'envoi de l'alerte email : {e}")



# Configuration globale pour tous les DAGs
global_default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'email': ['badreddineabba@gmail.com'],
    'email_on_failure': False,  # On désactive le mail par défaut d'Airflow car on utilise notre callback personnalisé plus lisible
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': failure_alert
}
