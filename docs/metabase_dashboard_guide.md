# Guide de Création du Tableau de Bord Metabase (MVP BI)

Ce document contient les instructions exactes pour configurer les 7 indicateurs de performance clés (KPIs) exigés par le MVP. L'interface décrite correspond à la version française de Metabase.

## 🚀 Étape Préliminaire (Pour chaque KPI)

1. Cliquez sur le bouton **+ (Nouveau)** en haut à droite.
2. Choisissez **Question SQL**.
3. Sélectionnez votre base de données (ex: **Thrift Server** ou **Hive**).
4. Copiez-collez la requête SQL correspondante ci-dessous.
5. Cliquez sur le bouton bleu **Exécuter** (en bas à droite).
6. Cliquez sur le bouton **Visualisation** (en bas à droite) et suivez les réglages spécifiques ci-dessous.
7. Cliquez sur **Enregistrer**, donnez le nom suggéré, et ajoutez-le à votre Dashboard.

---

## 📊 KPI 1 : Total des cours par Faculté

Cette requête affiche le volume de l'offre de formation réparti par université.

```sql
SELECT 
    source_system AS `Faculté`, 
    COUNT(*) AS `Nombre de Formations` 
FROM course_catalog 
GROUP BY source_system
ORDER BY `Nombre de Formations` DESC
```

* **Type de visuel :** `Histogramme`
* **Onglet "Affichage" :** Cochez l'option permettant d'afficher les valeurs sur les points de données (pour que le nombre apparaisse au-dessus des barres).
* **Onglet "Données" :** Vérifiez que l'Axe X est `Faculté` et l'Axe Y est `Nombre de Formations`.

---

## 📈 KPI 2 : Évolution des Actualités dans le temps

Montre le dynamisme de publication au fil des jours.

```sql
SELECT 
    CAST(business_timestamp AS DATE) AS `Date de publication`, 
    COUNT(*) AS `Nombre d articles` 
FROM university_news 
WHERE business_timestamp IS NOT NULL
GROUP BY CAST(business_timestamp AS DATE)
ORDER BY `Date de publication` ASC
```

* **Type de visuel :** `Courbe` ou `Surface`
* **Onglet "Affichage" :** Vous pouvez activer l'option "Ligne de tendance" si vous souhaitez mettre en évidence la croissance globale.
* **Onglet "Données" :** Axe X = `Date de publication`, Axe Y = `Nombre d articles`.

---

## 👨‍🏫 KPI 3 : Corps Enseignants Recensés

Une métrique de vanité simple montrant la portée du scraping des professeurs.

```sql
SELECT 
    COUNT(*) AS `Professeurs Actifs`
FROM faculty_profiles
```

* **Type de visuel :** `Numérique`
* **Onglet "Affichage" :** Vous pouvez ajouter une petite icône dans le champ de description ou préfixe (ex: 👨‍🏫) pour le rendre plus esthétique.

---

## 🌍 KPI 4 : Répartition des Langues du Contenu

Affiche la part des documents en Français, Arabe ou Anglais.

```sql
SELECT 
    language AS `Langue`, 
    COUNT(*) AS `Volume de Contenu` 
FROM unclassified_contents 
WHERE language IS NOT NULL AND language != 'unknown'
GROUP BY language
```

* **Type de visuel :** `Camembert`
* **Onglet "Affichage" :** 
  * Réglez le pourcentage du trou central ("Donut hole") à environ 50% pour un look plus moderne.
  * Assurez-vous d'afficher les pourcentages à côté des parts.

---

## 📚 KPI 5 : Volume Total du Data Lake

Prouve la capacité de votre pipeline Big Data à traiter de grandes masses d'informations.

```sql
SELECT 
    (SELECT COUNT(*) FROM documents_registry) +
    (SELECT COUNT(*) FROM unclassified_contents) +
    (SELECT COUNT(*) FROM university_news) AS `Documents Totaux Ingeres`
```

* **Type de visuel :** `Numérique`
* **Onglet "Affichage" :** Choisissez une couleur vive (Bleu ou Vert) pour ce chiffre très important.

---

## ⏱️ KPI 6 : Fraîcheur des Données

Prouve que l'ingestion Airflow est récente.

```sql
SELECT 
    MAX(crawl_timestamp) AS `Derniere Ingestion`
FROM course_catalog
```

* **Type de visuel :** `Numérique` ou `Détail`
* **Onglet "Données" :** Cliquez sur l'icône de la petite roue crantée (Paramètres) à côté de la colonne "Derniere Ingestion" pour formater l'affichage de la date de manière lisible (ex: Mercredi, 23 Juin 2026 à 15h30).

---

## ⚖️ KPI 7 : Poids des Fichiers par Faculté

Montre l'impact de chaque faculté sur le stockage MinIO.

```sql
SELECT 
    source_system AS `Faculté`, 
    SUM(file_size_bytes) / 1048576.0 AS `Volume en MB` 
FROM documents_registry 
GROUP BY source_system
ORDER BY `Volume en MB` DESC
```

* **Type de visuel :** `Ligne` (Graphique en barres horizontales)
* **Onglet "Données" :** Cliquez sur la roue crantée à côté de "Volume en MB" et forcez l'affichage à **2 décimales** maximum pour un rendu plus propre.

---

## 🧩 Assemblage Final du Tableau de Bord

Une fois vos 7 questions créées et enregistrées, rendez-vous sur votre Tableau de Bord (Dashboard) :

1. Cliquez sur le bouton en forme de crayon (Mode Édition).
2. **Ligne Supérieure :** Placez les 3 KPIs "Numériques" (`Professeurs`, `Documents Totaux`, `Dernière Ingestion`) tout en haut pour que ce soit la première chose que le jury voit.
3. **Deuxième Ligne :** Placez le "Camembert" (`Langues`) et la "Courbe" (`Actualités`) côte à côte.
4. **Ligne Inférieure :** Placez l'"Histogramme" (`Cours par faculté`) et la "Ligne" (`Volume MB par faculté`) en bas.
5. Ajustez la taille des graphiques pour que tout prenne bien l'espace de l'écran.
6. Cliquez sur **Enregistrer** (Save).

Votre Tableau de Bord professionnel est prêt !
