# Chicago Energy Benchmarking — Big Data Analysis

## Auteurs
| Nom | Prénom |
|-----|--------|
| Ayeb | Farah|
| Souidi | Ons |
| Talbi | Hamza |

**Classe :** RT4/2 — INSAT  
**Année universitaire :** 2025/2026  
**Enseignante :** Youssef Rabaa

---

## Contexte du projet
Ce projet s'inscrit dans le cadre du module Big Data. Il consiste en une analyse exploratoire
du dataset **Chicago Energy Benchmarking**, disponible sur le Chicago Data Portal.
Ce dataset recense la consommation énergétique des bâtiments commerciaux, résidentiels
et municipaux de plus de 50 000 pieds carrés à Chicago.

---

## Objectifs
1. Implémenter des jobs **MapReduce** pour extraire des statistiques clés
2. Utiliser **Apache Spark** (DataFrame API) pour des analyses plus avancées
3. Stocker les données dans **HBase** et produire des visualisations

---

## Dataset
| Attribut | Valeur |
|----------|--------|
| Source | Chicago Data Portal |
| URL | https://data.cityofchicago.org/resource/xq83-jr8c.csv |
| Format | CSV |
| Taille approximative | ~50 000 enregistrements |
| Dernière mise à jour | Février 2025 |

---

## Stack Technique
| Outil | Version | Rôle |
|-------|---------|------|
| Apache Hadoop | 3.3.6 | Stockage HDFS + YARN |
| Apache Spark | 3.5.0 | Traitement batch |
| Apache HBase | 2.5.8 | Base NoSQL |
| Docker | latest | Conteneurisation du cluster |
| Java | 1.8 | MapReduce + Spark + HBase API |
| Python | 3.x | Visualisation |
| Maven | 3.x | Build tool |



### Prérequis
- Docker installé et démarré
- Java 1.8
- Maven 3.x
- Python 3.x avec pip
