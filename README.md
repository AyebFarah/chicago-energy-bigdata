# Chicago Energy Benchmarking — Big Data Analysis

## Auteurs
| Nom | Prénom |
|-----|--------|
| Talbi | Hamza |
| Souidi | Ons |
| Ayeb | Farah |

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

---

## Structure du projet
\`\`\`
chicago-energy-bigdata/
├── dataset/              # Instructions de téléchargement du CSV
├── mapreduce/            # 3 jobs MapReduce Java
├── spark/                # Analyse Spark DataFrame
├── hbase/                # Stockage HBase + Spark-HBase
├── visualization/        # Scripts Python + graphiques
├── results/              # Sorties des traitements
├── docs/                 # Documentation technique
└── report/               # Rapport final PDF
\`\`\`

---

## Lancer le projet

### Prérequis
- Docker installé et démarré
- Java 1.8
- Maven 3.x
- Python 3.x avec pip

### Étape 1 — Lancer le cluster Hadoop
\`\`\`bash
docker pull liliasfaxi/hadoop-cluster:latest
docker network create --driver=bridge hadoop
docker run -itd --net=hadoop -p 9870:9870 -p 8088:8088 -p 7077:7077 -p 16010:16010 --name hadoop-master --hostname hadoop-master liliasfaxi/hadoop-cluster:latest
docker run -itd -p 8040:8042 --net=hadoop --name hadoop-worker1 --hostname hadoop-worker1 liliasfaxi/hadoop-cluster:latest
docker run -itd -p 8041:8042 --net=hadoop --name hadoop-worker2 --hostname hadoop-worker2 liliasfaxi/hadoop-cluster:latest
\`\`\`

### Étape 2 — Télécharger le dataset
\`\`\`bash
curl -o dataset/energy.csv "https://data.cityofchicago.org/resource/xq83-jr8c.csv?$limit=100000"
\`\`\`

### Étape 3 — MapReduce
Voir \`mapreduce/README.md\`

### Étape 4 — Spark
Voir \`spark/README.md\`

### Étape 5 — HBase
Voir \`hbase/README.md\`

### Étape 6 — Visualisation
\`\`\`bash
cd visualization
pip install pandas matplotlib numpy
python energy_visualizations.py
\`\`\`
