# 📘 EDT Reports — Rapports et comparaisons d’emplois du temps

Cette application **Streamlit** permet de charger un fichier Excel d’emploi du temps (EDT) et de comparer les heures de cours avec une **maquette pédagogique**.  
Elle propose plusieurs vues pour analyser les données : par matière, par enseignant ou sous forme de récapitulatif textuel.

---

## 🚀 Fonctionnalités

- 📂 **Import d’un fichier Excel (.xlsx)** contenant :  
  - une ou deux feuilles `EDT P1`, `EDT P2`,  
  - une feuille `Maquette` décrivant les volumes horaires cibles par matière.  

- 🔎 **Analyse automatique** :  
  - Extraction des séances (matières, enseignants, groupes, horaires).  
  - Regroupement des informations même si les cellules sont fusionnées dans Excel.  
  - Normalisation des noms de groupes (ex : `G1`, `G.1`, `groupe 1` → `G 1`).  

- 📊 **4 pages d’analyse** accessibles via la barre latérale :  
  1. **Comparaison Maquette vs EDT** : heures réalisées vs heures prévues (en cours de développement).  
  2. **Récap par matière** : détail des séances pour chaque matière.  
  3. **Récap par enseignant** : détail des séances pour chaque enseignant.  
  4. **Récapitulatif textuel** : résumé global par promo et matière.  

