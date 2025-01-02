# **ETL proces datasetu MovieLens**

Tento repozitár obsahuje implementáciu ETL procesu v Snowflake pre analýzu dát z datasetu MovieLens. Cieľom projektu je preskúmať používateľské správanie a preferencie pri hodnotení filmov. Výsledný dátový model umožňuje multidimenzionálnu analýzu a vizualizáciu kľúčových metrik.

---
## **1. Úvod a popis zdrojových dát**

Cieľom semestrálneho projektu je analyzovať dáta o filmoch, používateľoch, hodnoteniach a súvisiacich atribútoch, aby sa odhalili trendy v preferenciách filmov a správaní používateľov.

Zdrojové dáta pochádzajú z datasetu **MovieLens**, dostupného [tu](https://grouplens.org/datasets/movielens/).
Dataset obsahuje nasledujúce hlavné tabuľky:

- `age_group` - Skupiny podľa veku používateľov.
- `genres` - Informácie o filmových žánroch.
- `movies` - Detaily o filmoch, vrátane názvov a rokov vydania.
- `ratings` - Hodnotenia filmov od používateľov.
- `tags` - Štítky pridelené používateľmi k filmom.
- `users` - Demografické údaje o používateľoch.

Účelom ETL procesu bolo tieto dáta pripraviť, transformovať a sprístupniť pre viacdimenzionálnu analýzu.

---
### **1.1 Dátová architektúra**

### **ERD diagram**
Surové dáta sú usporiadané v relačnom modeli, ktorý je znázornený na **entitno-relačnom diagrame (ERD)**:

<p align="center">
  <img src="https://github.com/KV1k1/JELLYFISH_MovieLens_DB/blob/main/MovieLens_ERD.png" alt="ERD Schema">
  <br>
  <em>Obrázok 1 Entitno-relačná schéma MovieLens</em>
</p>

---
## **2. Dimenzionálny model**

Navrhnutý bol **hviezdicový model (star schema)**, pre efektívnu analýzu kde centrálny bod predstavuje faktová tabuľka **`fact_ratings`**, ktorá je prepojená s nasledujúcimi dimenziami:

-  **`dim_movies`**: Detaily o filmoch (názov, žáner, rok vydania).
-  **`dim_users`**: Demografické informácie o používateľoch (vek, pohlavie, povolanie).
-  **`dim_date`**: Dátumy hodnotení (deň, mesiac, rok).
-  **`dim_time`**: Časové údaje (hodina, minuta, sekunda, AM/PM).
-  **`dim_genres`**: Žánre filmov.

Štruktúra hviezdicového modelu  je znázornená na diagrame nižšie. Diagram ukazuje prepojenia medzi faktovou tabuľkou a dimenziami, čo zjednodušuje pochopenie a implementáciu modelu.

<p align="center">
  <img src="https://github.com/KV1k1/JELLYFISH_MovieLens_DB/blob/main/star_schema_vk.png" alt="Star Schema">
  <br>
  <em>Obrázok 2 Schéma hviezdy pre MovieLens</em>
</p>

---
## **3. ETL proces v Snowflake**

ETL proces pozostával z troch hlavných fáz: `extrahovanie` (Extract), `transformácia` (Transform) a `načítanie` (Load). Tento proces bol implementovaný v Snowflake s cieľom pripraviť zdrojové dáta zo staging vrstvy do viacdimenzionálneho modelu vhodného na analýzu a vizualizáciu.

---
### **3.1 Extract (Extrahovanie dát)**
Dáta zo zdrojového datasetu (formát `.csv`) boli najprv nahraté do Snowflake prostredníctvom interného stage úložiska s názvom `my_stage`. Stage v Snowflake slúži ako dočasné úložisko na import alebo export dát. Vytvorenie stage bolo zabezpečené príkazom:

#### Príklad kódu:
```sql
CREATE OR REPLACE STAGE MovieLens_Stage;
```

Do stage boli následne nahraté súbory obsahujúce údaje o knihách, používateľoch, hodnoteniach, zamestnaniach a úrovniach vzdelania. Dáta boli importované do staging tabuliek pomocou príkazu COPY INTO. Pre každú tabuľku sa použil podobný príkaz:

```sql
COPY INTO age_group_staging
FROM @MovieLens_Stage/age_group.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);
```

Podobne boli spracované ostatné tabuľky, pričom nekonzistentné záznamy boli spracované s `ON_ERROR = 'CONTINUE'`, ktorý zabezpečil pokračovanie procesu bez prerušenia pri chybách.

---
### **3.2 Transfor (Transformácia dát)**
V tejto fáze boli dáta zo staging tabuliek vyčistené, transformované a obohatené. Hlavným cieľom bolo pripraviť dimenzie a faktovú tabuľku, ktoré umožnia jednoduchú a efektívnu analýzu.

Dimenzie boli navrhnuté na poskytovanie kontextu pre faktovú tabuľku. Každá dimenzia je klasifikovaná podľa typu **Slowly Changing Dimensions (SCD)** podľa toho, ako sa správa pri zmenách údajov.

#### **Dimenzia dim_users**

Dimenzia `dim_users` obsahuje údaje o používateľoch, vrátane veku a zamestnania. Transformácia zahŕňala rozdelenie veku používateľov do kategórií (napr. „18-24“). Táto dimenzia je **SCD Typ 2**, čo znamená, že sleduje historické zmeny. Každá zmena je zaznamenaná ako nový záznam s časovým označením, pričom staré hodnoty sa zachovávajú.

```sql
CREATE TABLE dim_users AS
SELECT DISTINCT
    u.id AS dim_userID,
    ag.name AS age_group,
    u.gender,
    u.zip_code,
    o.name AS occupation
FROM users_staging u
LEFT JOIN age_group_staging ag ON u.age = ag.id
LEFT JOIN occupations_staging o ON u.occupation_id = o.id;
```

SCD Typ 2 – Každá zmena v týchto atribútoch vytvára nový záznam v dimenzii, pričom staré hodnoty sú archivované pre historické účely.

#### **Dimenzia dim_date**

Dimenzia `dim_date` obsahuje informácie o dátumoch hodnotení filmov. Zabezpečuje analýzu trendov podľa dní, mesiacov, rokov, týždňov a štvrťrokov. Táto dimenzia je **SCD Typ 0**, čo znamená, že údaje v tejto dimenzii sú nemenné a nebudú sa aktualizovať. Ak by bolo potrebné sledovať ďalšie historické zmeny (napr. pre pracovné dni vs. sviatky), klasifikácia by mohla byť prehodnotená na SCD Typ 1 (aktualizácia existujúcich hodnôt) alebo SCD Typ 2 (uchovávanie histórie).

```sql
CREATE TABLE dim_dates AS
SELECT DISTINCT
    ROW_NUMBER() OVER (ORDER BY CAST(rated_at AS DATE)) AS dim_dateID,
    CAST(rated_at AS DATE) AS rated_at,
    DATE_PART('day', rated_at) AS day,
    DATE_PART('dow', rated_at) + 1 AS day_of_week,
    CASE DATE_PART('dow', rated_at) + 1
        WHEN 1 THEN 'Monday'
        WHEN 2 THEN 'Tuesday'
        WHEN 3 THEN 'Wednesday'
        WHEN 4 THEN 'Thursday'
        WHEN 5 THEN 'Friday'
        WHEN 6 THEN 'Saturday'
        WHEN 7 THEN 'Sunday'
    END AS day_of_week_string,
    DATE_PART('week', rated_at) AS week,
    DATE_PART('month', rated_at) AS month,
    CASE DATE_PART('month', rated_at)
        WHEN 1 THEN 'January'
        WHEN 2 THEN 'February'
        WHEN 3 THEN 'March'
        WHEN 4 THEN 'April'
        WHEN 5 THEN 'May'
        WHEN 6 THEN 'June'
        WHEN 7 THEN 'July'
        WHEN 8 THEN 'August'
        WHEN 9 THEN 'September'
        WHEN 10 THEN 'October'
        WHEN 11 THEN 'November'
        WHEN 12 THEN 'December'
    END AS month_string,
    DATE_PART('year', rated_at) AS year,
    DATE_PART('quarter', rated_at) AS quarter
FROM ratings_staging
GROUP BY CAST(rated_at AS DATE), 
         DATE_PART(day, rated_at), 
         DATE_PART(dow, rated_at), 
         DATE_PART(month, rated_at), 
         DATE_PART(year, rated_at), 
         DATE_PART(week, rated_at), 
         DATE_PART(quarter, rated_at);
```

SCD Typ 0 – Dáta sú nemenné, žiadne historické zmeny sa nezachovávajú.

#### **Dimenzia dim_movies**

Dimenzia `dim_movies` obsahuje údaje o filmoch, ako sú názov, rok vydania a žánre. Táto dimenzia je **SCD Typ 0**.

```sql
CREATE TABLE dim_movies AS
SELECT 
    m.id AS dim_movieID,
    m.title,
    m.release_year,
    LISTAGG(g.name, ', ') WITHIN GROUP (ORDER BY g.name) AS genre -- Assuming multiple genres per movie
FROM movies_staging m
LEFT JOIN genres_movies_staging gm ON m.id = gm.movie_id
LEFT JOIN genres_staging g ON gm.genre_id = g.id
GROUP BY m.id, m.title, m.release_year;
```

#### **Dimenzia dim_time**

Dimenzia `dim_time` poskytuje podrobnosti o čase hodnotení, ako sú hodiny, minúty, sekundy a AM/PM rozlíšenie. Táto dimenzia je **SCD Typ 0**, pretože údaje o čase sú považované za nemenné. Ak by sa čas hodnotení zmenil, nový záznam by bol pridaný.

```sql
CREATE TABLE dim_time AS
SELECT DISTINCT
    ROW_NUMBER() OVER (ORDER BY DATE_TRUNC('HOUR', rated_at)) AS dim_timeID,
    TIME(rated_at) AS time,
    DATE_PART('hour', rated_at) AS hour,
    DATE_PART('minute', rated_at) AS minute,
    DATE_PART('second', rated_at) AS second,
    CASE
        WHEN DATE_PART('hour', rated_at) < 12 THEN 'AM'
        ELSE 'PM'
    END AS ampm
FROM ratings_staging
GROUP BY rated_at;
```

#### **Faktová tabuľka fact_ratings**

Faktová tabuľka `fact_ratings` obsahuje záznamy o hodnoteniach filmov, s prepojeniami na všetky dimenzie. Táto tabuľka je **SCD Typ 0**, pretože hodnotenia sú považované za jednorazové záznamy, ktoré sa neaktualizujú ani nemenia.

```sql
CREATE TABLE fact_ratings AS
SELECT DISTINCT
       r.id AS fact_ratingID,
       r.rated_at AS rating_datetime,
       r.rating AS rating,
       du.dim_userID AS dim_userID,
       dm.dim_movieID AS dim_movieID,
       dd.dim_dateID AS dim_dateID,
       dt.dim_timeID AS dim_timeID
FROM ratings_staging r
JOIN dim_dates dd ON CAST(r.rated_at AS DATE) = dd.rated_at
JOIN dim_time dt ON CAST(r.rated_at AS TIME) = dt.time
JOIN dim_users du ON du.dim_userID = r.user_id
JOIN dim_movies dm ON dm.dim_movieID = r.movie_id;
```

---
### **3.3 Load (Načítanie dát)**

Po úspešnom vytvorení dimenzií a faktovej tabuľky boli dáta nahrané do finálnej štruktúry. Na záver boli staging tabuľky odstránené, aby sa optimalizovalo využitie úložiska:

```sql
DROP TABLE IF EXISTS age_group_staging;
DROP TABLE IF EXISTS genres_staging;
DROP TABLE IF EXISTS movies_staging;
DROP TABLE IF EXISTS genres_movies_staging;
DROP TABLE IF EXISTS occupations_staging;
DROP TABLE IF EXISTS users_staging;
DROP TABLE IF EXISTS ratings_staging;
DROP TABLE IF EXISTS tags_staging;
```

ETL proces v Snowflake umožnil spracovanie pôvodných dát z `.csv` formátu do viacdimenzionálneho modelu typu hviezda. Tento proces zahŕňal čistenie, obohacovanie a reorganizáciu údajov. Výsledný model umožňuje analýzu čitateľských preferencií a správania používateľov, pričom poskytuje základ pre vizualizácie a reporty.

---
## **4 Vizualizácia dát**

Dashboard obsahuje `6 vizualizácií`, ktoré poskytujú základný prehľad o kľúčových metrikách a trendoch týkajúcich sa filmov, používateľov a hodnotení. Tieto vizualizácie odpovedajú na dôležité otázky a umožňujú lepšie pochopiť správanie používateľov a ich preferencie.

<p align="center">
  <img src="https://github.com/KV1k1/JELLYFISH_MovieLens_DB/blob/main/MovieLens_dashboard.png" alt="ERD Schema">
  <br>
  <em>Obrázok 3 Dashboard MovieLens datasetu</em>
</p>

---
### **Graf 1: Popularita žánru podľa počtu hodnotení (top 10)**


Tento graf zobrazuje 10 žánrov s najväčším počtom hodnotení filmov. Umožňuje identifikovať, ktoré žánre sú medzi používateľmi najpopulárnejšie. Z vizualizácie sa napríklad ukazuje, že žánre ako „Akcia“ a „Dobrodružný“ majú výrazne viac hodnotení ako iné žánre. Tieto informácie môžu byť cenné pri tvorbe marketingových stratégií alebo odporúčacích systémov.

<p align="center">
  <img src="https://github.com/KV1k1/JELLYFISH_MovieLens_DB/blob/main/graphs/top_10_genres.png" alt="Graf">
  <br>
  <em>Obrázok 4 Graf 1</em>
</p>

```sql
SELECT 
    dm.genre AS genre,
    COUNT(fr.fact_ratingID) AS total_ratings
FROM fact_ratings fr
JOIN dim_movies dm ON fr.dim_movieID = dm.dim_movieID
GROUP BY dm.genre
ORDER BY total_ratings DESC
LIMIT 10;
```
---
### **Graf 2: Rozdelenie hodnotení podľa pohlavia a času dňa**
Tento graf zobrazuje, ako sa hodnotenia delia podľa pohlavia používateľov a času dňa. Ukazuje sa, že ženy častejšie hodnotia filmy doobeda, zatiaľ čo muži majú tendenciu hodnotiť viac poobede. Tieto informácie môžu byť užitočné pri plánovaní kampaní, ktoré sa zameriavajú na určité časové obdobia.

<p align="center">
  <img src="https://github.com/KV1k1/JELLYFISH_MovieLens_DB/blob/main/graphs/gender_ampm.png" alt="Graf">
  <br>
  <em>Obrázok 5 Graf 2</em>
</p>

```sql
SELECT 
    dt.ampm AS time_period,
    du.gender,
    COUNT(fr.fact_ratingID) AS total_ratings
FROM fact_ratings fr
JOIN dim_time dt ON fr.dim_timeID = dt.dim_timeID
JOIN dim_users du ON fr.dim_userID = du.dim_userID
GROUP BY dt.ampm, du.gender
ORDER BY time_period, du.gender;
```
---
### **Graf 3: Celkové hodnotenia používateľov vs priemerné hodnotenie**
Tento graf zobrazuje celkový počet hodnotení, ktoré jednotliví používatelia udelili, a ich priemerné hodnotenie. Z vizualizácie môžeme zistiť, že napríklad používatelia v kategórii „55+“ majú nižší počet hodnotení, ale ich priemerné hodnotenie filmov je o niečo vyššie ako u mladších používateľov. Tieto údaje môžu byť použité na lepšie prispôsobenie odporúčaní na základe vekovej kategórie alebo profesie.

<p align="center">
  <img src="https://github.com/KV1k1/JELLYFISH_MovieLens_DB/blob/main/graphs/total_vs_avg_rating.png" alt="Graf">
  <br>
  <em>Obrázok 6 Graf 3</em>
</p>

```sql
SELECT 
    du.age_group,
    du.occupation,
    COUNT(fr.fact_ratingID) AS total_ratings,
    AVG(fr.rating) AS avg_rating
FROM fact_ratings fr
JOIN dim_users du ON fr.dim_userID = du.dim_userID
GROUP BY du.age_group, du.occupation
ORDER BY total_ratings DESC;
```
---
### **Graf 4: Frekvencia hodnotenia filmov podľa rokov**
Graf ukazuje, ako sa počet hodnotení filmov mení podľa jednotlivých rokov. Z vizualizácie je vidieť, že v posledných rokoch sa počet hodnotení dramaticky zvýšil. Tento trend môže odrážať rastúci záujem o filmy a rozšírenie platformy, kde používatelia hodnotia filmy.

<p align="center">
  <img src="https://github.com/KV1k1/JELLYFISH_MovieLens_DB/blob/main/graphs/rating_frequency_by_year.png" alt="Graf">
  <br>
  <em>Obrázok 7 Graf 4</em>
</p>

```sql
SELECT 
    dd.year AS year,
    COUNT(fr.fact_ratingID) AS total_ratings,
    AVG(fr.rating) AS avg_rating
FROM fact_ratings fr
JOIN dim_dates dd ON fr.dim_dateID = dd.dim_dateID
GROUP BY dd.year
ORDER BY dd.year;
```
---
### **Graf 5: Zmeny priemerného hodnotenia v priebehu času**
Tento graf zobrazuje, ako sa priemerné hodnotenie filmov mení v priebehu rokov. Z vizualizácie je zrejmé, že od roku 2010 sa priemerné hodnotenie postupne zvyšuje. Tento trend môže byť spôsobený zlepšením kvality filmov alebo zmenou kritérií hodnotenia používateľov.

<p align="center">
  <img src="https://github.com/KV1k1/JELLYFISH_MovieLens_DB/blob/main/graphs/avg_rating_changes.png" alt="Graf">
  <br>
  <em>Obrázok 8 Graf 5</em>
</p>

```sql
SELECT 
    dd.year AS year,
    AVG(fr.rating) AS avg_rating
FROM fact_ratings fr
JOIN dim_dates dd ON fr.dim_dateID = dd.dim_dateID
GROUP BY dd.year
ORDER BY dd.year;
```
---
### **Graf 6: Rozdelenie hodnotení podľa povolania**
Tento graf ukazuje, ako sa hodnotenia filmov líšia podľa povolaní používateľov. Z údajov vyplýva, že napríklad používatelia s profesiami v oblasti marketingu a knižníc sú medzi najaktívnejšími hodnotiteľmi filmov. Tieto informácie môžu byť využité na prispôsobenie marketingových kampaní alebo cieľového obsahu pre rôzne profesijné skupiny.

<p align="center">
  <img src="https://github.com/KV1k1/JELLYFISH_MovieLens_DB/blob/main/graphs/ratings_by_occupation.png" alt="Graf">
  <br>
  <em>Obrázok 9 Garf 6</em>
</p>

```sql
SELECT 
    du.occupation AS occupation,
    fr.rating AS rating,
    COUNT(fr.fact_ratingID) AS rating_count,
    AVG(fr.rating) AS avg_rating
FROM fact_ratings fr
JOIN dim_users du ON fr.dim_userID = du.dim_userID
GROUP BY du.occupation, fr.rating
ORDER BY occupation, rating;
```

Dashboard poskytuje komplexný pohľad na dáta, pričom zodpovedá dôležité otázky týkajúce sa čitateľských preferencií a správania používateľov. Vizualizácie umožňujú jednoduchú interpretáciu dát a môžu byť využité na optimalizáciu odporúčacích systémov, marketingových stratégií a knižničných služieb.

---

### **Alternatíva - Čo sa stane so „tags“ tabuľkou?**

Tabuľka „tags“ v databáze MovieLens predstavuje metadáta poskytnuté používateľmi o filmoch a nie je priamo súčasťou schémy `fact_ratings`.

**Čo môžeme s ňou robiť?**

- **Vytvoriť samostatnú faktovú tabuľku pre tagy**

Ak sa tagy často analyzujú (napríklad trend tagov alebo ich korelácia s hodnoteniami), podľa mňa, vytvorenie samostatnej faktovej tabuľky by bolo vhodné.

<p align="center">
  <img src="https://github.com/KV1k1/JELLYFISH_MovieLens_DB/blob/main/star_schema_tags.png" alt="Star Schema">
  <br>
  <em>Obrázok 10 fact_tags</em>
</p>

- **Dimenzionálna tabuľka**
  -  a. Ak nám nezáleží na jednotlivých tagoch, môžeme ich jednoducho pridať do tabuľky `dim_movies` ako zoznam oddelený čiarkami, čím by každý film mal zoznam svojich tagov.

```sql
ALTER TABLE dim_movies ADD tags VARCHAR;
UPDATE dim_movies m
SET tags = (
    SELECT STRING_AGG(t.tags, ', ')
    FROM tags_staging t
    WHERE t.movie_id = m.dim_movieID
);
```

  -  b. Alternatívne môžeme vytvoriť samostatnú dimenzionálnu tabuľku `dim_tags`, ktorá by obsahovala všetky tagy ako samostatné riadky, a tieto tagy by boli pripojené k filmom cez cudzí kľúč.

---
**Autor:** Viktória Kovácsová
