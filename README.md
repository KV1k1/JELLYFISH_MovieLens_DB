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

Účelom ETL procesu bolo pripraviť, transformovať a sprístupniť tieto dáta pre viacdimenzionálnu analýzu.

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

-  **`dim_movies`**: Detaily o filmoch (názov, rok vydania, zoznam všetkých žánrov priradených k filmu (associated_genres), hlavný žáner (main_genre), zoznam všetkých tagov (associated_tags), prvý tag (first_tag)).
-  **`dim_users`**: Demografické informácie o používateľoch (vek, pohlavie, povolanie).
-  **`dim_genres`**: Tabuľka žánrov filmov, kde každý riadok predstavuje jedinečný žáner.
-  **`dim_tags`**: Tabuľka tagov filmov obsahujúca každý unikátny tag, dátum jeho vytvorenia a počet jeho použití.
-  **`dim_dates`**: Dátumy hodnotenia (deň, mesiac, rok).
-  **`dim_time`**: Časové údaje (hodina, minuta, sekunda, AM/PM).

Štruktúra hviezdicového modelu  je znázornená na diagrame nižšie. Diagram ukazuje prepojenia medzi faktovou tabuľkou a dimenziami, čo zjednodušuje pochopenie a implementáciu modelu.

<p align="center">
  <img src="https://github.com/KV1k1/JELLYFISH_MovieLens_DB/blob/main/star_schema.png" alt="Star Schema">
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

Do stage boli následne nahraté súbory. Dáta boli importované do staging tabuliek pomocou príkazu COPY INTO. Pre každú tabuľku sa použil podobný príkaz:

```sql
COPY INTO age_group_staging
FROM @MovieLens_Stage/age_group.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);
```

Podobne boli spracované ostatné tabuľky.

---
### **3.2 Transfor (Transformácia dát)**
V tejto fáze boli dáta zo staging tabuliek vyčistené, transformované a obohatené. Hlavným cieľom bolo pripraviť dimenzie a faktovú tabuľku, ktoré umožnia jednoduchú a efektívnu analýzu.

Dimenzie boli navrhnuté na poskytovanie kontextu pre faktovú tabuľku. Každá dimenzia je klasifikovaná podľa typu **Slowly Changing Dimensions (SCD)** podľa toho, ako sa správa pri zmenách údajov.

#### **Dimenzia dim_genres**

Dimenzia `dim_genres` obsahuje informácie o rôznych žánroch filmov, ktoré môžu byť použité na ďalšiu analýzu alebo filtrovanie. Táto dimenzia je **SCD Typ 0**.

```sql
CREATE OR REPLACE TABLE dim_genres AS
SELECT
    id AS dim_genreID,
    name AS genre_name,
FROM genres_staging g;
```


#### **Dimenzia dim_movies**

Dimenzia `dim_movies` obsahuje údaje o filmoch, ako sú názov, rok vydania a žánre. Táto dimenzia je **SCD Typ 0**.

```sql
CREATE OR REPLACE TABLE dim_movies AS (
SELECT
    m.id AS dim_movieID,
    m.title,
    m.release_year,
    (                       -- comma seperated genres
        SELECT LISTAGG(DISTINCT g.name, ', ') WITHIN GROUP (ORDER BY g.name)
        FROM genres_movies_staging gm
        JOIN genres_staging g ON gm.genre_id = g.id
        WHERE gm.movie_id = m.id
    ) AS associated_genres,
    (                       -- first genre for a movie      
        SELECT MIN(g.name)
        FROM genres_movies_staging gm
        JOIN genres_staging g ON gm.genre_id = g.id
        WHERE gm.movie_id = m.id
    ) AS main_genre,
    (                       -- comma separated tags
        SELECT LISTAGG(DISTINCT t.tags, ', ') WITHIN GROUP (ORDER BY t.tags)
        FROM tags_staging t
        WHERE t.movie_id = m.id
    ) AS associated_tags,
    (                       -- first tag
        SELECT MIN(t.tags)
        FROM tags_staging t
        WHERE t.movie_id = m.id
    ) AS first_tag
FROM movies_staging m
);
```

#### **Dimenzia dim_tags**

Dimenzia `dim_genres` obsahuje informácie o rôznych žánroch filmov, ktoré môžu byť použité na ďalšiu analýzu alebo filtrovanie. Táto dimenzia je **SCD Typ 0** čo znamená, že sleduje historické zmeny. Každá zmena je zaznamenaná ako nový záznam s časovým označením, pričom staré hodnoty sa zachovávajú.

```sql
CREATE OR REPLACE TABLE dim_tags AS
SELECT DISTINCT
    ROW_NUMBER() OVER (ORDER BY t.tags) AS dim_tagID,
    t.tags AS tag_name,
    MIN(t.created_at) AS created_at,
    COUNT(DISTINCT t.movie_id) AS tag_usage_count, -- specific tag usage
FROM tags_staging t
LEFT JOIN ratings_staging r ON t.movie_id = r.movie_id
GROUP BY t.tags;
```

#### **Dimenzia dim_users**

Dimenzia `dim_users` obsahuje údaje o používateľoch, vrátane vekovej kategorizácie, pohlavia, PSČ a zamestnania. Transformácia zahŕňala rozdelenie veku používateľov do kategórií (napr. „18-24“). Táto dimenzia je **SCD Typ 2**.

```sql
CREATE OR REPLACE TABLE dim_users AS
SELECT
    u.id AS dim_userID,
    ag.name AS age_group,
    u.gender,
    u.zip_code,
    o.name AS occupation
FROM users_staging u
JOIN age_group_staging ag ON u.age = ag.id
JOIN occupations_staging o ON u.occupation_id = o.id;
```

SCD Typ 2 – Každá zmena v týchto atribútoch vytvára nový záznam v dimenzii, pričom staré hodnoty sú archivované pre historické účely.

#### **Dimenzia dim_dates**

Dimenzia `dim_dates` obsahuje informácie o dátumoch hodnotení filmov. Zabezpečuje analýzu trendov podľa dní, mesiacov, rokov, týždňov a štvrťrokov. Táto dimenzia je **SCD Typ 0**, čo znamená, že údaje v tejto dimenzii sú nemenné a nebudú sa aktualizovať. Ak by bolo potrebné sledovať ďalšie historické zmeny (napr. pre pracovné dni vs. sviatky), klasifikácia by mohla byť prehodnotená na SCD Typ 1 (aktualizácia existujúcich hodnôt) alebo SCD Typ 2 (uchovávanie histórie).

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

#### **Dimenzia dim_time**

Dimenzia `dim_time` poskytuje podrobnosti o čase hodnotení, ako sú hodiny, minúty, sekundy a AM/PM rozlíšenie. Táto dimenzia je **SCD Typ 0**.

```sql
CREATE OR REPLACE TABLE dim_time AS
SELECT DISTINCT
    ROW_NUMBER() OVER (ORDER BY rated_at) AS dim_timeID,
    CAST(rated_at AS TIME) AS time,
    DATE_PART('hour', time) AS hour,
    DATE_PART('minute', time) AS minute,
    DATE_PART('second', time) AS second,
    CASE
        WHEN DATE_PART('hour', time) < 12 THEN 'AM'
        ELSE 'PM'
    END AS ampm
FROM 
    (SELECT DISTINCT CAST(rated_at AS TIME) AS rated_at FROM ratings_staging);
```

#### **Faktová tabuľka fact_ratings**

Faktová tabuľka `fact_ratings` obsahuje záznamy o hodnoteniach filmov, s prepojeniami na všetky dimenzie a dodatočné metriky..

```sql
CREATE OR REPLACE TABLE fact_ratings AS
SELECT
    r.id AS fact_ratingID,                                  -- rating ID
    r.rated_at AS rating_datetime,                          -- datetime
    r.rating AS rating,                                     -- user rating
    du.dim_userID AS dim_userID,                            -- user dim ID
    dm.dim_movieID AS dim_movieID,                          -- movie dim ID
    dd.dim_dateID AS dim_dateID,                            -- date dim ID
    dt.dim_timeID AS dim_timeID,                            -- time dim ID
    dg.dim_genreID AS dim_genreID,                          -- genre dim ID
    dtg.dim_tagID AS dim_tagID,                             -- tag dim ID
    
    (           -- total number of ratings for the movie
        SELECT COUNT(*)
        FROM ratings_staging
        WHERE movie_id = r.movie_id
    ) AS num_of_movie_ratings,               
    
    (           -- avg movie rating
        SELECT ROUND(AVG(rating),2)
        FROM ratings_staging
        WHERE movie_id = r.movie_id
    ) AS avg_movie_rating,
     
    (           -- avg rating given by the user
        SELECT ROUND(AVG(rating),2)
        FROM ratings_staging
        WHERE user_id = r.user_id
    ) AS avg_user_rating,        
    
    (           -- total number of ratings given by the user
        SELECT COUNT(*)
        FROM ratings_staging
        WHERE user_id = r.user_id
    ) AS num_of_user_ratings,  
    CASE WHEN r.rating >= 4 THEN 'Yes' ELSE 'No' END AS user_recommends -- recommendation (if user rating >= 4)
FROM ratings_staging r
JOIN dim_dates dd ON CAST(r.rated_at AS DATE) = dd.rated_at
JOIN dim_time dt ON CAST(r.rated_at AS TIME) = dt.time
JOIN dim_users du ON du.dim_userID = r.user_id
JOIN dim_movies dm ON dm.dim_movieID = r.movie_id
LEFT JOIN dim_genres dg ON dg.genre_name = dm.main_genre
LEFT JOIN dim_tags dtg ON dm.first_tag = dtg.tag_name;
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

Dashboard obsahuje `5 vizualizácií`, ktoré poskytujú základný prehľad o kľúčových metrikách a trendoch týkajúcich sa filmov, používateľov a hodnotení. Tieto vizualizácie odpovedajú na dôležité otázky a umožňujú lepšie pochopiť správanie používateľov a ich preferencie.

<p align="center">
  <img src="https://github.com/KV1k1/JELLYFISH_MovieLens_DB/blob/main/MovieLens_dashboard.png" alt="ERD Schema">
  <br>
  <em>Obrázok 3 Dashboard MovieLens datasetu</em>
</p>

---
### **Graf 1: Rozdelenie hodnotení podľa pohlavia a času dňa**
Tento graf zobrazuje, ako sa hodnotenia delia podľa pohlavia používateľov a času dňa. Ukazuje sa, že ženy aj muži častejšie hodnotia filmy doobeda. Táto informácia môže byť užitočné pri plánovaní kampaní, ktoré sa zameriavajú na určité časové obdobia.

<p align="center">
  <img src="https://github.com/KV1k1/JELLYFISH_MovieLens_DB/blob/main/graphs/gender_ampm.png" alt="Graf">
  <br>
  <em>Obrázok 4 Graf 1</em>
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
### **Graf 2: Celkové hodnotenia používateľov vs priemerné hodnotenie**
Tento graf zobrazuje celkový počet hodnotení, ktoré jednotliví používatelia udelili, a ich priemerné hodnotenie. Z vizualizácie môžeme zistiť, že napríklad používatelia v kategórii „56+“ majú nižší počet hodnotení, ale ich priemerné hodnotenie filmov je o niečo vyššie ako u mladších používateľov. Tieto údaje môžu byť použité na lepšie prispôsobenie odporúčaní na základe vekovej kategórie alebo profesie.

<p align="center">
  <img src="https://github.com/KV1k1/JELLYFISH_MovieLens_DB/blob/main/graphs/total_vs_avg_rating.png" alt="Graf">
  <br>
  <em>Obrázok 5 Graf 2</em>
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
### **Graf 3: Frekvencia hodnotenia filmov podľa rokov**
Graf ukazuje, ako sa počet hodnotení filmov mení podľa jednotlivých rokov. Z vizualizácie je vidieť, že v posledných rokoch sa počet hodnotení dramaticky znížil.

<p align="center">
  <img src="https://github.com/KV1k1/JELLYFISH_MovieLens_DB/blob/main/graphs/rating_frequency_by_year.png" alt="Graf">
  <br>
  <em>Obrázok 6 Graf 3</em>
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
### **Graf 4: Zmeny priemerného hodnotenia v priebehu času**
Tento graf zobrazuje, ako sa priemerné hodnotenie filmov mení v priebehu rokov. Z vizualizácie je zrejmé, že od roku 2000 sa priemerné hodnotenie postupne klesá. Tento trend môže byť spôsobený zlepšením kvality filmov alebo zmenou kritérií hodnotenia používateľov.

<p align="center">
  <img src="https://github.com/KV1k1/JELLYFISH_MovieLens_DB/blob/main/graphs/avg_rating_changes.png" alt="Graf">
  <br>
  <em>Obrázok 7 Graf 4</em>
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
### **Graf 5: Rozdelenie hodnotení podľa povolania**
Tento graf ukazuje, ako sa hodnotenia filmov líšia podľa povolaní používateľov. Z údajov vyplýva, že napríklad používatelia s profesiami "Educator" a "Executive" sú medzi najaktívnejšími hodnotiteľmi filmov. Tieto informácie môžu byť využité na prispôsobenie marketingových kampaní alebo cieľového obsahu pre rôzne profesijné skupiny.

<p align="center">
  <img src="https://github.com/KV1k1/JELLYFISH_MovieLens_DB/blob/main/graphs/ratings_by_occupation.png" alt="Graf">
  <br>
  <em>Obrázok 8 Garf 5</em>
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

Dashboard poskytuje komplexný pohľad na dáta, pričom zodpovedá dôležité otázky týkajúce sa preferencií divákov a ich správania pri hodnotení filmov. Vizualizácie umožňujú jednoduchú interpretáciu dát a môžu byť využité optimalizáciu odporúčacích systémov, marketingových stratégií a plánovania filmových kampaní.


---
**Autor:** Viktória Kovácsová
