# Arter i avverkningar

Matchar artobservationer (från Artportalen) mot Skogsstyrelsens lager för avverkningsanmälningar och utförda avverkningar.
Det går att ha flera arter och valfritt område.
Beräknar andel fynd som påverkas av avverkning och andel av avverkningar som har artfynd.


# Datastruktur
För att skriptet ska fungera behöver följande filer finnas i projektets rotmapp:

1. Excel-filer (.xlsx) exporterade från Artportalen. Följande kolumner krävs
    Ost och Nord i koordinatsystemet EPSG:3006.
    Startdatum, Noggrannhet och Artnamn.
    Notera: Skriptet hoppar över de två första raderna i Excel-filen (skiprows=2) för att hantera Artportalens standardexport, 
    och rensar automatiskt bort rader som saknar koordinater.

2. Kartskikt: Skogsstyrelsens rikstäckande GeoPackage-filer:
    sksUtfordAvverk.gpkg (Utförda avverkningar)
    sksAvverkAnm.gpkg (Avverkningsanmälningar)

# Analyslogik
Filtrerar automatiskt bort 
    avverkningar som skett före det äldsta artfyndet
    artfynd med noggranhet > 50 m
Hanterar tunga nationella GeoPackage-filer genom att endast läsa in avverkningsområden som överlappar fynd med convex_hull.
Beräknar träffar både direkt **Inuti** och inom en **Närzon (50m)** från avverkningsytan.
Bearbetad data sparas som `.parquet`-filer i mappen `processed/` för att snabba upp framtida körningar.

# Användning
1. Placera Excel-filer med artfynd i projektmappen. Nedladdat från Artportalen.
2. kontrollera att sökvägen i get_config() pekar på din lokala projektmapp.
3. Se till att `sksUtfordAvverk.gpkg` och `sksAvverkAnm.gpkg` finns i mappen.
4. Ta ev. bort tidigare `.parquet`-filer i mappen `processed/` om de inte ska användas.
5. Kör skriptet och granska resultatet i monitorn och `processed/Art_analys_resultat.xlsx`.

# Filstruktur

```
├── Arter_i_avverkning.py  
├───in_data
│   ├─── excelfil
│   ├─── sksAvverkAnm.gpkg
│   └─── sksUtfordAvverk.gpkg
└───out_data             
    ├── Art_cache.parquet
    └── Art_analys_resultat.xlsx
```

# Licence 
Detta repository är licensierat under MIT open license


