import pandas as pd
import geopandas as gpd
from pathlib import Path
import sys

def get_config():
    """
        Initierar projektets inställningar.
        
        Returns:
            dict: En dictionary innehållande sökvägar (Path), kolumnnamn (list), 
                och koordinatsystem (str).
    """
    root = Path(r"C:\Users\Asus\Documents\Git repos\Arter_i_avverkning")
    processed_dir = root / "processed"
    processed_dir.mkdir(parents=True, exist_ok=True)
    
    return {
        "input_dir": root,
        "map_dir": root,
        "cache_obs": processed_dir / "Art_cache.parquet",
        "cache_layers": {
            "utford": processed_dir / "utford_cache.parquet",
            "anmald": processed_dir / "anmald_cache.parquet"
        },
        "output_file": processed_dir / "Art_analys_resultat.xlsx",
        "layers": {
            "utford": "sksUtfordAvverk.gpkg",
            "anmald": "sksAvverkAnm.gpkg"
        },
        "keep_cols": [
            'Rödlistade', 'Artnamn', 'Vetenskapligt namn', 'Antal', 'Enhet', 
            'Huvudlokal', 'Lokalnamn', 'Ost', 'Nord', 'Noggrannhet', 
            'Diffusion', 'Startdatum', 'Starttid', 'Publik kommentar', 
            'Rapportör', 'Observatörer', 'Län'
        ],
        "crs": "EPSG:3006"
    }


def load_observations(cfg):
    """
    Läser in och tvättar artdata från Excel eller Parquet-cache.
    
    Args:
        cfg (dict): Konfigurations-dictionary från get_config().
        
    Returns:
        gpd.GeoDataFrame: En GeoDataFrame med punktgeometri (SWEREF99 TM) 
                          innehållande tvättad artdata.
    """

    if cfg["cache_obs"].exists():
        print(f"[Cache] Laddar observationer från {cfg['cache_obs'].name}...")
        return gpd.read_parquet(cfg["cache_obs"])

    print(f"[Inläsning] Läser Excel-filer från {cfg['input_dir']}...")
    obs_files = list(cfg["input_dir"].glob("*.xlsx"))
    if not obs_files:
        print("[Fel] Inga filer hittades."); sys.exit()

    dfs = []
    for f in obs_files:
        print(f" -> läser {f.name}")
        df = pd.read_excel(f, skiprows=2)
        
        # Behåll endast kolumnerna från config (om de finns i filen)
        valid_cols = [c for c in cfg["keep_cols"] if c in df.columns]
        df = df[valid_cols].copy()
        
        # Fixa datatyper för att undvika ArrowInvalid-krasch
        df['Ost'] = pd.to_numeric(df['Ost'], errors='coerce')
        df['Nord'] = pd.to_numeric(df['Nord'], errors='coerce')

        if 'Noggrannhet' in df.columns:
            # Tvinga till siffra (om det finns text där)
            df['Noggrannhet'] = pd.to_numeric(df['Noggrannhet'], errors='coerce')
            # Behåll endast rader med noggrannhet <= 50 (och de som saknar värde helt om du vill)
            df = df[df['Noggrannhet'] <= 50]
        
        if 'Antal' in df.columns:
            # Gör om 'noterad' till NaN så kolumnen blir rent numerisk
            df['Antal'] = pd.to_numeric(df['Antal'], errors='coerce')
        
        # Rensa bort rader som saknar koordinater
        df = df.dropna(subset=['Ost', 'Nord'])
        df['Källa'] = f.name
        dfs.append(df)
    
    combined_df = pd.concat(dfs, ignore_index=True)

    for col in combined_df.columns:
        if combined_df[col].dtype == 'object' and col != 'geometry':
            combined_df[col] = combined_df[col].astype(str).replace('nan', '')

    gdf_obs = gpd.GeoDataFrame(
        combined_df, 
        geometry=gpd.points_from_xy(combined_df.Ost, combined_df.Nord), 
        crs=cfg["crs"]
    )
    
    try:
        gdf_obs.to_parquet(cfg["cache_obs"])
        print(f" -> Cache skapad: {len(gdf_obs)} observationer.")
    except Exception as e:
        print(f"[Varning] Kunde inte spara cache (kör vidare ändå): {e}")
        
    return gdf_obs

def load_filtered_logging(cfg, bbox, start_year):
    """
    Läser in avverkningsskikt begränsat till ett geografiskt område och tidsspann.
    
    Args:
        cfg (dict): Konfigurations-dictionary.
        bbox (tuple): Geografisk begränsning (minx, miny, maxx, maxy).
        start_year (int): Det tidigaste året som ska inkluderas i analysen.
        
    Returns:
        dict: En dictionary där nycklarna är lagernamn ('utford' och 'anmäld') och 
              värdena är GeoDataFrames med polygoner.
    """
    logging_data = {}
    
    for key, filename in cfg["layers"].items(): # utförd och anmäld
        cache_path = cfg["cache_layers"][key]
        
        # Om vi laddar från cache, har vi redan filtrerat (eller så filtrerar vi igen för säkerhets skull)
        if cache_path.exists():
            print(f"[Cache] Laddar {key}...")
            gdf = gpd.read_parquet(cache_path)
        else:
            path = cfg["map_dir"] / filename
            print(f"[Inläsning] Filtrerar {filename} mot BBOX...")
            gdf = gpd.read_file(path, bbox=bbox)
            if gdf.crs != cfg["crs"]:
                gdf = gdf.to_crs(cfg["crs"])

        date_col = 'Avvdatum' if key == 'utford' else 'Inkomdatum'    # --- FILTRERING PÅ TID ---
        
        if date_col in gdf.columns:
            before_count = len(gdf)
            gdf[date_col] = pd.to_datetime(gdf[date_col], errors='coerce')
            # Behåll endast avverkningar från och med samma år som första observationen
            gdf = gdf[gdf[date_col].dt.year >= start_year]
            
            diff = before_count - len(gdf)
            if diff > 0:
                print(f" -> Tog bort {diff} st {key} avverkningar från före år {start_year}")

        # Spara till cache 
        if not cache_path.exists():
            gdf.to_parquet(cache_path)
            
        logging_data[key] = gdf
        
    return logging_data


def run_spatial_analysis(gdf_obs, logging_data):
    """
    Korsar artpunkter med avverkningspolygoner geografiskt.
    
    Args:
        gdf_obs (gpd.GeoDataFrame): GeoDataFrame med artpunkter.
        logging_data (dict): Dictionary med GeoDataFrames för avverkning.
        
    Returns:
        dict: Resultat per lager innehållande 'matches' (GeoDataFrame med träffar)
              och 'total_relevant_count' (int).
    """
    results = {}
    
    # Skapa en solid yta (gummiband) runt alla observationer
    study_area_mask = gdf_obs.union_all().convex_hull.buffer(50)
    
    # Skapa 50m buffert runt alla observationer för själva träff-analysen
    gdf_obs_buffered = gdf_obs.copy()
    gdf_obs_buffered['geometry'] = gdf_obs_buffered.geometry.buffer(50)
    
    for key, gdf_logging in logging_data.items():

        # Behåll endast avverkningar som nuddar studieområdet
        relevant_logging = gdf_logging[gdf_logging.intersects(study_area_mask)].copy()
        
        print(f"[Analys] Matchar mot {len(relevant_logging)} relevanta områden i {key}...")
        
        # 1. Direkt i (Status: Inuti)
        in_zone = gpd.sjoin(gdf_obs, relevant_logging, how="inner", predicate="within")
        in_zone['Status_Analys'] = 'Inuti'
        
        # 2. Inom 50m (Status: Nära)
        all_near = gpd.sjoin(gdf_obs_buffered, relevant_logging, how="inner", predicate="intersects")
        all_near['Status_Analys'] = 'Nära (50m)'
        
        # 3. Kombinera och prioritera 'Inuti'
        combined = pd.concat([in_zone, all_near])
        combined = combined[~combined.index.duplicated(keep='first')]
        
        # Spara både träffarna OCH det filtrerade lagret för statistik
        results[key] = {
            'matches': combined,
            'total_relevant_count': len(relevant_logging)
        }
        
    return results


def describe_and_save(gdf_obs, results, logging_data, cfg):
    """
    Sammanställer analysen i terminalen och sparar detaljer till Excel.
    
    Args:
        gdf_obs (gpd.GeoDataFrame): Den ursprungliga artdatan.
        results (dict): Resultat-dict från run_spatial_analysis().
        logging_data (dict): Inlästa avverkningar.
        cfg (dict): Konfigurations-dictionary.
        
    Returns:
        None: Funktionen skriver ut till terminalen och sparar en fil på hårddisken.
    """
    # 1. Arter
    arter = ", ".join(gdf_obs['Artnamn'].unique())
    
    # 2. Tidsperiod Observationer
    obs_dates = pd.to_datetime(gdf_obs['Startdatum'], errors='coerce')
    obs_min, obs_max = int(obs_dates.min().year), int(obs_dates.max().year)
    
    # 3. Geografi - Län
    unika_lan = gdf_obs['Län'].dropna().unique() if 'Län' in gdf_obs.columns else []
    lan_info = ", ".join(unika_lan) if len(unika_lan) > 0 else "Okänt område"

    print("\n" + "="*70)
    print(f"    ANALYS AV:                {arter.upper()}")
    print(f"    Studieområde:             {lan_info} (inom ytterkant av artfynd)")
    print(f"    Tidsperiod artfynd:       {obs_min} till {obs_max}")
    print(f"    Filter:                   Noggrannhet <= 50 meter")
    print("="*70 + "\n")

    for key in ["utford", "anmald"]:
        # Hämta data från results
        analysis_data = results.get(key, {'matches': pd.DataFrame(), 'total_in_area': 0})
        res_df = analysis_data['matches']
        total_relevant = analysis_data.get('total_relevant_count', 0)

        layer_df = logging_data.get(key, pd.DataFrame())
        
        layer_full_name = "UTFÖRDA AVVERKNINGAR" if key == "utford" else "ANMÄLDA AVVERKNINGAR"
        
        # Datumhantering 
        date_col = 'Avvdatum' if key == 'utford' else 'Inkomdatum'
        date_info = ""
        if not layer_df.empty and date_col in layer_df.columns:
            l_dates = pd.to_datetime(layer_df[date_col], errors='coerce').dropna()
            date_info = f"{l_dates.min().year} till {l_dates.max().year}"
        else:
            date_info = "Saknas"

        # Statistik
        n_inuti = len(res_df[res_df['Status_Analys'] == 'Inuti'])
        n_nara = len(res_df[res_df['Status_Analys'] == 'Nära (50m)'])
        
        all_affected_areas = res_df['index_right'].unique()
        areas_inuti_ids = res_df[res_df['Status_Analys'] == 'Inuti']['index_right'].unique()
        areas_nara_only = len(set(all_affected_areas) - set(areas_inuti_ids))
        
        print(f"{layer_full_name} ({date_info}):")
        print(f"  VÄXTPLATSER :")
        print(f"    - Direkt i avverkning:                       {n_inuti} st")
        print(f"    - I närzon (0-50m):                          {n_nara} st")
        print(f"  AVVERKNINGSOMRÅDEN:")
        print(f"    - Antal områden med artfynd inuti:           {len(areas_inuti_ids)} st")
        print(f"    - Antal områden med artfynd endast i närzon: {areas_nara_only} st")
        andel_omraden = (len(all_affected_areas) / total_relevant) * 100
        print(f"    - TOTALT berörda områden:                    {len(all_affected_areas)} st utav {total_relevant} ({andel_omraden:.1f}%)")
        print()

    all_dfs = [results[k]['matches'] for k in results]
    all_affected_idx = pd.concat(all_dfs).index.unique()
    
    print(f"TOTALT UNIKA VÄXTPLATSER SOM BERÖRS:             {len(all_affected_idx)} av {len(gdf_obs)}")
    print(f"ANDEL BERÖRDA VÄXTPLATSER:                       {(len(all_affected_idx)/len(gdf_obs))*100:.1f}%")
    print("="*70)

    # Spara till Excel
    with pd.ExcelWriter(cfg["output_file"]) as writer:
        gdf_obs.drop(columns='geometry').to_excel(writer, sheet_name='Alla_Fynd', index=False)
        for key, data in results.items():
            df = data['matches']
            if not df.empty:
                df.drop(columns='geometry').to_excel(writer, sheet_name=f'Träffar_{key}', index=False)



if __name__ == "__main__":
    cfg = get_config()   # Definierar sökvägar och parametrar
    
    gdf_obs = load_observations(cfg)  # Laddar artdata till geo_dataframe
    
        # Ladda bara de avverkningar som är aktuella, med startår och BBOX som filter
    obs_dates = pd.to_datetime(gdf_obs['Startdatum'], errors='coerce')
    start_year = int(obs_dates.min().year)
    boundary = tuple(gdf_obs.total_bounds)   
    logging_data = load_filtered_logging(cfg, boundary, start_year)
    
    analysis_results = run_spatial_analysis(gdf_obs, logging_data)
    
    describe_and_save(gdf_obs, analysis_results, logging_data, cfg)